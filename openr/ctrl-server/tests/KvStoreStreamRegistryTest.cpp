/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <chrono>
#include <optional>
#include <vector>

#include <folly/CancellationToken.h>
#include <folly/coro/Collect.h>
#include <folly/coro/GtestHelpers.h>
#include <folly/coro/WithCancellation.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/init/Init.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <folly/synchronization/Baton.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <openr/common/Constants.h>
#include <openr/common/Util.h>
#include <openr/ctrl-server/KvStoreStreamRegistry.h>

namespace openr {

namespace {

const std::string kArea{"area1"};

thrift::Publication
makePublication(const std::vector<std::string>& keys) {
  thrift::Publication pub;
  pub.area() = kArea;
  for (const auto& key : keys) {
    pub.keyVals()->emplace(
        key, createThriftValue(1, "node1", std::string("value")));
  }
  return pub;
}

// Keeps every key, TTL updates included: such subscribers share messages.
KvStorePublicationFilter
allKeysFilter() {
  thrift::KeyDumpParams params;
  params.ignoreTtl() = false;
  return KvStorePublicationFilter({kArea}, std::move(params));
}

KvStorePublicationFilter
keysFilter(std::vector<std::string> keys) {
  thrift::KeyDumpParams params;
  params.keys() = std::move(keys);
  return KvStorePublicationFilter({kArea}, std::move(params));
}

std::set<std::string>
keysOf(const thrift::Publication& pub) {
  std::set<std::string> keys;
  for (const auto& [key, _] : *pub.keyVals()) {
    keys.insert(key);
  }
  return keys;
}

/*
 * Admission limits that no test reaches; memory limits of 1 MiB that a test
 * reaches only when it lowers them.
 */
constexpr KvStoreStreamLimits kTestLimits{
    .maxSnapshotsInFlight = 100,
    .maxSubscribers = 100,
    .maxQueueBytes = 1024 * 1024,
    .maxTotalBytes = 1024 * 1024};

KvStoreStreamHandle
add(KvStoreStreamRegistry& registry,
    int64_t id,
    KvStorePublicationFilter filter) {
  auto handle = registry.tryAdd(id, std::move(filter), kTestLimits);
  if (!handle) {
    throw std::logic_error("tryAdd() rejected a subscriber below the limits");
  }
  return std::move(*handle);
}

folly::exception_wrapper
terminated() {
  return folly::make_exception_wrapper<std::runtime_error>(
      "publisher terminated");
}

// Publication `seq` of the concurrency test: keys prefix:a and adj:b.
thrift::Publication
makeSequencedPublication(int64_t seq) {
  thrift::Publication pub;
  pub.area() = kArea;
  for (const auto& key : {"prefix:a", "adj:b"}) {
    pub.keyVals()->emplace(
        key, createThriftValue(seq, "node1", std::string("value")));
  }
  return pub;
}

/*
 * Stream generator stand-in: owns the handle and pops `count` messages, each
 * with `numKeys` keys, recording the version of prefix:a. Posts `received`,
 * then, with `awaitClose`, waits for the queue to close. The handle is gone
 * before the result is ready.
 */
folly::coro::Task<std::vector<int64_t>>
consume(
    KvStoreStreamHandle handle,
    size_t numKeys,
    int64_t count,
    bool awaitClose,
    folly::Baton<>* received) {
  // A local, unlike the parameter, is destroyed before the task completes.
  auto owned = std::move(handle);
  std::vector<int64_t> versions;
  while (static_cast<int64_t>(versions.size()) < count) {
    auto pub = co_await owned.queue().pop();
    EXPECT_EQ(numKeys, pub.keyVals()->size());
    versions.push_back(*pub.keyVals()->at("prefix:a").version());
  }
  received->post();
  if (awaitClose) {
    auto end = co_await folly::coro::co_awaitTry(owned.queue().pop());
    EXPECT_TRUE(end.hasException<std::runtime_error>());
  }
  co_return versions;
}

std::vector<int64_t>
sequence(int64_t count) {
  std::vector<int64_t> seqs;
  for (int64_t seq = 1; seq <= count; ++seq) {
    seqs.push_back(seq);
  }
  return seqs;
}

} // namespace

/*
 * Subscribers that keep all keys receive the whole publication. A key filter
 * receives the matching keys. A subscriber of another area receives nothing.
 */
CO_TEST(KvStoreStreamRegistryTest, FanOutAppliesEachFilter) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  auto all1 = add(*registry, 1, allKeysFilter());
  auto all2 = add(*registry, 2, allKeysFilter());
  auto filtered = add(*registry, 3, keysFilter({"prefix:a"}));
  auto otherArea =
      add(*registry,
          4,
          KvStorePublicationFilter({"area2"}, thrift::KeyDumpParams{}));

  registry->fanOut(makePublication({"prefix:a", "adj:b"}), kTestLimits);

  const std::set<std::string> allKeys{"prefix:a", "adj:b"};
  const std::set<std::string> filteredKeys{"prefix:a"};
  EXPECT_EQ(allKeys, keysOf(co_await all1.queue().pop()));
  EXPECT_EQ(allKeys, keysOf(co_await all2.queue().pop()));
  EXPECT_EQ(filteredKeys, keysOf(co_await filtered.queue().pop()));
  EXPECT_EQ(0, otherArea.queue().queuedMessages());
}

/*
 * A subscriber that keeps all keys receives each publication as KvStore
 * published it, apart from timestamp_ms: TTL updates, perfEvents, and a
 * publication with neither keyVals nor expiredKeys.
 */
CO_TEST(KvStoreStreamRegistryTest, FanOutPassesPublicationThroughUnfiltered) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  auto handle = add(*registry, 1, allKeysFilter());
  auto ttlUpdate = makePublication({"prefix:a"});
  ttlUpdate.keyVals()->emplace(
      "adj:b",
      createThriftValue(
          1 /* version */,
          "node1",
          std::nullopt /* data */,
          300000 /* ttl */,
          2 /* ttlVersion */));
  thrift::PerfEvent event;
  event.nodeName() = "node1";
  event.eventDescr() = "KVSTORE_UPDATE";
  event.unixTs() = 1000;
  thrift::PerfEvents perfEvents;
  perfEvents.events()->push_back(std::move(event));
  ttlUpdate.perfEvents() = std::move(perfEvents);
  thrift::Publication nodeIdsOnly;
  nodeIdsOnly.area() = kArea;
  nodeIdsOnly.nodeIds() = std::vector<std::string>{"node1"};
  const std::vector<thrift::Publication> publications{ttlUpdate, nodeIdsOnly};

  for (const auto& pub : publications) {
    registry->fanOut(pub, kTestLimits);
  }

  for (const auto& expected : publications) {
    auto received = co_await handle.queue().pop();
    EXPECT_TRUE(received.timestamp_ms().has_value());
    received.timestamp_ms().reset();
    EXPECT_EQ(expected, received);
  }
}

/*
 * Subscribers with the same key filter share one message, whatever areas
 * they select. Other key filters, and filters that keep all keys, get their
 * own message. A key filter that matches nothing gets no message.
 */
TEST(KvStoreStreamRegistryTest, FanOutSharesOneCopyPerKeyFilter) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  thrift::KeyDumpParams prefixParams;
  prefixParams.keys() = {"prefix:a"};
  auto prefix1 = add(*registry, 1, keysFilter({"prefix:a"}));
  auto prefix2 = add(
      *registry, 2, KvStorePublicationFilter({kArea, "area2"}, prefixParams));
  auto adj = add(*registry, 3, keysFilter({"adj:b"}));
  // Default params ignore TTL updates, so they do not keep all keys.
  auto default1 = add(*registry, 4, KvStorePublicationFilter({kArea}, {}));
  auto default2 = add(*registry, 5, KvStorePublicationFilter({kArea}, {}));
  auto all1 = add(*registry, 6, allKeysFilter());
  auto all2 = add(*registry, 7, allKeysFilter());
  auto none1 = add(*registry, 8, keysFilter({"none"}));
  auto none2 = add(*registry, 9, keysFilter({"none"}));

  registry->fanOut(makePublication({"prefix:a", "adj:b"}), kTestLimits);

  const auto front = [](KvStoreStreamHandle& handle) {
    return handle.queue().frontPublicationForTest();
  };
  ASSERT_NE(nullptr, front(prefix1));
  ASSERT_NE(nullptr, front(adj));
  ASSERT_NE(nullptr, front(default1));
  ASSERT_NE(nullptr, front(all1));
  EXPECT_EQ(front(prefix1), front(prefix2));
  EXPECT_EQ(front(default1), front(default2));
  EXPECT_EQ(front(all1), front(all2));
  EXPECT_NE(front(prefix1), front(adj));
  EXPECT_NE(front(default1), front(all1));
  EXPECT_EQ(std::set<std::string>{"prefix:a"}, keysOf(*front(prefix1)));
  EXPECT_EQ(std::set<std::string>{"adj:b"}, keysOf(*front(adj)));
  EXPECT_EQ(0, none1.queue().queuedMessages());
  EXPECT_EQ(0, none2.queue().queuedMessages());
}

// A publication without a matching key queues no message.
TEST(KvStoreStreamRegistryTest, FanOutSkipsUnmatchedPublication) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  auto filtered = add(*registry, 1, keysFilter({"prefix:a"}));

  registry->fanOut(makePublication({"adj:b"}), kTestLimits);

  EXPECT_EQ(0, filtered.queue().queuedMessages());
  EXPECT_EQ(0, filtered.queue().queuedBytes());
}

// pop() suspends on an empty queue and resumes at the next publication.
CO_TEST(KvStoreStreamRegistryTest, PopWaitsForFanOut) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  auto handle = add(*registry, 1, allKeysFilter());

  auto [pub, _] = co_await folly::coro::collectAll(
      handle.queue().pop(), [&]() -> folly::coro::Task<void> {
        registry->fanOut(makePublication({"key1"}), kTestLimits);
        co_return;
      }());

  EXPECT_EQ(std::set<std::string>{"key1"}, keysOf(pub));
}

// Queued bytes follow the queued messages; sent messages count the pops.
CO_TEST(KvStoreStreamRegistryTest, QueuedBytesFollowMessages) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  auto handle = add(*registry, 1, allKeysFilter());

  registry->fanOut(makePublication({"key1"}), kTestLimits);
  registry->fanOut(makePublication({"key1", "key2"}), kTestLimits);
  EXPECT_EQ(2, handle.queue().queuedMessages());
  const auto bytesOfTwo = handle.queue().queuedBytes();
  EXPECT_GT(bytesOfTwo, 0);

  co_await handle.queue().pop();
  const auto bytesOfOne = handle.queue().queuedBytes();
  EXPECT_GT(bytesOfOne, 0);
  EXPECT_LT(bytesOfOne, bytesOfTwo);

  co_await handle.queue().pop();
  EXPECT_EQ(0, handle.queue().queuedBytes());
  EXPECT_EQ(2, handle.queue().sentMessages());
}

// closeAll() drops the queued messages and ends pop() with the reason.
CO_TEST(KvStoreStreamRegistryTest, CloseAllEndsPop) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  auto handle = add(*registry, 1, allKeysFilter());
  registry->fanOut(makePublication({"key1"}), kTestLimits);

  registry->closeAll(terminated());

  EXPECT_EQ(0, handle.queue().queuedMessages());
  EXPECT_EQ(0, handle.queue().queuedBytes());
  auto result = co_await folly::coro::co_awaitTry(handle.queue().pop());
  CO_ASSERT_TRUE(result.hasException<std::runtime_error>());
  EXPECT_STREQ(
      "publisher terminated",
      result.exception().get_exception<std::runtime_error>()->what());
}

// A queue closed while pop() waits ends that pop().
CO_TEST(KvStoreStreamRegistryTest, CloseAllWakesWaitingPop) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  auto handle = add(*registry, 1, allKeysFilter());

  auto result = co_await folly::coro::co_awaitTry(
      folly::coro::collectAll(
          handle.queue().pop(), [&]() -> folly::coro::Task<void> {
            registry->closeAll(terminated());
            co_return;
          }()));

  EXPECT_TRUE(result.hasException<std::runtime_error>());
}

// Cancellation of the stream ends a waiting pop().
CO_TEST(KvStoreStreamRegistryTest, CancellationEndsWaitingPop) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  auto handle = add(*registry, 1, allKeysFilter());
  folly::CancellationSource cancel;

  auto result = co_await folly::coro::co_awaitTry(
      folly::coro::collectAll(
          folly::coro::co_withCancellation(
              cancel.getToken(), handle.queue().pop()),
          [&]() -> folly::coro::Task<void> {
            cancel.requestCancellation();
            co_return;
          }()));

  EXPECT_TRUE(result.hasException<folly::OperationCancelled>());
}

/*
 * A subscriber that arrives after closeAll(), as when a subscription races
 * with the handler stop, gets a handle even at the limits: it is not
 * registered, takes no snapshot slot, and its pop() ends with the closeAll()
 * reason.
 */
CO_TEST(KvStoreStreamRegistryTest, AddAfterCloseAllClosesQueue) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  registry->closeAll(terminated());

  auto handle = registry->tryAdd(
      1, allKeysFilter(), {.maxSnapshotsInFlight = 0, .maxSubscribers = 0});
  CO_ASSERT_TRUE(handle.hasValue());
  registry->fanOut(makePublication({"key1"}), kTestLimits);

  EXPECT_EQ(0, registry->size());
  EXPECT_EQ(0, registry->snapshotsInFlight());
  EXPECT_EQ(0, handle->queue().queuedMessages());
  auto result = co_await folly::coro::co_awaitTry(handle->queue().pop());
  CO_ASSERT_TRUE(result.hasException<std::runtime_error>());
  EXPECT_STREQ(
      "publisher terminated",
      result.exception().get_exception<std::runtime_error>()->what());
}

/*
 * The fan-out runs on an event base thread while the streams pop on a thread
 * pool, as in the handler. Subscribers 1, 2 and 4 keep all keys and share
 * each message; subscriber 3 filters prefix:a. Subscriber 4 stops after half
 * the publications and its handle is destroyed while the fan-out runs.
 * closeAll() then ends the waiting pops. Each queue delivers its messages in
 * publication order, none is lost, and every subscriber leaves.
 */
TEST(KvStoreStreamRegistryTest, ConcurrentFanOutPopAndRemove) {
  constexpr int64_t kNumPublications{500};
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  folly::CPUThreadPoolExecutor executor(4);
  folly::ScopedEventBaseThread fanOutThread;
  std::vector<folly::Baton<>> received(4);

  const auto start = [&](int64_t id,
                         KvStorePublicationFilter filter,
                         size_t numKeys,
                         int64_t count,
                         bool awaitClose) {
    return folly::coro::co_withExecutor(
               folly::getKeepAliveToken(executor),
               consume(
                   add(*registry, id, std::move(filter)),
                   numKeys,
                   count,
                   awaitClose,
                   &received.at(id - 1)))
        .start();
  };
  auto all1 = start(1, allKeysFilter(), 2, kNumPublications, true);
  auto all2 = start(2, allKeysFilter(), 2, kNumPublications, true);
  auto filtered = start(3, keysFilter({"prefix:a"}), 1, kNumPublications, true);
  auto leaver = start(4, allKeysFilter(), 2, kNumPublications / 2, false);
  ASSERT_EQ(4, registry->size());

  fanOutThread.getEventBase()->runInEventBaseThread([&]() {
    for (int64_t seq = 1; seq <= kNumPublications; ++seq) {
      registry->fanOut(makeSequencedPublication(seq), kTestLimits);
    }
  });
  EXPECT_EQ(sequence(kNumPublications / 2), std::move(leaver).get());
  for (int i = 0; i < 3; ++i) {
    received.at(i).wait();
  }
  EXPECT_EQ(3, registry->size());
  fanOutThread.getEventBase()->runInEventBaseThreadAndWait(
      [&]() { registry->closeAll(terminated()); });

  EXPECT_EQ(sequence(kNumPublications), std::move(all1).get());
  EXPECT_EQ(sequence(kNumPublications), std::move(all2).get());
  EXPECT_EQ(sequence(kNumPublications), std::move(filtered).get());
  EXPECT_EQ(0, registry->size());
}

// The handle removes its subscriber once, after any moves.
TEST(KvStoreStreamRegistryTest, HandleRemovesSubscriber) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  {
    auto handle = add(*registry, 1, allKeysFilter());
    auto moved = std::move(handle);
    EXPECT_EQ(1, registry->size());
  }
  EXPECT_EQ(0, registry->size());

  registry->fanOut(makePublication({"key1"}), kTestLimits);
}

/*
 * A handle that outlives its registry keeps a usable queue: it pops what was
 * queued and closes. Its destruction removes nothing and does not touch the
 * destroyed registry (ASAN and TSAN modes check the memory).
 */
CO_TEST(KvStoreStreamRegistryTest, HandleOutlivesRegistry) {
  std::optional<KvStoreStreamHandle> handle;
  std::weak_ptr<KvStoreStreamRegistry> weakRegistry;
  {
    auto registry = std::make_shared<KvStoreStreamRegistry>();
    weakRegistry = registry;
    handle.emplace(add(*registry, 1, allKeysFilter()));
    registry->fanOut(makePublication({"key1"}), kTestLimits);
  }
  CO_ASSERT_TRUE(weakRegistry.expired());

  CO_ASSERT_EQ(1, handle->queue().queuedMessages());
  auto pub = co_await handle->queue().pop();
  EXPECT_EQ(std::set<std::string>{"key1"}, keysOf(pub));
  handle->queue().close(terminated());
  auto result = co_await folly::coro::co_awaitTry(handle->queue().pop());
  EXPECT_TRUE(result.hasException<std::runtime_error>());

  handle.reset();
}

// Subscriber info reports the id and the messages each stream sent.
CO_TEST(KvStoreStreamRegistryTest, SubscriberInfoCountsSentMessages) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  auto sender = add(*registry, 7, allKeysFilter());
  auto filtered = add(*registry, 8, keysFilter({"prefix:a"}));
  registry->fanOut(makePublication({"adj:b"}), kTestLimits);
  co_await sender.queue().pop();

  auto infos = registry->getSubscriberInfo();
  CO_ASSERT_EQ(2, infos.size());
  std::map<int64_t, int64_t> sentById;
  for (const auto& info : infos) {
    sentById.emplace(*info.subscriber_id(), *info.total_streamed_msgs());
  }
  const std::map<int64_t, int64_t> expected{{7, 1}, {8, 0}};
  EXPECT_EQ(expected, sentById);
}

/*
 * Above the snapshot gate, tryAdd() registers nothing and reports the gate
 * with the counts it saw, until a slot is released.
 */
TEST(KvStoreStreamRegistryTest, SnapshotGateRejectsAboveLimit) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  const KvStoreStreamLimits limits{
      .maxSnapshotsInFlight = 2, .maxSubscribers = 16};
  auto first = registry->tryAdd(1, allKeysFilter(), limits);
  auto second = registry->tryAdd(2, allKeysFilter(), limits);
  ASSERT_TRUE(first.hasValue());
  ASSERT_TRUE(second.hasValue());

  auto rejected = registry->tryAdd(3, allKeysFilter(), limits);
  ASSERT_TRUE(rejected.hasError());
  EXPECT_EQ(
      KvStoreStreamRejection::Reason::SNAPSHOTS_IN_FLIGHT,
      rejected.error().reason);
  EXPECT_EQ(2, rejected.error().snapshotsInFlight);
  EXPECT_EQ(2, rejected.error().subscribers);
  EXPECT_EQ(2, registry->size());
  EXPECT_EQ(2, registry->snapshotsInFlight());

  first->releaseSnapshotSlot();
  first->releaseSnapshotSlot();
  EXPECT_EQ(1, registry->snapshotsInFlight());
  EXPECT_TRUE(registry->tryAdd(3, allKeysFilter(), limits).hasValue());
  EXPECT_EQ(0, registry->snapshotSlotReclaims());
}

// Above the subscriber cap, tryAdd() registers nothing and reports the cap.
TEST(KvStoreStreamRegistryTest, SubscriberCapRejectsAboveLimit) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  const KvStoreStreamLimits limits{
      .maxSnapshotsInFlight = 4, .maxSubscribers = 2};
  auto first = registry->tryAdd(1, allKeysFilter(), limits);
  auto second = registry->tryAdd(2, allKeysFilter(), limits);
  ASSERT_TRUE(first.hasValue());
  ASSERT_TRUE(second.hasValue());
  first->releaseSnapshotSlot();
  second->releaseSnapshotSlot();

  auto rejected = registry->tryAdd(3, allKeysFilter(), limits);
  ASSERT_TRUE(rejected.hasError());
  EXPECT_EQ(
      KvStoreStreamRejection::Reason::SUBSCRIBERS, rejected.error().reason);
  EXPECT_EQ(0, rejected.error().snapshotsInFlight);
  EXPECT_EQ(2, rejected.error().subscribers);
  EXPECT_EQ(2, registry->size());
}

/*
 * With the gate full, tryAdd() reclaims the slots admitted at least
 * kKvStoreStreamSnapshotSlotTimeout earlier. Their subscribers stay
 * registered, and a later release or removal does not count them again.
 */
TEST(KvStoreStreamRegistryTest, FullGateReclaimsExpiredSnapshotSlots) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  const KvStoreStreamLimits limits{
      .maxSnapshotsInFlight = 2, .maxSubscribers = 16};
  const auto t0 = std::chrono::steady_clock::now();
  const auto timeout = Constants::kKvStoreStreamSnapshotSlotTimeout;
  std::optional<KvStoreStreamHandle> stuck;
  std::optional<KvStoreStreamHandle> recent;
  {
    auto first = registry->tryAdd(1, allKeysFilter(), limits, t0);
    auto second = registry->tryAdd(
        2, allKeysFilter(), limits, t0 + std::chrono::seconds(1));
    ASSERT_TRUE(first.hasValue());
    ASSERT_TRUE(second.hasValue());
    stuck.emplace(std::move(*first));
    recent.emplace(std::move(*second));
  }

  // One second short of the timeout: nothing is reclaimed.
  auto early = registry->tryAdd(
      3, allKeysFilter(), limits, t0 + timeout - std::chrono::seconds(1));
  ASSERT_TRUE(early.hasError());
  EXPECT_EQ(
      KvStoreStreamRejection::Reason::SNAPSHOTS_IN_FLIGHT,
      early.error().reason);
  EXPECT_EQ(0, registry->snapshotSlotReclaims());

  // At the timeout, only the oldest slot is reclaimed.
  auto admitted = registry->tryAdd(4, allKeysFilter(), limits, t0 + timeout);
  ASSERT_TRUE(admitted.hasValue());
  EXPECT_EQ(1, registry->snapshotSlotReclaims());
  EXPECT_EQ(3, registry->size());
  EXPECT_EQ(2, registry->snapshotsInFlight());

  stuck->releaseSnapshotSlot();
  EXPECT_EQ(2, registry->snapshotsInFlight());
  stuck.reset();
  EXPECT_EQ(2, registry->size());
  EXPECT_EQ(2, registry->snapshotsInFlight());
  recent.reset();
  EXPECT_EQ(1, registry->size());
  EXPECT_EQ(1, registry->snapshotsInFlight());
}

// Below the gate, tryAdd() reclaims nothing, however old the slots are.
TEST(KvStoreStreamRegistryTest, GateBelowLimitReclaimsNothing) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  const KvStoreStreamLimits limits{
      .maxSnapshotsInFlight = 2, .maxSubscribers = 16};
  const auto t0 = std::chrono::steady_clock::now();
  auto old = registry->tryAdd(1, allKeysFilter(), limits, t0);
  ASSERT_TRUE(old.hasValue());

  auto next = registry->tryAdd(
      2,
      allKeysFilter(),
      limits,
      t0 + 10 * Constants::kKvStoreStreamSnapshotSlotTimeout);
  ASSERT_TRUE(next.hasValue());
  EXPECT_EQ(0, registry->snapshotSlotReclaims());
  EXPECT_EQ(2, registry->snapshotsInFlight());
}

// A subscriber removed before its first pull releases its snapshot slot.
TEST(KvStoreStreamRegistryTest, RemoveReleasesSnapshotSlot) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  {
    auto handle = add(*registry, 1, allKeysFilter());
    EXPECT_EQ(1, registry->snapshotsInFlight());
  }
  EXPECT_EQ(0, registry->snapshotsInFlight());
  EXPECT_EQ(0, registry->size());
}

/*
 * A queue that holds messages and would exceed the per-stream limit closes
 * and leaves the registry. A smaller queue takes the filtered copy of the
 * same publication up to the limit.
 */
CO_TEST(KvStoreStreamRegistryTest, QueueLimitClosesOverflowingStream) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  auto all = add(*registry, 1, allKeysFilter());
  auto key1Only = add(*registry, 2, keysFilter({"key1"}));
  registry->fanOut(makePublication({"key1"}), kTestLimits);
  // key1Only takes a second message of the same size: exactly the limit.
  auto limits = kTestLimits;
  limits.maxQueueBytes = 2 * key1Only.queue().queuedBytes();

  registry->fanOut(makePublication({"key1", "key2"}), limits);

  EXPECT_EQ(1, registry->overflowDrops());
  EXPECT_EQ(0, registry->budgetDrops());
  EXPECT_EQ(1, registry->size());
  EXPECT_EQ(2, key1Only.queue().queuedMessages());
  EXPECT_EQ(limits.maxQueueBytes, key1Only.queue().queuedBytes());
  EXPECT_EQ(key1Only.queue().queuedBytes(), registry->referencedBytes());
  auto overflowed = co_await folly::coro::co_awaitTry(all.queue().pop());
  CO_ASSERT_TRUE(overflowed.hasException<std::runtime_error>());
  EXPECT_THAT(
      overflowed.exception().what().toStdString(),
      testing::HasSubstr("queue limit"));
  const std::set<std::string> key1Keys{"key1"};
  EXPECT_EQ(key1Keys, keysOf(co_await key1Only.queue().pop()));
  EXPECT_EQ(key1Keys, keysOf(co_await key1Only.queue().pop()));
}

/*
 * An empty queue accepts a message larger than the per-stream limit: its
 * client is not behind. The next message closes the queue only if the first
 * is still queued.
 */
CO_TEST(KvStoreStreamRegistryTest, EmptyQueueAcceptsMessageAboveLimit) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  auto handle = add(*registry, 1, allKeysFilter());
  auto limits = kTestLimits;
  limits.maxQueueBytes = 1;

  registry->fanOut(makePublication({"key1", "key2"}), limits);
  EXPECT_EQ(0, registry->overflowDrops());
  EXPECT_EQ(1, handle.queue().queuedMessages());
  EXPECT_LT(limits.maxQueueBytes, handle.queue().queuedBytes());
  const std::set<std::string> bothKeys{"key1", "key2"};
  EXPECT_EQ(bothKeys, keysOf(co_await handle.queue().pop()));

  registry->fanOut(makePublication({"key3"}), limits);
  EXPECT_EQ(0, registry->overflowDrops());
  EXPECT_EQ(1, handle.queue().queuedMessages());

  registry->fanOut(makePublication({"key4"}), limits);
  EXPECT_EQ(1, registry->overflowDrops());
  EXPECT_EQ(0, registry->size());
  auto overflowed = co_await folly::coro::co_awaitTry(handle.queue().pop());
  EXPECT_TRUE(overflowed.hasException<std::runtime_error>());
}

/*
 * Evictable bytes: the messages of a queue that is behind and that no other
 * queue references.
 */
CO_TEST(KvStoreStreamRegistryTest, EvictableBytesCountExclusiveMessages) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  auto first = add(*registry, 1, allKeysFilter());
  auto second = add(*registry, 2, allKeysFilter());
  auto filtered = add(*registry, 3, keysFilter({"key1"}));

  // One message each: no queue is behind.
  registry->fanOut(makePublication({"key1"}), kTestLimits);
  EXPECT_EQ(0, first.queue().evictableBytes());
  EXPECT_EQ(0, filtered.queue().evictableBytes());

  // Two messages each: first and second share theirs.
  registry->fanOut(makePublication({"key1"}), kTestLimits);
  EXPECT_EQ(0, first.queue().evictableBytes());
  EXPECT_EQ(0, second.queue().evictableBytes());
  EXPECT_EQ(filtered.queue().queuedBytes(), filtered.queue().evictableBytes());

  // Once second popped them, first alone references the shared messages.
  co_await second.queue().pop();
  co_await second.queue().pop();
  EXPECT_EQ(first.queue().queuedBytes(), first.queue().evictableBytes());
}

/*
 * Above the total budget, the queue whose close frees the most bytes closes
 * and leaves the registry, and the total fits again.
 */
CO_TEST(KvStoreStreamRegistryTest, TotalBudgetClosesQueueThatFreesMost) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  auto all = add(*registry, 1, allKeysFilter());
  auto key1Only = add(*registry, 2, keysFilter({"key1"}));
  registry->fanOut(makePublication({"key1", "key2", "key3"}), kTestLimits);
  registry->fanOut(makePublication({"key1", "key2", "key3"}), kTestLimits);
  const auto smallBytes = key1Only.queue().queuedBytes();
  CO_ASSERT_LT(smallBytes, all.queue().queuedBytes());
  auto limits = kTestLimits;
  limits.maxTotalBytes = registry->referencedBytes() - 1;

  // A publication of another area queues nothing and checks the budget.
  thrift::Publication otherArea;
  otherArea.area() = "area2";
  registry->fanOut(otherArea, limits);

  EXPECT_EQ(1, registry->budgetDrops());
  EXPECT_EQ(0, registry->overflowDrops());
  EXPECT_EQ(1, registry->size());
  EXPECT_EQ(smallBytes, registry->referencedBytes());
  auto dropped = co_await folly::coro::co_awaitTry(all.queue().pop());
  CO_ASSERT_TRUE(dropped.hasException<std::runtime_error>());
  EXPECT_THAT(
      dropped.exception().what().toStdString(),
      testing::HasSubstr("total budget"));
  const std::set<std::string> key1Keys{"key1"};
  EXPECT_EQ(key1Keys, keysOf(co_await key1Only.queue().pop()));
}

/*
 * Closing a queue whose messages other queues share frees nothing, so the
 * budget closes the smaller queue that alone references its messages.
 */
CO_TEST(KvStoreStreamRegistryTest, TotalBudgetSkipsSharedMessages) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  auto first = add(*registry, 1, allKeysFilter());
  auto second = add(*registry, 2, allKeysFilter());
  auto filtered = add(*registry, 3, keysFilter({"key1"}));
  registry->fanOut(makePublication({"key1", "key2", "key3"}), kTestLimits);
  registry->fanOut(makePublication({"key1", "key2", "key3"}), kTestLimits);
  const auto sharedBytes = first.queue().queuedBytes();
  CO_ASSERT_LT(filtered.queue().queuedBytes(), sharedBytes);
  auto limits = kTestLimits;
  limits.maxTotalBytes = registry->referencedBytes() - 1;

  thrift::Publication otherArea;
  otherArea.area() = "area2";
  registry->fanOut(otherArea, limits);

  EXPECT_EQ(1, registry->budgetDrops());
  EXPECT_EQ(2, registry->size());
  EXPECT_EQ(sharedBytes, registry->referencedBytes());
  EXPECT_EQ(2, first.queue().queuedMessages());
  EXPECT_EQ(2, second.queue().queuedMessages());
  auto dropped = co_await folly::coro::co_awaitTry(filtered.queue().pop());
  EXPECT_TRUE(dropped.hasException<std::runtime_error>());
}

/*
 * Default-filter subscribers share one message per publication. Above the
 * budget, none of them closes: with one message no queue is behind, and with
 * two, closing one frees nothing. The loop ends without a candidate.
 */
TEST(KvStoreStreamRegistryTest, TotalBudgetSparesSharedDefaultSubscribers) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  std::vector<KvStoreStreamHandle> handles;
  for (int64_t id = 1; id <= 4; ++id) {
    handles.push_back(
        add(*registry,
            id,
            KvStorePublicationFilter({kArea}, thrift::KeyDumpParams{})));
  }
  thrift::Publication large;
  large.area() = kArea;
  large.keyVals()->emplace(
      "key1", createThriftValue(1, "node1", std::string(64 * 1024, 'v')));
  auto limits = kTestLimits;
  limits.maxTotalBytes = 1;

  registry->fanOut(large, limits);
  EXPECT_EQ(0, registry->budgetDrops());
  registry->fanOut(large, limits);

  EXPECT_EQ(0, registry->budgetDrops());
  EXPECT_EQ(4, registry->size());
  for (auto& handle : handles) {
    EXPECT_EQ(2, handle.queue().queuedMessages());
  }
  EXPECT_EQ(handles.front().queue().queuedBytes(), registry->referencedBytes());
}

/*
 * A queue closed at the limit leaves the registry at once and releases its
 * snapshot slot; a slot that tryAdd() reclaimed is not released twice. The
 * handles' later removal changes nothing.
 */
CO_TEST(KvStoreStreamRegistryTest, ClosedQueueLeavesRegistry) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  auto limits = kTestLimits;
  limits.maxSnapshotsInFlight = 2;
  const auto t0 = std::chrono::steady_clock::now();
  const auto timeout = Constants::kKvStoreStreamSnapshotSlotTimeout;
  std::optional<KvStoreStreamHandle> reclaimed;
  std::optional<KvStoreStreamHandle> holding;
  std::optional<KvStoreStreamHandle> unmatched;
  {
    auto first = registry->tryAdd(1, allKeysFilter(), limits, t0);
    auto second = registry->tryAdd(
        2, allKeysFilter(), limits, t0 + std::chrono::seconds(1));
    // The gate is full: the third reclaims the first one's slot.
    auto third =
        registry->tryAdd(3, keysFilter({"none"}), limits, t0 + timeout);
    CO_ASSERT_TRUE(first.hasValue());
    CO_ASSERT_TRUE(second.hasValue());
    CO_ASSERT_TRUE(third.hasValue());
    reclaimed.emplace(std::move(*first));
    holding.emplace(std::move(*second));
    unmatched.emplace(std::move(*third));
  }
  CO_ASSERT_EQ(1, registry->snapshotSlotReclaims());
  CO_ASSERT_EQ(2, registry->snapshotsInFlight());

  registry->fanOut(makePublication({"key1"}), kTestLimits);
  auto tight = kTestLimits;
  tight.maxQueueBytes = 1;
  registry->fanOut(makePublication({"key1"}), tight);

  EXPECT_EQ(2, registry->overflowDrops());
  EXPECT_EQ(1, registry->size());
  EXPECT_EQ(1, registry->snapshotsInFlight());
  reclaimed.reset();
  holding.reset();
  EXPECT_EQ(1, registry->size());
  EXPECT_EQ(1, registry->snapshotsInFlight());
  unmatched.reset();
  EXPECT_EQ(0, registry->size());
  EXPECT_EQ(0, registry->snapshotsInFlight());
}

/*
 * Messages outlive the registry: the last pop of a shared message releases
 * its bytes without touching the destroyed registry (ASAN and TSAN modes
 * check the memory).
 */
CO_TEST(KvStoreStreamRegistryTest, SharedMessageOutlivesRegistry) {
  std::optional<KvStoreStreamHandle> first;
  std::optional<KvStoreStreamHandle> second;
  std::weak_ptr<KvStoreStreamRegistry> weakRegistry;
  {
    auto registry = std::make_shared<KvStoreStreamRegistry>();
    weakRegistry = registry;
    first.emplace(add(*registry, 1, allKeysFilter()));
    second.emplace(add(*registry, 2, allKeysFilter()));
    registry->fanOut(makePublication({"key1"}), kTestLimits);
  }
  CO_ASSERT_TRUE(weakRegistry.expired());

  const std::set<std::string> key1Keys{"key1"};
  EXPECT_EQ(key1Keys, keysOf(co_await first->queue().pop()));
  EXPECT_EQ(key1Keys, keysOf(co_await second->queue().pop()));
  first.reset();
  second.reset();
}

// A message that several queues share charges the budget once.
CO_TEST(KvStoreStreamRegistryTest, SharedMessageChargesBudgetOnce) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  auto first = add(*registry, 1, allKeysFilter());
  auto second = add(*registry, 2, allKeysFilter());

  registry->fanOut(makePublication({"key1"}), kTestLimits);

  const auto messageBytes = first.queue().queuedBytes();
  EXPECT_EQ(messageBytes, second.queue().queuedBytes());
  EXPECT_EQ(messageBytes, registry->referencedBytes());
  co_await first.queue().pop();
  EXPECT_EQ(messageBytes, registry->referencedBytes());
  co_await second.queue().pop();
  EXPECT_EQ(0, registry->referencedBytes());
}

} // namespace openr

int
main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  const folly::Init init(&argc, &argv);
  return RUN_ALL_TESTS();
}
