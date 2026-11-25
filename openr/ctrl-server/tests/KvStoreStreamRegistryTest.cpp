/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

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
#include <gtest/gtest.h>

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

// Keeps all keys: the default params set ignoreTtl.
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
  auto all1 = registry->add(1, allKeysFilter());
  auto all2 = registry->add(2, allKeysFilter());
  auto filtered = registry->add(3, keysFilter({"prefix:a"}));
  auto otherArea = registry->add(
      4, KvStorePublicationFilter({"area2"}, thrift::KeyDumpParams{}));

  registry->fanOut(makePublication({"prefix:a", "adj:b"}));

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
  auto handle = registry->add(1, allKeysFilter());
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
    registry->fanOut(pub);
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
  auto prefix1 = registry->add(1, keysFilter({"prefix:a"}));
  auto prefix2 = registry->add(
      2, KvStorePublicationFilter({kArea, "area2"}, prefixParams));
  auto adj = registry->add(3, keysFilter({"adj:b"}));
  // Default params ignore TTL updates, so they do not keep all keys.
  auto default1 = registry->add(4, KvStorePublicationFilter({kArea}, {}));
  auto default2 = registry->add(5, KvStorePublicationFilter({kArea}, {}));
  auto all1 = registry->add(6, allKeysFilter());
  auto all2 = registry->add(7, allKeysFilter());
  auto none1 = registry->add(8, keysFilter({"none"}));
  auto none2 = registry->add(9, keysFilter({"none"}));

  registry->fanOut(makePublication({"prefix:a", "adj:b"}));

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
  auto filtered = registry->add(1, keysFilter({"prefix:a"}));

  registry->fanOut(makePublication({"adj:b"}));

  EXPECT_EQ(0, filtered.queue().queuedMessages());
  EXPECT_EQ(0, filtered.queue().queuedBytes());
}

// pop() suspends on an empty queue and resumes at the next publication.
CO_TEST(KvStoreStreamRegistryTest, PopWaitsForFanOut) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  auto handle = registry->add(1, allKeysFilter());

  auto [pub, _] = co_await folly::coro::collectAll(
      handle.queue().pop(), [&]() -> folly::coro::Task<void> {
        registry->fanOut(makePublication({"key1"}));
        co_return;
      }());

  EXPECT_EQ(std::set<std::string>{"key1"}, keysOf(pub));
}

// Queued bytes follow the queued messages; sent messages count the pops.
CO_TEST(KvStoreStreamRegistryTest, QueuedBytesFollowMessages) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  auto handle = registry->add(1, allKeysFilter());

  registry->fanOut(makePublication({"key1"}));
  registry->fanOut(makePublication({"key1", "key2"}));
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
  auto handle = registry->add(1, allKeysFilter());
  registry->fanOut(makePublication({"key1"}));

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
  auto handle = registry->add(1, allKeysFilter());

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
  auto handle = registry->add(1, allKeysFilter());
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
 * A subscriber that registers after closeAll(), as when a subscription
 * races with the handler stop, is not registered and its pop() ends with
 * the closeAll() reason.
 */
CO_TEST(KvStoreStreamRegistryTest, AddAfterCloseAllClosesQueue) {
  auto registry = std::make_shared<KvStoreStreamRegistry>();
  registry->closeAll(terminated());

  auto handle = registry->add(1, allKeysFilter());
  registry->fanOut(makePublication({"key1"}));

  EXPECT_EQ(0, registry->size());
  EXPECT_EQ(0, handle.queue().queuedMessages());
  auto result = co_await folly::coro::co_awaitTry(handle.queue().pop());
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
                   registry->add(id, std::move(filter)),
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
      registry->fanOut(makeSequencedPublication(seq));
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
    auto handle = registry->add(1, allKeysFilter());
    auto moved = std::move(handle);
    EXPECT_EQ(1, registry->size());
  }
  EXPECT_EQ(0, registry->size());

  registry->fanOut(makePublication({"key1"}));
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
    handle.emplace(registry->add(1, allKeysFilter()));
    registry->fanOut(makePublication({"key1"}));
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
  auto sender = registry->add(7, allKeysFilter());
  auto filtered = registry->add(8, keysFilter({"prefix:a"}));
  registry->fanOut(makePublication({"adj:b"}));
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

} // namespace openr

int
main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  const folly::Init init(&argc, &argv);
  return RUN_ALL_TESTS();
}
