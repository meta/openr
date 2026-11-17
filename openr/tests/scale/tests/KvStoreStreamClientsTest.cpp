/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <openr/tests/scale/KvStoreStreamClients.h>

#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

#include <fb303/BaseService.h>
#include <folly/CancellationToken.h>
#include <folly/Synchronized.h>
#include <folly/synchronization/SaturatingSemaphore.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp/TApplicationException.h>
#include <thrift/lib/cpp/transport/TTransportException.h>
#include <thrift/lib/cpp2/TrustedServerException.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include <thrift/lib/cpp2/util/ScopedServerInterfaceThread.h>

#include <openr/if/gen-cpp2/OpenrCtrlCpp.h>

namespace openr {

namespace {

using thrift::StreamEndReason;
using thrift::StreamSubscriberState;
using StatsPredicate = std::function<bool(const thrift::StreamClientsStats&)>;

constexpr std::chrono::seconds kWaitTimeout{10};

thrift::Publication
makePublication(const std::vector<std::string>& keys) {
  thrift::Publication pub;
  pub.area() = "0";
  for (const auto& key : keys) {
    thrift::Value value;
    value.version() = 1;
    value.originatorId() = "node1";
    value.value() = "value-" + key;
    pub.keyVals()->emplace(key, std::move(value));
  }
  return pub;
}

// The Thrift Compact size estimate Open/R uses for its stream limits.
int64_t
compactSize(const thrift::Publication& pub) {
  apache::thrift::CompactProtocolWriter writer;
  return pub.serializedSizeZC(&writer);
}

/*
 * OpenrCtrlCpp server double. Serves a fixed snapshot, rejects the next
 * `rejects` subscriptions with LOADSHEDDING, and hands each stream to the
 * test as a publisher.
 */
class FakeOpenrCtrlHandler final : public thrift::OpenrCtrlCppSvIf,
                                   public facebook::fb303::BaseService {
 public:
  using Publisher = apache::thrift::ServerStreamPublisher<thrift::Publication>;

  explicit FakeOpenrCtrlHandler(thrift::Publication snapshot)
      : facebook::fb303::BaseService("fake-openr"),
        snapshot_(std::move(snapshot)) {}

  folly::coro::Task<apache::thrift::ResponseAndServerStream<
      std::vector<thrift::Publication>,
      thrift::Publication>>
  co_subscribeAndGetAreaKvStores(
      std::unique_ptr<thrift::KeyDumpParams> filter,
      std::unique_ptr<std::set<std::string>> areas) override {
    auto state = state_.wlock();
    state->subscribeFilters.push_back(*filter);
    state->subscribeAreas.push_back(*areas);
    if (state->rejects > 0) {
      --state->rejects;
      throw apache::thrift::TrustedServerException::appOverloadError(
          "too many subscribers");
    }
    auto [stream, publisher] =
        apache::thrift::ServerStream<thrift::Publication>::createPublisher(
            [this] { streamEnded_.post(); });
    state->publishers.emplace_back(std::move(publisher));
    co_return {{snapshot_}, std::move(stream)};
  }

  folly::coro::Task<std::unique_ptr<thrift::Publication>>
  co_getKvStoreKeyValsFilteredArea(
      std::unique_ptr<thrift::KeyDumpParams> filter,
      std::unique_ptr<std::string> /* area */) override {
    state_.wlock()->pollFilters.push_back(*filter);
    co_return std::make_unique<thrift::Publication>(snapshot_);
  }

  void
  getRegexCounters(
      std::map<std::string, int64_t>& out,
      std::unique_ptr<std::string> regex) override {
    state_.wlock()->counterRegexes.push_back(*regex);
    out = {{"subscribers.kvstore", 1}};
  }

  void
  setRejects(int rejects) {
    state_.wlock()->rejects = rejects;
  }

  void
  publish(size_t stream, thrift::Publication pub) {
    state_.withRLock(
        [&](const auto& state) { state.publishers.at(stream)->next(pub); });
  }

  void
  fail(size_t stream, const std::string& message) {
    if (auto publisher = take(stream)) {
      std::move(*publisher)
          .complete(folly::make_exception_wrapper<std::runtime_error>(message));
    }
  }

  void
  complete(size_t stream) {
    if (auto publisher = take(stream)) {
      std::move(*publisher).complete();
    }
  }

  // A publisher must complete before it is destroyed.
  void
  completeAll() {
    const auto numStreams = state_.rlock()->publishers.size();
    for (size_t i = 0; i < numStreams; ++i) {
      complete(i);
    }
  }

  // True once a stream ended on the server, by completion or cancellation.
  bool
  waitStreamEnded() {
    return streamEnded_.try_wait_for(kWaitTimeout);
  }

  std::vector<thrift::KeyDumpParams>
  subscribeFilters() const {
    return state_.rlock()->subscribeFilters;
  }

  std::vector<std::set<std::string>>
  subscribeAreas() const {
    return state_.rlock()->subscribeAreas;
  }

  std::vector<thrift::KeyDumpParams>
  pollFilters() const {
    return state_.rlock()->pollFilters;
  }

  std::vector<std::string>
  counterRegexes() const {
    return state_.rlock()->counterRegexes;
  }

 private:
  struct State {
    int rejects{0};
    std::vector<std::optional<Publisher>> publishers;
    std::vector<thrift::KeyDumpParams> subscribeFilters;
    std::vector<std::set<std::string>> subscribeAreas;
    std::vector<thrift::KeyDumpParams> pollFilters;
    std::vector<std::string> counterRegexes;
  };

  std::optional<Publisher>
  take(size_t stream) {
    auto state = state_.wlock();
    return std::exchange(state->publishers.at(stream), std::nullopt);
  }

  const thrift::Publication snapshot_;
  folly::Synchronized<State> state_;
  folly::SaturatingSemaphore<true> streamEnded_;
};

thrift::StreamSubscriberGroup
makeSubscriberGroup(int32_t count = 1) {
  thrift::StreamSubscriberGroup group;
  group.count() = count;
  group.areas() = {"0"};
  group.reconnectDelayMs() = 0;
  return group;
}

thrift::StreamClientsConfig
makeConfig(const thrift::StreamSubscriberGroup& group) {
  thrift::StreamClientsConfig config;
  config.subscribers() = {group};
  return config;
}

thrift::StreamSubscriberChange
makeChange(std::vector<int32_t> ids) {
  thrift::StreamSubscriberChange change;
  change.ids() = std::move(ids);
  return change;
}

} // namespace

class KvStoreStreamClientsTest : public ::testing::Test {
 protected:
  void
  TearDown() override {
    clients_.reset();
    if (fake_) {
      fake_->completeAll();
    }
    server_.reset();
  }

  /*
   * Starts the fake DUT on `port` (0: any free port). streamExpireTime sets
   * its Thrift credit timeout.
   */
  void
  startServer(
      uint16_t port = 0,
      std::optional<std::chrono::milliseconds> streamExpireTime =
          std::nullopt) {
    fake_ = std::make_shared<FakeOpenrCtrlHandler>(snapshot_);
    server_ = std::make_unique<apache::thrift::ScopedServerInterfaceThread>(
        fake_, "::1", port, [=](apache::thrift::ThriftServer& server) {
          if (streamExpireTime) {
            server.setStreamExpireTime(*streamExpireTime);
          }
        });
  }

  void
  stopServer() {
    fake_->completeAll();
    server_.reset();
  }

  // Starts the clients against the fake DUT unless `config` names a DUT.
  void
  startClients(thrift::StreamClientsConfig config) {
    if (!config.dut()->host().has_value()) {
      config.dut()->host() = "::1";
      config.dut()->port() = server_->getPort();
    }
    clients_ = std::make_unique<KvStoreStreamClients>(config, [this] {
      std::lock_guard lock(mutex_);
      statsChanged_.notify_all();
    });
  }

  void
  start(
      const thrift::StreamClientsConfig& config,
      std::optional<std::chrono::milliseconds> streamExpireTime =
          std::nullopt) {
    startServer(/*port=*/0, streamExpireTime);
    startClients(config);
  }

  // Waits until `predicate` holds for the client stats; false at timeout.
  bool
  waitForStats(const StatsPredicate& predicate) {
    std::unique_lock lock(mutex_);
    return statsChanged_.wait_for(
        lock, kWaitTimeout, [&] { return predicate(clients_->getStats()); });
  }

  bool
  waitForSubscriber(
      int32_t id,
      const std::function<bool(const thrift::StreamSubscriberStats&)>&
          predicate) {
    return waitForStats([&](const auto& stats) {
      return predicate(stats.subscribers()->at(id));
    });
  }

  bool
  waitForSubscriptions(int32_t id, int32_t subscriptions) {
    return waitForSubscriber(id, [&](const auto& sub) {
      return *sub.subscriptions() == subscriptions;
    });
  }

  thrift::StreamSubscriberStats
  subscriber(int32_t id) const {
    return clients_->getStats().subscribers()->at(id);
  }

  const thrift::Publication snapshot_{
      makePublication({"adj:node1", "prefix:node1"})};
  std::shared_ptr<FakeOpenrCtrlHandler> fake_;
  std::unique_ptr<apache::thrift::ScopedServerInterfaceThread> server_;
  std::unique_ptr<KvStoreStreamClients> clients_;
  std::mutex mutex_;
  std::condition_variable statsChanged_;
};

/*
 * A subscriber records the snapshot of its subscription, then each stream
 * message, in Compact size estimate bytes and keys.
 */
TEST_F(KvStoreStreamClientsTest, CountsSnapshotAndMessages) {
  start(makeConfig(makeSubscriberGroup()));
  ASSERT_TRUE(waitForSubscriptions(0, 1));

  const std::vector<thrift::Publication> updates{
      makePublication({"adj:a"}), makePublication({"adj:b", "adj:c"})};
  for (const auto& update : updates) {
    fake_->publish(0, update);
  }
  ASSERT_TRUE(waitForSubscriber(
      0, [](const auto& sub) { return *sub.messagesReceived() == 2; }));

  const auto stats = subscriber(0);
  EXPECT_EQ(compactSize(snapshot_), *stats.snapshotBytes());
  EXPECT_EQ(2, *stats.snapshotKeys());
  EXPECT_EQ(
      compactSize(updates.at(0)) + compactSize(updates.at(1)),
      *stats.bytesReceived());
  EXPECT_EQ(3, *stats.keysReceived());
  EXPECT_EQ(StreamSubscriberState::STREAMING, *stats.state());
}

/*
 * Each subscriber sends the group filter and areas, with "{id}" in the
 * senderId replaced by its id.
 */
TEST_F(KvStoreStreamClientsTest, SendsGroupFilter) {
  auto group = makeSubscriberGroup(2);
  group.keyPrefixes() = {"adj:", "prefix:"};
  group.senderId() = "scale-{id}";
  group.ignoreTtl() = false;
  start(makeConfig(group));
  ASSERT_TRUE(waitForSubscriptions(0, 1));
  ASSERT_TRUE(waitForSubscriptions(1, 1));

  std::vector<std::string> senderIds;
  for (const auto& filter : fake_->subscribeFilters()) {
    EXPECT_EQ(*group.keyPrefixes(), *filter.keys());
    EXPECT_FALSE(*filter.ignoreTtl());
    senderIds.push_back(filter.senderId().value_or(""));
  }
  EXPECT_THAT(senderIds, testing::UnorderedElementsAre("scale-0", "scale-1"));
  EXPECT_THAT(
      fake_->subscribeAreas(), testing::Each(std::set<std::string>{"0"}));
}

/*
 * A LOADSHEDDING reject counts as an end reason, and the subscriber
 * subscribes again after its reconnect delay.
 */
TEST_F(KvStoreStreamClientsTest, RetriesAfterLoadshedding) {
  start(makeConfig(makeSubscriberGroup()));
  ASSERT_TRUE(waitForSubscriptions(0, 1));
  fake_->setRejects(1);
  clients_->change([] {
    auto change = makeChange({0});
    change.reconnect() = true;
    return change;
  }());

  ASSERT_TRUE(waitForSubscriptions(0, 2));
  const auto stats = subscriber(0);
  EXPECT_EQ(3, *stats.subscribeAttempts());
  EXPECT_EQ(2, *stats.reconnects());
  const std::map<StreamEndReason, int32_t> expected{
      {StreamEndReason::CLIENT_CANCEL, 1}, {StreamEndReason::LOADSHEDDING, 1}};
  EXPECT_EQ(expected, *stats.endReasons());
}

/*
 * A completed stream ends with COMPLETED, and the subscriber subscribes
 * again with a new snapshot.
 */
TEST_F(KvStoreStreamClientsTest, SubscribesAgainAfterCompletion) {
  start(makeConfig(makeSubscriberGroup()));
  ASSERT_TRUE(waitForSubscriptions(0, 1));

  fake_->complete(0);

  ASSERT_TRUE(waitForSubscriptions(0, 2));
  const auto stats = subscriber(0);
  EXPECT_EQ(1, *stats.reconnects());
  const std::map<StreamEndReason, int32_t> expected{
      {StreamEndReason::COMPLETED, 1}};
  EXPECT_EQ(expected, *stats.endReasons());
  EXPECT_EQ(2, fake_->subscribeFilters().size());
}

/*
 * With a negative reconnect delay, a subscriber stops after its stream ends
 * and keeps the end reason.
 */
TEST_F(KvStoreStreamClientsTest, StopsWithoutReconnectDelay) {
  auto group = makeSubscriberGroup();
  group.reconnectDelayMs() = -1;
  start(makeConfig(group));
  ASSERT_TRUE(waitForSubscriptions(0, 1));

  fake_->complete(0);

  ASSERT_TRUE(waitForSubscriber(0, [](const auto& sub) {
    return *sub.state() == StreamSubscriberState::STOPPED;
  }));
  EXPECT_EQ(StreamEndReason::COMPLETED, *subscriber(0).lastEndReason());
  EXPECT_EQ(1, fake_->subscribeFilters().size());
}

/*
 * A reconnect request cancels the stream and subscribes again without the
 * reconnect delay.
 */
TEST_F(KvStoreStreamClientsTest, ReconnectSkipsReconnectDelay) {
  auto group = makeSubscriberGroup();
  group.reconnectDelayMs() = 3600 * 1000;
  start(makeConfig(group));
  ASSERT_TRUE(waitForSubscriptions(0, 1));

  auto change = makeChange({0});
  change.reconnect() = true;
  clients_->change(change);

  ASSERT_TRUE(waitForSubscriptions(0, 2));
  EXPECT_EQ(StreamEndReason::CLIENT_CANCEL, *subscriber(0).lastEndReason());
}

/*
 * A stalled subscriber with no buffer grants no credit and reads nothing.
 * After resume, it reads the queued message.
 */
TEST_F(KvStoreStreamClientsTest, StalledSubscriberReadsAfterResume) {
  auto group = makeSubscriberGroup();
  group.stalled() = true;
  group.bufferSize() = 0;
  start(makeConfig(group));
  ASSERT_TRUE(waitForSubscriber(0, [](const auto& sub) {
    return *sub.subscriptions() == 1 &&
        *sub.state() == StreamSubscriberState::STALLED;
  }));

  fake_->publish(0, makePublication({"adj:a"}));
  EXPECT_EQ(0, *subscriber(0).messagesReceived());

  auto change = makeChange({0});
  change.stalled() = false;
  clients_->change(change);

  ASSERT_TRUE(waitForSubscriber(
      0, [](const auto& sub) { return *sub.messagesReceived() == 1; }));
  EXPECT_FALSE(*subscriber(0).stalled());
}

/*
 * A stalled subscriber that used its credit loses its stream at the Thrift
 * credit timeout and reports CREDIT_TIMEOUT after resume.
 */
TEST_F(KvStoreStreamClientsTest, ReportsCreditTimeout) {
  auto group = makeSubscriberGroup();
  group.stalled() = true;
  group.bufferSize() = 1;
  group.reconnectDelayMs() = -1;
  start(makeConfig(group), std::chrono::milliseconds(100));
  ASSERT_TRUE(waitForSubscriptions(0, 1));

  fake_->publish(0, makePublication({"adj:a"}));
  ASSERT_TRUE(fake_->waitStreamEnded());
  auto change = makeChange({0});
  change.stalled() = false;
  clients_->change(change);

  ASSERT_TRUE(waitForSubscriber(0, [](const auto& sub) {
    return *sub.state() == StreamSubscriberState::STOPPED;
  }));
  const auto stats = subscriber(0);
  EXPECT_EQ(1, *stats.messagesReceived());
  EXPECT_EQ(StreamEndReason::CREDIT_TIMEOUT, *stats.lastEndReason());
}

// A change applies to the listed subscribers only.
TEST_F(KvStoreStreamClientsTest, ChangesListedSubscribers) {
  start(makeConfig(makeSubscriberGroup(2)));

  auto change = makeChange({1});
  change.readDelayMs() = 50;
  clients_->change(change);

  EXPECT_EQ(0, *subscriber(0).readDelayMs());
  EXPECT_EQ(50, *subscriber(1).readDelayMs());
}

// A change that names an unknown subscriber changes nothing.
TEST_F(KvStoreStreamClientsTest, ChangeRejectsUnknownSubscriber) {
  start(makeConfig(makeSubscriberGroup()));

  auto change = makeChange({0, 1});
  change.readDelayMs() = 50;
  EXPECT_THROW(clients_->change(change), std::invalid_argument);

  EXPECT_EQ(0, *subscriber(0).readDelayMs());
}

// A poller sends its filter and records each response.
TEST_F(KvStoreStreamClientsTest, PollerRecordsResponses) {
  thrift::StreamPollerGroup group;
  group.keyPrefixes() = {"adj:"};
  group.intervalMs() = 0;
  group.senderId() = "poll-{id}";
  thrift::StreamClientsConfig config;
  config.pollers() = {group};
  start(config);

  ASSERT_TRUE(waitForStats(
      [](const auto& stats) { return *stats.pollers()->at(0).polls() >= 2; }));
  const auto stats = clients_->getStats().pollers()->at(0);
  EXPECT_EQ(0, *stats.errors());
  EXPECT_EQ(2, *stats.lastKeys());
  EXPECT_EQ(compactSize(snapshot_), *stats.lastBytes());
  const auto filter = fake_->pollFilters().at(0);
  EXPECT_EQ(*group.keyPrefixes(), *filter.keys());
  EXPECT_EQ("poll-0", filter.senderId().value_or(""));
}

/*
 * Clients that start before the DUT serves count CONNECTION ends, then
 * subscribe once the DUT comes up.
 */
TEST_F(KvStoreStreamClientsTest, SubscribesOnceTheDutComesUp) {
  startServer();
  const auto port = server_->getPort();
  stopServer();
  auto group = makeSubscriberGroup();
  group.reconnectDelayMs() = 50;
  auto config = makeConfig(group);
  config.dut()->host() = "::1";
  config.dut()->port() = port;
  startClients(config);
  ASSERT_TRUE(waitForSubscriber(0, [](const auto& sub) {
    return sub.endReasons()->count(StreamEndReason::CONNECTION) > 0;
  }));

  startServer(port);

  ASSERT_TRUE(waitForSubscriptions(0, 1));
  EXPECT_GE(*subscriber(0).reconnects(), 1);
}

// The DUT counters come from the DUT of the clients, without a session.
TEST_F(KvStoreStreamClientsTest, ReadsDutCounters) {
  start(makeConfig(makeSubscriberGroup(0)));

  const auto counters = clients_->getDutCounters("^subscribers\\.kvstore");

  ASSERT_TRUE(counters.hasValue());
  const std::map<std::string, int64_t> expected{{"subscribers.kvstore", 1}};
  EXPECT_EQ(expected, *counters);
  EXPECT_THAT(
      fake_->counterRegexes(), testing::ElementsAre("^subscribers\\.kvstore"));
}

// A DUT that does not serve fails the counter read.
TEST_F(KvStoreStreamClientsTest, DutCountersFailWhileTheDutIsDown) {
  start(makeConfig(makeSubscriberGroup(0)));
  stopServer();

  EXPECT_TRUE(clients_->getDutCounters("").hasException());
}

namespace {

thrift::StreamClientsConfig
makeConfigWithDut(const thrift::StreamSubscriberGroup& group) {
  auto config = makeConfig(group);
  config.dut()->host() = "::1";
  return config;
}

} // namespace

TEST(KvStoreStreamClientsConfigTest, RejectsNegativeCount) {
  auto group = makeSubscriberGroup();
  group.count() = -1;
  EXPECT_THROW(
      KvStoreStreamClients{makeConfigWithDut(group)}, std::invalid_argument);
}

TEST(KvStoreStreamClientsConfigTest, RejectsUnsetDutHost) {
  EXPECT_THROW(
      KvStoreStreamClients{makeConfig(makeSubscriberGroup())},
      std::invalid_argument);
}

TEST(KvStoreStreamClientsConfigTest, RejectsInvalidDutPort) {
  auto config = makeConfigWithDut(makeSubscriberGroup());
  config.dut()->port() = 0;
  EXPECT_THROW(KvStoreStreamClients{config}, std::invalid_argument);
}

/*
 * Open/R closes a stream with an error message per cause; the subscriber
 * reports the matching end reason.
 */
class KvStoreStreamClientsErrorTest
    : public KvStoreStreamClientsTest,
      public testing::WithParamInterface<
          std::pair<std::string, StreamEndReason>> {};

TEST_P(KvStoreStreamClientsErrorTest, ReportsServerError) {
  const auto& [message, reason] = GetParam();
  auto group = makeSubscriberGroup();
  group.reconnectDelayMs() = -1;
  start(makeConfig(group));
  ASSERT_TRUE(waitForSubscriptions(0, 1));

  fake_->fail(0, message);

  ASSERT_TRUE(waitForSubscriber(0, [](const auto& sub) {
    return *sub.state() == StreamSubscriberState::STOPPED;
  }));
  EXPECT_EQ(reason, *subscriber(0).lastEndReason());
}

INSTANTIATE_TEST_SUITE_P(
    OpenrStreamErrors,
    KvStoreStreamClientsErrorTest,
    testing::Values(
        std::make_pair(
            "KvStore stream queue limit of 33554432 bytes exceeded",
            StreamEndReason::OVERFLOW),
        std::make_pair(
            "KvStore stream total budget of 268435456 bytes exceeded",
            StreamEndReason::BUDGET),
        std::make_pair("publisher terminated", StreamEndReason::SHUTDOWN),
        std::make_pair("unexpected failure", StreamEndReason::OTHER)));

class ClassifyEndTest
    : public testing::TestWithParam<
          std::pair<folly::exception_wrapper, StreamEndReason>> {};

TEST_P(ClassifyEndTest, MapsExceptionToReason) {
  const auto& [ew, reason] = GetParam();
  EXPECT_EQ(reason, KvStoreStreamClients::classifyEnd(ew));
}

INSTANTIATE_TEST_SUITE_P(
    ClientErrors,
    ClassifyEndTest,
    testing::Values(
        std::make_pair(
            folly::make_exception_wrapper<
                apache::thrift::transport::TTransportException>(
                apache::thrift::transport::TTransportException::NOT_OPEN,
                "Connection refused"),
            StreamEndReason::CONNECTION),
        std::make_pair(
            folly::make_exception_wrapper<folly::OperationCancelled>(),
            StreamEndReason::CLIENT_CANCEL),
        std::make_pair(
            folly::make_exception_wrapper<
                apache::thrift::TApplicationException>(
                apache::thrift::TApplicationException::LOADSHEDDING,
                "too many subscribers"),
            StreamEndReason::LOADSHEDDING),
        std::make_pair(
            folly::make_exception_wrapper<std::system_error>(
                std::make_error_code(std::errc::host_unreachable),
                "Failed to resolve address"),
            StreamEndReason::CONNECTION),
        std::make_pair(
            folly::make_exception_wrapper<
                apache::thrift::TApplicationException>(
                apache::thrift::TApplicationException::INTERRUPTION,
                "server closing connection"),
            StreamEndReason::CONNECTION)));

} // namespace openr
