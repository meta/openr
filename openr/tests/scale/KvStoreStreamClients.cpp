/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <openr/tests/scale/KvStoreStreamClients.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include <fmt/format.h>
#include <folly/CancellationToken.h>
#include <folly/SocketAddress.h>
#include <folly/Synchronized.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Sleep.h>
#include <folly/coro/Task.h>
#include <folly/coro/WithCancellation.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/logging/xlog.h>
#include <thrift/lib/cpp/TApplicationException.h>
#include <thrift/lib/cpp/transport/TTransportException.h>
#include <thrift/lib/cpp/util/EnumUtils.h>
#include <thrift/lib/cpp2/async/RocketClientChannel.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>

#include <openr/if/gen-cpp2/OpenrCtrlCppAsyncClient.h>

namespace openr {

namespace detail {

struct StreamSubscriber {
  /*
   * Settings that change() writes. Each change cancels `wake`, which ends
   * the current stall or read-delay wait; a reconnect also cancels
   * `reconnect`, which ends the current stream. change() replaces each
   * cancelled source with a new one.
   */
  struct Control {
    int32_t readDelayMs{0};
    int32_t bufferSize{0};
    bool stalled{false};
    folly::CancellationSource wake;
    folly::CancellationSource reconnect;
  };

  int32_t id{0};
  thrift::KeyDumpParams filter;
  std::set<std::string> areas;
  int32_t reconnectDelayMs{0};
  folly::EventBase* evb{nullptr};
  folly::Synchronized<Control> control;
  folly::Synchronized<thrift::StreamSubscriberStats> stats;
};

struct StreamPoller {
  int32_t id{0};
  thrift::KeyDumpParams filter;
  std::string area;
  std::chrono::milliseconds interval{0};
  folly::EventBase* evb{nullptr};
  folly::Synchronized<thrift::StreamPollerStats> stats;
};

} // namespace detail

namespace {

using detail::StreamPoller;
using detail::StreamSubscriber;
using thrift::StreamEndReason;
using thrift::StreamSubscriberState;
using Clock = std::chrono::steady_clock;
using Notify = std::function<void()>;
using OpenrCtrlCppClient = apache::thrift::Client<thrift::OpenrCtrlCpp>;

constexpr std::chrono::milliseconds kConnectTimeout{5000};
constexpr std::chrono::milliseconds kRpcTimeout{60000};
constexpr std::chrono::milliseconds kCountersTimeout{10000};
// Upper bound of one wait for a change, a reconnect request or a stop.
constexpr std::chrono::hours kIdleWait{1};
constexpr size_t kMaxIoThreads{8};
constexpr std::string_view kSenderIdToken{"{id}"};

/*
 * Substrings of the stream errors. Open/R sends the queue limit, total
 * budget and shutdown messages; the Thrift server sends the credit timeout,
 * e.g. "Stream credit timeout: chunksInMemory=1".
 */
constexpr std::array<std::pair<std::string_view, StreamEndReason>, 4>
    kEndMessages{{
        {"queue limit", StreamEndReason::OVERFLOW},
        {"total budget", StreamEndReason::BUDGET},
        {"publisher terminated", StreamEndReason::SHUTDOWN},
        {"credit timeout", StreamEndReason::CREDIT_TIMEOUT},
    }};

int64_t
elapsedMs(Clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             Clock::now() - start)
      .count();
}

int64_t
serializedSize(const thrift::Publication& pub) {
  apache::thrift::CompactProtocolWriter writer;
  return pub.serializedSizeZC(&writer);
}

int64_t
numKeys(const thrift::Publication& pub) {
  return pub.keyVals()->size() + pub.expiredKeys()->size();
}

std::string
expandSenderId(std::string senderId, int32_t id) {
  const auto idStr = std::to_string(id);
  for (auto pos = senderId.find(kSenderIdToken); pos != std::string::npos;
       pos = senderId.find(kSenderIdToken, pos + idStr.size())) {
    senderId.replace(pos, kSenderIdToken.size(), idStr);
  }
  return senderId;
}

void
requireNonNegative(int64_t value, std::string_view name) {
  if (value < 0) {
    throw std::invalid_argument(
        fmt::format("{} must not be negative (got {})", name, value));
  }
}

void
validateConfig(const thrift::StreamClientsConfig& config) {
  const auto& dut = *config.dut();
  if (!dut.host().has_value() || dut.host()->empty()) {
    throw std::invalid_argument("dut.host must be set");
  }
  if (*dut.port() < 1 || *dut.port() > 65535) {
    throw std::invalid_argument(
        fmt::format("dut.port must be in 1-65535 (got {})", *dut.port()));
  }
  for (const auto& group : *config.subscribers()) {
    requireNonNegative(*group.count(), "subscribers.count");
    requireNonNegative(*group.readDelayMs(), "subscribers.readDelayMs");
    requireNonNegative(*group.bufferSize(), "subscribers.bufferSize");
  }
  for (const auto& group : *config.pollers()) {
    requireNonNegative(*group.count(), "pollers.count");
    requireNonNegative(*group.intervalMs(), "pollers.intervalMs");
  }
}

/*
 * Resolves the DUT host on each call, so a client can start before the DUT
 * name resolves. Throws std::system_error when the lookup fails.
 */
std::unique_ptr<OpenrCtrlCppClient>
makeClient(folly::EventBase* evb, const thrift::DutConnection& dut) {
  const folly::SocketAddress address(
      *dut.host(),
      static_cast<uint16_t>(*dut.port()),
      /*allowNameLookup=*/true);
  auto socket =
      folly::AsyncSocket::newSocket(evb, address, kConnectTimeout.count());
  return std::make_unique<OpenrCtrlCppClient>(
      apache::thrift::RocketClientChannel::newChannel(std::move(socket)));
}

// Returns at the end of `duration` or when `token` is cancelled.
folly::coro::Task<void>
sleepFor(std::chrono::milliseconds duration, folly::CancellationToken token) {
  co_await folly::coro::co_withCancellation(
      std::move(token), folly::coro::sleepReturnEarlyOnCancel(duration));
}

template <typename Stats, typename Fn>
void
updateStats(folly::Synchronized<Stats>& stats, const Notify& notify, Fn&& fn) {
  stats.withWLock(std::forward<Fn>(fn));
  if (notify) {
    notify();
  }
}

void
setState(
    StreamSubscriber& sub, StreamSubscriberState state, const Notify& notify) {
  const bool changed = sub.stats.withWLock([&](auto& stats) {
    if (*stats.state() == state) {
      return false;
    }
    stats.state() = state;
    return true;
  });
  if (changed && notify) {
    notify();
  }
}

void
recordEnd(
    StreamSubscriber& sub,
    StreamEndReason reason,
    std::string error,
    const Notify& notify) {
  XLOGF(
      INFO,
      "[STREAM-CLIENT] Subscriber {} stream ended: {} {}",
      sub.id,
      apache::thrift::util::enumNameSafe(reason),
      error);
  updateStats(sub.stats, notify, [&](auto& stats) {
    stats.lastEndReason() = reason;
    stats.lastError() = std::move(error);
    auto& endReasons = stats.endReasons().value();
    endReasons[reason] += 1;
  });
}

void
recordEnd(
    StreamSubscriber& sub,
    const folly::exception_wrapper& ew,
    const Notify& notify) {
  recordEnd(
      sub,
      KvStoreStreamClients::classifyEnd(ew),
      ew.what().toStdString(),
      notify);
}

void
recordSnapshot(
    StreamSubscriber& sub,
    const std::vector<thrift::Publication>& snapshot,
    int64_t latencyMs,
    const Notify& notify) {
  int64_t bytes{0};
  int64_t keys{0};
  for (const auto& pub : snapshot) {
    bytes += serializedSize(pub);
    keys += numKeys(pub);
  }
  updateStats(sub.stats, notify, [&](auto& stats) {
    stats.subscriptions() = stats.subscriptions().value() + 1;
    stats.snapshotBytes() = bytes;
    stats.snapshotKeys() = keys;
    stats.snapshotLatencyMs() = latencyMs;
  });
}

void
recordMessage(
    StreamSubscriber& sub,
    const thrift::Publication& pub,
    const Notify& notify) {
  const auto bytes = serializedSize(pub);
  const auto keys = numKeys(pub);
  updateStats(sub.stats, notify, [&](auto& stats) {
    stats.messagesReceived() = stats.messagesReceived().value() + 1;
    stats.bytesReceived() = stats.bytesReceived().value() + bytes;
    stats.keysReceived() = stats.keysReceived().value() + keys;
  });
}

/*
 * Returns true once the subscriber may read, false when `cancel` is
 * cancelled first.
 */
folly::coro::Task<bool>
waitWhileStalled(
    StreamSubscriber& sub,
    folly::CancellationToken cancel,
    const Notify& notify) {
  while (!cancel.isCancellationRequested()) {
    auto [stalled, wake] = sub.control.withRLock([](const auto& control) {
      return std::make_pair(control.stalled, control.wake.getToken());
    });
    setState(
        sub,
        stalled ? StreamSubscriberState::STALLED
                : StreamSubscriberState::STREAMING,
        notify);
    if (!stalled) {
      co_return true;
    }
    co_await sleepFor(
        kIdleWait, folly::cancellation_token_merge(std::move(wake), cancel));
  }
  co_return false;
}

folly::coro::Task<void>
waitReadDelay(StreamSubscriber& sub, folly::CancellationToken cancel) {
  auto [delayMs, wake] = sub.control.withRLock([](const auto& control) {
    return std::make_pair(control.readDelayMs, control.wake.getToken());
  });
  if (delayMs > 0) {
    co_await sleepFor(
        std::chrono::milliseconds(delayMs),
        folly::cancellation_token_merge(std::move(wake), std::move(cancel)));
  }
}

/*
 * One subscription: subscribe RPC, then stream reads until the stream ends,
 * the subscriber is stopped or a reconnect is requested.
 */
folly::coro::Task<void>
subscribeOnce(
    StreamSubscriber& sub,
    const thrift::DutConnection& dut,
    const Notify& notify,
    folly::CancellationToken reconnect) {
  const auto cancel = folly::cancellation_token_merge(
      co_await folly::coro::co_current_cancellation_token,
      std::move(reconnect));
  const auto bufferSize = sub.control.rlock()->bufferSize;
  updateStats(sub.stats, notify, [](auto& stats) {
    stats.state() = StreamSubscriberState::CONNECTING;
    stats.subscribeAttempts() = stats.subscribeAttempts().value() + 1;
    if (*stats.subscribeAttempts() > 1) {
      stats.reconnects() = stats.reconnects().value() + 1;
    }
  });

  // Declared before the stream: the stream needs the channel.
  auto client = folly::makeTryWith([&] { return makeClient(sub.evb, dut); });
  if (client.hasException()) {
    recordEnd(sub, client.exception(), notify);
    co_return;
  }
  apache::thrift::RpcOptions options;
  options.setTimeout(kRpcTimeout);
  options.setChunkBufferSize(bufferSize);
  const auto start = Clock::now();
  auto response = co_await folly::coro::co_awaitTry(
      folly::coro::co_withCancellation(
          cancel,
          (*client)->co_subscribeAndGetAreaKvStores(
              options, sub.filter, sub.areas)));
  if (response.hasException()) {
    recordEnd(sub, response.exception(), notify);
    co_return;
  }
  recordSnapshot(sub, response->response, elapsedMs(start), notify);

  auto stream = std::move(response->stream).toAsyncGenerator();
  while (co_await waitWhileStalled(sub, cancel, notify)) {
    auto message = co_await folly::coro::co_awaitTry(
        folly::coro::co_withCancellation(cancel, stream.next()));
    if (message.hasException()) {
      recordEnd(sub, message.exception(), notify);
      co_return;
    }
    auto& next = message.value();
    if (!next.has_value()) {
      recordEnd(sub, StreamEndReason::COMPLETED, "", notify);
      co_return;
    }
    recordMessage(sub, *next, notify);
    co_await waitReadDelay(sub, cancel);
  }
  recordEnd(sub, StreamEndReason::CLIENT_CANCEL, "", notify);
}

folly::coro::Task<void>
runSubscriber(
    StreamSubscriber& sub,
    const thrift::DutConnection& dut,
    const Notify& notify) {
  const auto stop = co_await folly::coro::co_current_cancellation_token;
  while (!stop.isCancellationRequested()) {
    auto reconnect = sub.control.rlock()->reconnect.getToken();
    auto result = co_await folly::coro::co_awaitTry(
        subscribeOnce(sub, dut, notify, reconnect));
    if (result.hasException()) {
      recordEnd(sub, result.exception(), notify);
    }
    /*
     * A reconnect request cancels `reconnect`: it ends the current stream
     * and then skips the wait below.
     */
    const auto resume = folly::cancellation_token_merge(stop, reconnect);
    if (sub.reconnectDelayMs >= 0) {
      setState(sub, StreamSubscriberState::WAITING, notify);
      co_await sleepFor(
          std::chrono::milliseconds(sub.reconnectDelayMs), resume);
      continue;
    }
    setState(sub, StreamSubscriberState::STOPPED, notify);
    while (!resume.isCancellationRequested()) {
      co_await sleepFor(kIdleWait, resume);
    }
  }
  setState(sub, StreamSubscriberState::STOPPED, notify);
}

void
recordPoll(
    StreamPoller& poller,
    const folly::Try<thrift::Publication>& pub,
    int64_t latencyMs,
    const Notify& notify) {
  updateStats(poller.stats, notify, [&](auto& stats) {
    stats.polls() = stats.polls().value() + 1;
    stats.lastLatencyMs() = latencyMs;
    stats.maxLatencyMs() = std::max(*stats.maxLatencyMs(), latencyMs);
    if (pub.hasException()) {
      stats.errors() = stats.errors().value() + 1;
      stats.lastError() = pub.exception().what().toStdString();
      return;
    }
    stats.lastBytes() = serializedSize(*pub);
    stats.lastKeys() = numKeys(*pub);
  });
}

folly::coro::Task<thrift::Publication>
poll(
    StreamPoller& poller,
    const thrift::DutConnection& dut,
    std::unique_ptr<OpenrCtrlCppClient>& client) {
  if (!client) {
    client = makeClient(poller.evb, dut);
  }
  apache::thrift::RpcOptions options;
  options.setTimeout(kRpcTimeout);
  co_return co_await client->co_getKvStoreKeyValsFilteredArea(
      options, poller.filter, poller.area);
}

folly::coro::Task<void>
runPoller(
    StreamPoller& poller,
    const thrift::DutConnection& dut,
    const Notify& notify) {
  const auto stop = co_await folly::coro::co_current_cancellation_token;
  std::unique_ptr<OpenrCtrlCppClient> client;
  while (!stop.isCancellationRequested()) {
    const auto start = Clock::now();
    auto pub = co_await folly::coro::co_awaitTry(poll(poller, dut, client));
    if (stop.isCancellationRequested()) {
      break;
    }
    recordPoll(poller, pub, elapsedMs(start), notify);
    if (pub.hasException() &&
        KvStoreStreamClients::classifyEnd(pub.exception()) ==
            StreamEndReason::CONNECTION) {
      client.reset();
    }
    co_await sleepFor(poller.interval, stop);
  }
}

std::unique_ptr<StreamSubscriber>
makeSubscriber(int32_t id, const thrift::StreamSubscriberGroup& group) {
  auto sub = std::make_unique<StreamSubscriber>();
  sub->id = id;
  sub->filter.keys() = *group.keyPrefixes();
  sub->filter.ignoreTtl() = *group.ignoreTtl();
  if (group.senderId().has_value()) {
    sub->filter.senderId() = expandSenderId(*group.senderId(), id);
  }
  sub->areas = *group.areas();
  sub->reconnectDelayMs = *group.reconnectDelayMs();
  sub->control.withWLock([&](auto& control) {
    control.readDelayMs = *group.readDelayMs();
    control.bufferSize = *group.bufferSize();
    control.stalled = *group.stalled();
  });
  sub->stats.withWLock([&](auto& stats) {
    stats.id() = id;
    stats.readDelayMs() = *group.readDelayMs();
    stats.bufferSize() = *group.bufferSize();
    stats.stalled() = *group.stalled();
  });
  return sub;
}

std::unique_ptr<StreamPoller>
makePoller(int32_t id, const thrift::StreamPollerGroup& group) {
  auto poller = std::make_unique<StreamPoller>();
  poller->id = id;
  poller->filter.keys() = *group.keyPrefixes();
  if (group.senderId().has_value()) {
    poller->filter.senderId() = expandSenderId(*group.senderId(), id);
  }
  poller->area = *group.area();
  poller->interval = std::chrono::milliseconds(*group.intervalMs());
  poller->stats.wlock()->id() = id;
  return poller;
}

folly::coro::Task<std::map<std::string, int64_t>>
readDutCounters(
    folly::EventBase* evb, thrift::DutConnection dut, std::string regex) {
  auto client = makeClient(evb, dut);
  apache::thrift::RpcOptions options;
  options.setTimeout(kCountersTimeout);
  co_return co_await client->co_getRegexCounters(options, regex);
}

} // namespace

KvStoreStreamClients::KvStoreStreamClients(
    const thrift::StreamClientsConfig& config,
    std::function<void()> onStatsChange)
    : dut_(*config.dut()), onStatsChange_(std::move(onStatsChange)) {
  validateConfig(config);
  for (const auto& group : *config.subscribers()) {
    for (int32_t i = 0; i < *group.count(); ++i) {
      subscribers_.push_back(
          makeSubscriber(static_cast<int32_t>(subscribers_.size()), group));
    }
  }
  for (const auto& group : *config.pollers()) {
    for (int32_t i = 0; i < *group.count(); ++i) {
      pollers_.push_back(
          makePoller(static_cast<int32_t>(pollers_.size()), group));
    }
  }

  const auto numClients = subscribers_.size() + pollers_.size();
  ioExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(
      std::clamp<size_t>(numClients, 1, kMaxIoThreads));
  for (auto& sub : subscribers_) {
    sub->evb = ioExecutor_->getEventBase();
    scope_.add(
        folly::coro::co_withExecutor(
            folly::getKeepAliveToken(sub->evb),
            runSubscriber(*sub, dut_, onStatsChange_)));
  }
  for (auto& poller : pollers_) {
    poller->evb = ioExecutor_->getEventBase();
    scope_.add(
        folly::coro::co_withExecutor(
            folly::getKeepAliveToken(poller->evb),
            runPoller(*poller, dut_, onStatsChange_)));
  }
  XLOGF(
      INFO,
      "[STREAM-CLIENT] Started {} subscribers and {} pollers against {}:{}",
      subscribers_.size(),
      pollers_.size(),
      *dut_.host(),
      *dut_.port());
}

KvStoreStreamClients::~KvStoreStreamClients() {
  /*
   * Synchronous adapter: the scale test server handler and its Session are
   * synchronous. The clients finish on their IO threads, which close their
   * connections there.
   */
  folly::coro::blockingWait(scope_.cancelAndJoinAsync());
  XLOG(INFO, "[STREAM-CLIENT] Stopped all clients");
}

void
KvStoreStreamClients::change(const thrift::StreamSubscriberChange& change) {
  if (change.readDelayMs().has_value()) {
    requireNonNegative(*change.readDelayMs(), "readDelayMs");
  }
  if (change.bufferSize().has_value()) {
    requireNonNegative(*change.bufferSize(), "bufferSize");
  }
  std::vector<StreamSubscriber*> targets;
  for (const auto id : *change.ids()) {
    if (id < 0 || static_cast<size_t>(id) >= subscribers_.size()) {
      throw std::invalid_argument(fmt::format("Unknown subscriber id {}", id));
    }
    targets.push_back(subscribers_.at(id).get());
  }
  if (targets.empty()) {
    for (auto& sub : subscribers_) {
      targets.push_back(sub.get());
    }
  }

  for (auto* sub : targets) {
    std::vector<folly::CancellationSource> cancelled;
    sub->control.withWLock([&](auto& control) {
      control.readDelayMs = change.readDelayMs().value_or(control.readDelayMs);
      control.bufferSize = change.bufferSize().value_or(control.bufferSize);
      control.stalled = change.stalled().value_or(control.stalled);
      cancelled.push_back(std::exchange(control.wake, {}));
      if (*change.reconnect()) {
        cancelled.push_back(std::exchange(control.reconnect, {}));
      }
      sub->stats.withWLock([&](auto& stats) {
        stats.readDelayMs() = control.readDelayMs;
        stats.bufferSize() = control.bufferSize;
        stats.stalled() = control.stalled;
      });
    });
    for (auto& source : cancelled) {
      source.requestCancellation();
    }
  }
}

thrift::StreamClientsStats
KvStoreStreamClients::getStats() const {
  thrift::StreamClientsStats out;
  out.running() = true;
  for (const auto& sub : subscribers_) {
    out.subscribers()->push_back(sub->stats.copy());
  }
  for (const auto& poller : pollers_) {
    out.pollers()->push_back(poller->stats.copy());
  }
  return out;
}

folly::Try<std::map<std::string, int64_t>>
KvStoreStreamClients::getDutCounters(const std::string& regex) {
  auto* evb = ioExecutor_->getEventBase();
  // Synchronous adapter, as the destructor.
  return folly::makeTryWith([&] {
    return folly::coro::blockingWait(
        folly::coro::co_withExecutor(
            folly::getKeepAliveToken(evb), readDutCounters(evb, dut_, regex)));
  });
}

thrift::StreamEndReason
KvStoreStreamClients::classifyEnd(const folly::exception_wrapper& ew) {
  using apache::thrift::TApplicationException;
  if (ew.is_compatible_with<folly::OperationCancelled>()) {
    return StreamEndReason::CLIENT_CANCEL;
  }
  if (ew.is_compatible_with<apache::thrift::transport::TTransportException>() ||
      ew.is_compatible_with<std::system_error>()) {
    return StreamEndReason::CONNECTION;
  }
  if (const auto* ex = ew.get_exception<TApplicationException>()) {
    switch (ex->getType()) {
    case TApplicationException::LOADSHEDDING:
      return StreamEndReason::LOADSHEDDING;
    case TApplicationException::INTERRUPTION:
      return StreamEndReason::CONNECTION;
    default:
      break;
    }
  }
  const auto what = ew.what().toStdString();
  for (const auto& [text, reason] : kEndMessages) {
    if (what.find(text) != std::string::npos) {
      return reason;
    }
  }
  return StreamEndReason::OTHER;
}

} // namespace openr
