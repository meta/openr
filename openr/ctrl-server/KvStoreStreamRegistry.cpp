/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <openr/ctrl-server/KvStoreStreamRegistry.h>

#include <algorithm>
#include <optional>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <folly/logging/xlog.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>

#include <fb303/ServiceData.h>

#include <openr/common/Constants.h>
#include <openr/common/Util.h>
#include <openr/if/gen-cpp2/KvStore_types_custom_protocol.h>

namespace openr {

namespace {

constexpr std::string_view kSubscribersCounter{"subscribers.kvstore"};
constexpr std::string_view kSnapshotSlotReclaimsStat{
    "subscribers.kvstore.snapshot_slot_reclaims"};
constexpr std::string_view kQueuedBytesCounter{
    "subscribers.kvstore.queued_bytes"};
constexpr std::string_view kOverflowDropsStat{
    "subscribers.kvstore.overflow_drops"};
constexpr std::string_view kBudgetDropsStat{"subscribers.kvstore.budget_drops"};
constexpr std::string_view kSlowClientsCounter{
    "subscribers.kvstore.slow_clients"};
constexpr std::string_view kSlowClientEventsStat{
    "subscribers.kvstore.slow_client_events"};
constexpr std::string_view kMaxOccupancyCounter{
    "subscribers.kvstore.queue_occupancy_pct_max"};

constexpr std::string_view kMergeModeEntriesStat{
    "subscribers.kvstore.merge_mode_entries"};

template <typename T>
size_t
serializedSize(const T& object) {
  apache::thrift::CompactProtocolWriter writer;
  return object.serializedSizeZC(&writer);
}

/*
 * A Publication that charges the total stream budget while it lives. One
 * allocation holds both; messages reference the Publication through an
 * aliasing shared_ptr.
 */
struct ChargedPublication {
  ChargedPublication(
      thrift::Publication&& pub,
      size_t bytes,
      std::shared_ptr<std::atomic<size_t>> budget)
      : publication(std::move(pub)), bytes(bytes), budget(std::move(budget)) {
    // Charged once construction can no longer throw.
    this->budget->fetch_add(bytes);
  }
  ChargedPublication(const ChargedPublication&) = delete;
  ChargedPublication& operator=(const ChargedPublication&) = delete;
  ChargedPublication(ChargedPublication&&) = delete;
  ChargedPublication& operator=(ChargedPublication&&) = delete;
  ~ChargedPublication() {
    budget->fetch_sub(bytes);
  }

  thrift::Publication publication;
  const size_t bytes;
  const std::shared_ptr<std::atomic<size_t>> budget;
};

} // namespace

KvStoreStreamQueue::KvStoreStreamQueue(
    std::shared_ptr<std::atomic<size_t>> referencedBytes)
    : state_(std::in_place, std::move(referencedBytes)) {}

KvStoreStreamQueue::PushResult
KvStoreStreamQueue::push(
    KvStoreStreamMessage message, const KvStoreStreamLimits& limits) {
  auto result = PushResult::QUEUED;
  // Whether this push added an item for pop(): it then adds a token.
  bool added{false};
  // Destroyed after the lock is released: freeing Publications takes time.
  std::deque<KvStoreStreamMessage> dropped;
  std::optional<KvStoreStreamMergeBuffer> droppedMerged;
  std::deque<KvStoreStreamMessage> folded;
  std::vector<std::shared_ptr<thrift::Publication>> released;
  {
    auto state = state_.wlock();
    if (state->closeReason) {
      return PushResult::CLOSED;
    }
    /*
     * An empty queue accepts any message as it is, without entering merge
     * mode: its client is not behind, and the total budget bounds the memory.
     */
    const bool empty = state->messages.empty() && state->merged.empty();
    if (!empty && !state->merging &&
        state->bytes() + message.bytes >= limits.mergeBytes) {
      folded = enterMergeMode(*state, limits.mergeExitBytes);
    }
    bool overflow{false};
    if (state->merging) {
      added = state->merged.add(message) || added;
      overflow = !empty && state->bytes() > limits.maxQueueBytes;
    } else {
      overflow =
          !empty && state->bytes() + message.bytes > limits.maxQueueBytes;
    }
    if (overflow) {
      state->closeReason =
          folly::make_exception_wrapper<std::runtime_error>(fmt::format(
              "KvStore stream queue limit of {} bytes exceeded",
              limits.maxQueueBytes));
      dropped.swap(state->messages);
      state->messageBytes = 0;
      // Releases the merged bytes when destroyed.
      droppedMerged.emplace(std::move(state->merged));
      result = PushResult::OVERFLOW;
      // The close token.
      added = true;
    } else if (!state->merging) {
      state->messageBytes += message.bytes;
      state->messages.push_back(std::move(message));
      added = true;
    }
    released = state->merged.takeReleased();
  }
  /*
   * The lock is released: `folded`, `dropped`, `droppedMerged` and
   * `released` free their Publications at return, without blocking fanOut()
   * or pop().
   */
  if (added) {
    available_.signal();
  }
  return result;
}

std::deque<KvStoreStreamMessage>
KvStoreStreamQueue::enterMergeMode(State& state, size_t mergeExitBytes) {
  state.merging = true;
  state.mergeExitBytes = mergeExitBytes;
  /*
   * Each message starts at most one segment, so the tokens of the messages
   * cover the segments; the rest are surplus.
   */
  for (const auto& message : state.messages) {
    std::ignore = state.merged.add(message);
  }
  std::deque<KvStoreStreamMessage> folded;
  folded.swap(state.messages);
  state.messageBytes = 0;
  facebook::fb303::fbData->addStatValue(
      kMergeModeEntriesStat, 1, facebook::fb303::COUNT);
  return folded;
}

folly::coro::Task<thrift::Publication>
KvStoreStreamQueue::pop() {
  while (true) {
    co_await available_.co_wait();
    std::optional<thrift::Publication> mergedPub;
    // A segment that keeps entries after its chunk keeps its token.
    bool segmentLeft{false};
    KvStoreStreamMessage message;
    // Pinned Publications that the pop released: freed after the lock.
    std::vector<std::shared_ptr<thrift::Publication>> released;
    {
      auto state = state_.wlock();
      if (state->closeReason) {
        state->closeReason.throw_exception();
      }
      if (!state->merged.empty()) {
        const auto segments = state->merged.segments();
        mergedPub =
            state->merged.pop(Constants::kKvStoreStreamMergedPopMaxBytes);
        segmentLeft = state->merged.segments() == segments;
        released = state->merged.takeReleased();
        if (state->merging && state->bytes() < state->mergeExitBytes) {
          state->merging = false;
        }
      } else if (!state->messages.empty()) {
        message = std::move(state->messages.front());
        state->messages.pop_front();
        state->messageBytes -= message.bytes;
      } else {
        // Surplus tokens of folded messages; see `available_`.
        while (available_.try_wait()) {
        }
        continue;
      }
      ++state->sentMessages;
      state->lastSendTime = std::chrono::system_clock::now();
    }
    if (mergedPub) {
      if (segmentLeft) {
        available_.signal();
      }
      mergedPub->timestamp_ms() = getUnixTimeStampMs();
      co_return std::move(*mergedPub);
    }
    /*
     * Other queues of a shared message may read the Publication on other
     * threads, so copy it. use_count() cannot decide this: it is a relaxed
     * load, so a count of 1 does not order the other owners' reads before a
     * move. A message that is not shared was only ever in this queue, and
     * the queue lock orders the fan-out's reads before this move.
     */
    if (!message.shared) {
      co_return std::move(*message.publication);
    }
    co_return thrift::Publication(*message.publication);
  }
}

void
KvStoreStreamQueue::close(folly::exception_wrapper reason) {
  // Destroyed after the lock is released: freeing Publications takes time.
  std::deque<KvStoreStreamMessage> dropped;
  std::optional<KvStoreStreamMergeBuffer> droppedMerged;
  {
    auto state = state_.wlock();
    if (state->closeReason) {
      return;
    }
    state->closeReason = std::move(reason);
    dropped.swap(state->messages);
    state->messageBytes = 0;
    // Releases the merged bytes when destroyed.
    droppedMerged.emplace(std::move(state->merged));
  }
  available_.signal();
}

size_t
KvStoreStreamQueue::queuedBytes() const {
  return state_.rlock()->bytes();
}

size_t
KvStoreStreamQueue::queuedMessages() const {
  auto state = state_.rlock();
  return state->messages.size() + state->merged.segments();
}

bool
KvStoreStreamQueue::isMerging() const {
  return state_.rlock()->merging;
}

bool
KvStoreStreamQueue::isClosed() const {
  return static_cast<bool>(state_.rlock()->closeReason);
}

size_t
KvStoreStreamQueue::evictableBytes() const {
  auto state = state_.rlock();
  /*
   * A queue in merge mode holds the updates of several publications: it is
   * behind. Closing it frees the budget bytes of its merged entries, the
   * per-entry overhead included, and the pinned Publications that no other
   * queue references.
   */
  if (!state->merging &&
      state->messages.size() + state->merged.segments() <= 1) {
    return 0;
  }
  size_t bytes{state->merged.evictableBytes()};
  for (const auto& message : state->messages) {
    /*
     * use_count() only ranks queues here; nothing reads or moves the
     * Publication based on it. A message that is not shared was only ever
     * in this queue.
     */
    if (!message.shared || message.publication.use_count() == 1) {
      bytes += message.bytes;
    }
  }
  return bytes;
}

int64_t
KvStoreStreamQueue::sentMessages() const {
  return state_.rlock()->sentMessages;
}

std::chrono::system_clock::time_point
KvStoreStreamQueue::lastSendTime() const {
  return state_.rlock()->lastSendTime;
}

const thrift::Publication* FOLLY_NULLABLE
KvStoreStreamQueue::frontPublicationForTest() const {
  auto state = state_.rlock();
  if (state->messages.empty()) {
    return nullptr;
  }
  return state->messages.front().publication.get();
}

KvStoreStreamSubscriber::KvStoreStreamSubscriber(
    int64_t id,
    KvStorePublicationFilter filter,
    std::string client,
    std::shared_ptr<std::atomic<size_t>> referencedBytes)
    : id(id),
      filter(std::move(filter)),
      client(std::move(client)),
      subscriptionTime(std::chrono::steady_clock::now()),
      queue(std::move(referencedBytes)) {}

KvStoreStreamHandle::KvStoreStreamHandle(
    std::weak_ptr<KvStoreStreamRegistry> registry,
    std::shared_ptr<KvStoreStreamSubscriber> subscriber)
    : registry_(std::move(registry)), subscriber_(std::move(subscriber)) {}

void
KvStoreStreamHandle::releaseSnapshotSlot() {
  if (!subscriber_) {
    return;
  }
  if (auto registry = registry_.lock()) {
    registry->releaseSnapshotSlot(subscriber_->id);
  }
}

KvStoreStreamHandle::~KvStoreStreamHandle() {
  if (!subscriber_) {
    return;
  }
  if (auto registry = registry_.lock()) {
    registry->remove(subscriber_->id);
  }
}

folly::Expected<KvStoreStreamHandle, KvStoreStreamRejection>
KvStoreStreamRegistry::tryAdd(
    int64_t id,
    KvStorePublicationFilter filter,
    const KvStoreStreamLimits& limits,
    std::string client,
    std::chrono::steady_clock::time_point now) {
  auto subscriber = std::make_shared<KvStoreStreamSubscriber>(
      id, std::move(filter), std::move(client), referencedBytes_);
  enum class Admission { ADMITTED, REJECTED };
  KvStoreStreamRejection rejection;
  std::vector<int64_t> reclaimed;
  const auto admission = state_.withWLock([&](auto& state) {
    if (state.closeReason) {
      rejection.reason = KvStoreStreamRejection::Reason::STOPPED;
      rejection.subscribers = state.subscribers.size();
      return Admission::REJECTED;
    }
    if (state.snapshotsInFlight >= limits.maxSnapshotsInFlight) {
      /*
       * A client that grants no credit after the first response keeps Thrift
       * from starting its stream; reclaim the slots held that long.
       */
      for (auto& [otherId, other] : state.subscribers) {
        if (other->holdsSnapshotSlot &&
            now - other->admissionTime >=
                Constants::kKvStoreStreamSnapshotSlotTimeout) {
          other->holdsSnapshotSlot = false;
          --state.snapshotsInFlight;
          reclaimed.push_back(otherId);
        }
      }
    }
    rejection.snapshotsInFlight = state.snapshotsInFlight;
    rejection.subscribers = state.subscribers.size();
    if (state.snapshotsInFlight >= limits.maxSnapshotsInFlight) {
      rejection.reason = KvStoreStreamRejection::Reason::SNAPSHOTS_IN_FLIGHT;
      return Admission::REJECTED;
    }
    if (state.subscribers.size() >= limits.maxSubscribers) {
      rejection.reason = KvStoreStreamRejection::Reason::SUBSCRIBERS;
      return Admission::REJECTED;
    }
    ++state.snapshotsInFlight;
    ++state.openHandles;
    subscriber->admissionTime = now;
    state.subscribers.emplace(id, subscriber);
    facebook::fb303::fbData->setCounter(
        kSubscribersCounter, state.subscribers.size());
    return Admission::ADMITTED;
  });
  for (const auto reclaimedId : reclaimed) {
    ++snapshotSlotReclaims_;
    facebook::fb303::fbData->addStatValue(
        kSnapshotSlotReclaimsStat, 1, facebook::fb303::COUNT);
    XLOGF(
        WARNING,
        "KvStore stream-{} lost its snapshot slot: not started within {}s of "
        "admission",
        reclaimedId,
        Constants::kKvStoreStreamSnapshotSlotTimeout.count());
  }
  if (admission == Admission::REJECTED) {
    return folly::makeUnexpected(rejection);
  }
  XLOGF(INFO, "KvStore stream-{} started.", id);
  return KvStoreStreamHandle(weak_from_this(), std::move(subscriber));
}

void
KvStoreStreamRegistry::releaseSnapshotSlot(int64_t id) {
  state_.withWLock([&](auto& state) {
    auto it = state.subscribers.find(id);
    if (it == state.subscribers.end() || !it->second->holdsSnapshotSlot) {
      return;
    }
    it->second->holdsSnapshotSlot = false;
    --state.snapshotsInFlight;
  });
}

bool
KvStoreStreamRegistry::erase(State& state, int64_t id) {
  auto it = state.subscribers.find(id);
  if (it == state.subscribers.end()) {
    return false;
  }
  // A slot that tryAdd() reclaimed no longer counts.
  if (it->second->holdsSnapshotSlot) {
    --state.snapshotsInFlight;
  }
  /*
   * The gauge drops at once, not at the next check. `slowClients` counts the
   * subscribers whose `slow` flag is set; both change under this lock only.
   */
  if (it->second->slow) {
    XDCHECK_GT(state.slowClients, 0);
    setSlowClients(state, state.slowClients > 0 ? state.slowClients - 1 : 0);
  }
  state.subscribers.erase(it);
  facebook::fb303::fbData->setCounter(
      kSubscribersCounter, state.subscribers.size());
  return true;
}

void
KvStoreStreamRegistry::postIfStreamsEnded(const State& state) {
  if (state.closeReason && state.openHandles == 0) {
    streamsEnded_.post();
  }
}

void
KvStoreStreamRegistry::setSlowClients(State& state, size_t slowClients) {
  state.slowClients = slowClients;
  slowClients_ = slowClients;
  facebook::fb303::fbData->setCounter(kSlowClientsCounter, slowClients);
}

void
KvStoreStreamRegistry::remove(int64_t id) {
  const bool registered = state_.withWLock([&](auto& state) {
    const bool erased = erase(state, id);
    XDCHECK_GT(state.openHandles, 0);
    if (state.openHandles > 0) {
      --state.openHandles;
    }
    postIfStreamsEnded(state);
    return erased;
  });
  if (registered) {
    XLOGF(INFO, "KvStore stream-{} ended.", id);
  }
}

void
KvStoreStreamRegistry::removeClosed(const std::vector<int64_t>& ids) {
  if (ids.empty()) {
    return;
  }
  state_.withWLock([&](auto& state) {
    for (const auto id : ids) {
      erase(state, id);
    }
  });
}

KvStoreStreamMessage
KvStoreStreamRegistry::makeMessage(thrift::Publication&& pub) const {
  pub.timestamp_ms() = getUnixTimeStampMs();
  const auto bytes = serializedSize(pub);
  auto owner = std::make_shared<ChargedPublication>(
      std::move(pub), bytes, referencedBytes_);
  auto* publication = &owner->publication;
  return KvStoreStreamMessage{
      std::shared_ptr<thrift::Publication>(std::move(owner), publication),
      bytes};
}

std::optional<KvStoreStreamMessage>
KvStoreStreamRegistry::makeFilteredMessage(
    const KvStorePublicationFilter& filter,
    const thrift::Publication& pub) const {
  if (filter.keepsAllKeys()) {
    return makeMessage(thrift::Publication(pub));
  }
  if (auto filtered = filter.apply(pub)) {
    return makeMessage(std::move(*filtered));
  }
  return std::nullopt;
}

void
KvStoreStreamRegistry::fanOut(
    const thrift::Publication& pub, const KvStoreStreamLimits& limits) {
  /*
   * Filter, copy, serialize and push outside the lock. A subscriber removed
   * meanwhile stays alive through its shared_ptr, and a push to a closed
   * queue is a no-op.
   */
  std::vector<std::shared_ptr<KvStoreStreamSubscriber>> subscribers;
  state_.withRLock([&](const auto& state) {
    subscribers.reserve(state.subscribers.size());
    for (const auto& [_, subscriber] : state.subscribers) {
      subscribers.push_back(subscriber);
    }
  });

  /*
   * Group the matching subscribers by key filter; each group shares one
   * message. A linear search: a node has few subscribers (e.g. 16).
   * `subscribers` keeps the group members alive.
   */
  struct Group {
    std::vector<KvStoreStreamSubscriber*> members;
  };
  std::vector<Group> groups;
  for (const auto& subscriber : subscribers) {
    if (!subscriber->filter.matchesArea(*pub.area())) {
      continue;
    }
    auto it = std::find_if(
        groups.begin(), groups.end(), [&subscriber](const Group& group) {
          return group.members.front()->filter.sameKeyFilter(
              subscriber->filter);
        });
    if (it == groups.end()) {
      it = groups.emplace(groups.end());
    }
    it->members.push_back(subscriber.get());
  }

  std::vector<int64_t> overflowed;
  const auto push = [&](KvStoreStreamSubscriber& member,
                        KvStoreStreamMessage message) {
    if (member.queue.push(std::move(message), limits) ==
        KvStoreStreamQueue::PushResult::OVERFLOW) {
      overflowed.push_back(member.id);
    }
  };
  for (const auto& group : groups) {
    // Dropped at the end of the iteration: the budget counts queued bytes.
    auto message = makeFilteredMessage(group.members.front()->filter, pub);
    if (!message) {
      continue;
    }
    message->shared = group.members.size() > 1;
    for (size_t i = 0; i + 1 < group.members.size(); ++i) {
      push(*group.members[i], *message);
    }
    push(*group.members.back(), std::move(*message));
  }

  for (const auto id : overflowed) {
    ++overflowDrops_;
    facebook::fb303::fbData->addStatValue(
        kOverflowDropsStat, 1, facebook::fb303::COUNT);
    XLOGF(
        WARNING,
        "KvStore stream-{} closed: queue limit of {} bytes exceeded",
        id,
        limits.maxQueueBytes);
  }
  // A closed stream leaves at once: it no longer counts for admission.
  removeClosed(overflowed);
  enforceTotalBudget(limits.maxTotalBytes);
  facebook::fb303::fbData->setCounter(kQueuedBytesCounter, referencedBytes());
}

void
KvStoreStreamRegistry::evaluateSlowClients(
    const KvStoreStreamLimits& limits,
    std::chrono::steady_clock::time_point now) {
  size_t maxOccupancyPct{0};
  std::vector<std::shared_ptr<KvStoreStreamSubscriber>> entered;
  std::vector<std::shared_ptr<KvStoreStreamSubscriber>> left;
  state_.withWLock([&](auto& state) {
    size_t numSlow{0};
    for (auto& [_, subscriber] : state.subscribers) {
      const auto sent = subscriber->queue.sentMessages();
      if (sent > subscriber->sentAtLastCheck) {
        subscriber->lastProgressTime = now;
      }
      subscriber->sentAtLastCheck = sent;
      const auto bytes = subscriber->queue.queuedBytes();
      if (limits.maxQueueBytes > 0) {
        maxOccupancyPct =
            std::max(maxOccupancyPct, bytes * 100 / limits.maxQueueBytes);
      }
      /*
       * A closed queue's stream is ending: it is no longer slow, and it did
       * not recover. fanOut() removes the queues it closes at once; queues
       * that closeAll() closes stay until their stream ends.
       */
      if (limits.slowClientBytes == 0 || subscriber->queue.isClosed()) {
        subscriber->aboveSlowSince.reset();
        subscriber->slow = false;
        continue;
      }
      if (subscriber->slow) {
        // Hysteresis: one check just below the threshold does not end it.
        if (bytes * 2 < limits.slowClientBytes) {
          subscriber->aboveSlowSince.reset();
          subscriber->slow = false;
          left.push_back(subscriber);
        } else {
          ++numSlow;
        }
        continue;
      }
      if (bytes < limits.slowClientBytes) {
        subscriber->aboveSlowSince.reset();
        continue;
      }
      if (!subscriber->aboveSlowSince) {
        subscriber->aboveSlowSince = now;
      }
      // A client that stopped pulling is stalled, not slow.
      const bool pulls = subscriber->lastProgressTime &&
          now - *subscriber->lastProgressTime <=
              Constants::kKvStoreStreamSlowClientProgressWindow;
      if (pulls &&
          now - *subscriber->aboveSlowSince >= limits.slowClientDuration) {
        subscriber->slow = true;
        ++numSlow;
        entered.push_back(subscriber);
      }
    }
    setSlowClients(state, numSlow);
  });

  facebook::fb303::fbData->setCounter(kMaxOccupancyCounter, maxOccupancyPct);
  facebook::fb303::fbData->setCounter(kQueuedBytesCounter, referencedBytes());
  for (const auto& subscriber : entered) {
    ++slowClientEvents_;
    facebook::fb303::fbData->addStatValue(
        kSlowClientEventsStat, 1, facebook::fb303::COUNT);
    XLOGF(
        WARNING,
        "KvStore stream-{} of {} is slow: its queue held at least {} bytes at "
        "every check for {}s, and the client pulled a message within the last "
        "{}s. Filter: {}",
        subscriber->id,
        subscriber->client,
        limits.slowClientBytes,
        limits.slowClientDuration.count(),
        Constants::kKvStoreStreamSlowClientProgressWindow.count(),
        subscriber->filter.describe());
  }
  for (const auto& subscriber : left) {
    XLOGF(
        INFO,
        "KvStore stream-{} of {} left the slow state: its queue fell below {} "
        "bytes",
        subscriber->id,
        subscriber->client,
        limits.slowClientBytes / 2);
  }
}

void
KvStoreStreamRegistry::enforceTotalBudget(size_t maxTotalBytes) {
  /*
   * Each pass closes the queue whose close frees the most bytes. Queues that
   * are not behind, or whose messages other queues also reference, are no
   * candidates: closing them frees nothing. A closed queue leaves the
   * registry, so the loop ends after at most one pass per subscriber.
   */
  while (referencedBytes() > maxTotalBytes) {
    std::shared_ptr<KvStoreStreamSubscriber> candidate;
    size_t candidateBytes{0};
    state_.withRLock([&](const auto& state) {
      for (const auto& [_, subscriber] : state.subscribers) {
        const auto bytes = subscriber->queue.evictableBytes();
        if (bytes > candidateBytes) {
          candidate = subscriber;
          candidateBytes = bytes;
        }
      }
    });
    if (!candidate) {
      /*
       * The remaining bytes belong to messages that pops hold, to queues
       * with one message, or to messages that several queues share.
       */
      return;
    }
    candidate->queue.close(
        folly::make_exception_wrapper<std::runtime_error>(fmt::format(
            "KvStore stream total budget of {} bytes exceeded",
            maxTotalBytes)));
    ++budgetDrops_;
    facebook::fb303::fbData->addStatValue(
        kBudgetDropsStat, 1, facebook::fb303::COUNT);
    XLOGF(
        WARNING,
        "KvStore stream-{} closed: total budget of {} bytes exceeded, "
        "its queue alone referenced {} bytes",
        candidate->id,
        maxTotalBytes,
        candidateBytes);
    removeClosed({candidate->id});
  }
}

void
KvStoreStreamRegistry::closeAll(const folly::exception_wrapper& reason) {
  size_t numStreams{0};
  state_.withWLock([&](auto& state) {
    if (!state.closeReason) {
      state.closeReason = reason;
    }
    for (const auto& [_, subscriber] : state.subscribers) {
      subscriber->queue.close(reason);
    }
    numStreams = state.openHandles;
    postIfStreamsEnded(state);
  });
  XLOGF(INFO, "[Exit] Terminating {} active KvStore stream(s).", numStreams);
}

bool
KvStoreStreamRegistry::waitForStreamsToEnd(std::chrono::milliseconds timeout) {
  return streamsEnded_.try_wait_for(timeout);
}

size_t
KvStoreStreamRegistry::size() const {
  return state_.rlock()->subscribers.size();
}

size_t
KvStoreStreamRegistry::openStreams() const {
  return state_.rlock()->openHandles;
}

size_t
KvStoreStreamRegistry::snapshotsInFlight() const {
  return state_.rlock()->snapshotsInFlight;
}

std::vector<thrift::StreamSubscriberInfo>
KvStoreStreamRegistry::getSubscriberInfo() const {
  std::vector<thrift::StreamSubscriberInfo> infos;
  const auto now = std::chrono::steady_clock::now();
  state_.withRLock([&](const auto& state) {
    for (const auto& [id, subscriber] : state.subscribers) {
      thrift::StreamSubscriberInfo info;
      info.subscriber_id() = id;
      info.uptime() = std::chrono::duration_cast<std::chrono::milliseconds>(
                          now - subscriber->subscriptionTime)
                          .count();
      info.last_msg_sent_time() =
          std::chrono::time_point_cast<std::chrono::milliseconds>(
              subscriber->queue.lastSendTime())
              .time_since_epoch()
              .count();
      info.total_streamed_msgs() = subscriber->queue.sentMessages();
      infos.emplace_back(std::move(info));
    }
  });
  return infos;
}

} // namespace openr
