/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <chrono>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <folly/CppAttributes.h>
#include <folly/ExceptionWrapper.h>
#include <folly/Expected.h>
#include <folly/Synchronized.h>
#include <folly/container/F14Map.h>
#include <folly/coro/Task.h>
#include <folly/fibers/Semaphore.h>
#include <folly/synchronization/SaturatingSemaphore.h>

#include <openr/ctrl-server/KvStoreStreamMergeBuffer.h>
#include <openr/if/gen-cpp2/KvStore_types.h>
#include <openr/if/gen-cpp2/OpenrCtrl_types.h>
#include <openr/kvstore/KvStorePublicationFilter.h>

namespace openr {

/*
 * Admission, memory, merge and slow-client limits. Example:
 * {.maxSnapshotsInFlight = 4, .maxSubscribers = 16, .maxQueueBytes = 32 MiB,
 * .maxTotalBytes = 256 MiB, .mergeBytes = 8 MiB, .mergeExitBytes = 4 MiB,
 * .slowClientBytes = 16 MiB, .slowClientDuration = 300s}.
 */
struct KvStoreStreamLimits {
  size_t maxSnapshotsInFlight{0};
  size_t maxSubscribers{0};
  size_t maxQueueBytes{0};
  size_t maxTotalBytes{0};
  size_t mergeBytes{0};
  size_t mergeExitBytes{0};
  // 0 disables slow-client detection.
  size_t slowClientBytes{0};
  std::chrono::seconds slowClientDuration{0};
};

/*
 * Queue of one KvStore stream subscriber. The fan-out pushes without blocking
 * and the stream generator, its one consumer, pops. Thread-safe. Open/R
 * reserves no capacity: the queue holds references and grows and shrinks
 * with its entries.
 */
class KvStoreStreamQueue {
 public:
  /*
   * `referencedBytes` is the total stream budget of the registry. The merge
   * buffer of this queue charges it directly
   * (KvStoreStreamMergeBuffer::budgetBytes()); a value it points into a
   * shared message stays charged once, by that message.
   */
  explicit KvStoreStreamQueue(
      std::shared_ptr<std::atomic<size_t>> referencedBytes);
  KvStoreStreamQueue(const KvStoreStreamQueue&) = delete;
  KvStoreStreamQueue& operator=(const KvStoreStreamQueue&) = delete;
  KvStoreStreamQueue(KvStoreStreamQueue&&) = delete;
  KvStoreStreamQueue& operator=(KvStoreStreamQueue&&) = delete;
  ~KvStoreStreamQueue() = default;

  enum class PushResult {
    QUEUED,
    /* The queue was closed before; the message is dropped. */
    CLOSED,
    /*
     * The queue held messages and this one would take it above
     * limits.maxQueueBytes; the queue is closed.
     */
    OVERFLOW,
  };

  /*
   * At limits.mergeBytes queued, the queue enters merge mode
   * (KvStoreStreamMergeBuffer): it keeps the latest value of each key and the
   * expiry of each expired key, and drops nodeIds and tobeUpdatedKeys. A
   * value never replaces a queued expiry. pop() then returns merged entries
   * per area, at most about Constants::kKvStoreStreamMergedPopMaxBytes each.
   * After a pop leaves less than limits.mergeExitBytes, the queue returns to
   * publication order; merged entries left are older and pop first. An empty
   * queue accepts any message as it is, whatever its size, and does not
   * enter merge mode.
   *
   * limits.maxQueueBytes, limits.mergeBytes and limits.mergeExitBytes compare
   * serialized bytes (queuedBytes()) in both modes. A merged value counts no
   * more than it takes in the folded messages, and an expiry at most twice
   * as much for keys of 6 characters or more. Entering merge mode at
   * mergeBytes therefore stays below a maxQueueBytes of 4x mergeBytes (the
   * defaults) and does not by itself close the stream.
   */
  PushResult push(
      KvStoreStreamMessage message, const KvStoreStreamLimits& limits);

  /*
   * Waits for the next message. Moves the Publication out of a message that
   * is not shared, and copies a shared one. Throws the close reason after
   * close(), and folly::OperationCancelled when the stream is cancelled.
   */
  folly::coro::Task<thrift::Publication> pop();

  // Drops the queued messages. pop() throws `reason` from now on.
  void close(folly::exception_wrapper reason);

  /*
   * Serialized bytes of the queued messages and merged entries
   * (KvStoreStreamMergeBuffer::bytes(), without the per-entry overhead).
   */
  size_t queuedBytes() const;
  // Queued messages plus merged segments: what pop() still returns at least.
  size_t queuedMessages() const;
  // True after close(), or after push() closed the queue.
  bool isClosed() const;
  bool isMerging() const;
  /*
   * Total-budget bytes that closing the queue would free: its merged entries
   * with their per-entry overhead and the pinned Publications that nothing
   * else references (KvStoreStreamMergeBuffer::evictableBytes()), and its
   * messages that no other queue references. 0 when the queue holds
   * at most one message or segment and is not in merge mode: its client is
   * not behind. Example: 0 for a queue whose messages all-keys subscribers
   * share.
   */
  size_t evictableBytes() const;
  // Messages that pop() handed to the stream.
  int64_t sentMessages() const;
  // When pop() last handed a message to the stream.
  std::chrono::system_clock::time_point lastSendTime() const;

  /*
   * The Publication of the oldest queued message, or nullptr. Valid until the
   * next pop() or close().
   */
  const thrift::Publication* FOLLY_NULLABLE frontPublicationForTest() const;
  // Tokens that pop() can take without waiting.
  size_t
  availableTokensForTest() const {
    return available_.getAvailableTokens();
  }

 private:
  struct State {
    explicit State(std::shared_ptr<std::atomic<size_t>> budget)
        : merged(std::move(budget)) {}

    std::deque<KvStoreStreamMessage> messages;
    size_t messageBytes{0};
    bool merging{false};
    size_t mergeExitBytes{0};
    // Older than `messages`.
    KvStoreStreamMergeBuffer merged;
    int64_t sentMessages{0};
    std::chrono::system_clock::time_point lastSendTime;
    folly::exception_wrapper closeReason;

    size_t
    bytes() const {
      return messageBytes + merged.bytes();
    }
  };

  /*
   * Folds the queued messages into state.merged and returns them, so the
   * caller frees them after it releases the lock.
   */
  static std::deque<KvStoreStreamMessage> enterMergeMode(
      State& state, size_t mergeExitBytes);

  folly::Synchronized<State> state_;
  /*
   * Tokens: at least one per queued message and per merged segment, and one
   * for close(). push() signals when it queues a message or starts a segment;
   * pop() signals again when a segment keeps entries after its chunk.
   * Folding messages into fewer segments leaves surplus tokens: pop() drains
   * them with try_wait() when it finds the queue open and empty under the
   * lock. With one consumer, nothing can be queued then without a token
   * still to come, so the drain loses none. co_wait() is cancellable.
   */
  folly::fibers::Semaphore available_{0};
};

struct KvStoreStreamSubscriber {
  KvStoreStreamSubscriber(
      int64_t id,
      KvStorePublicationFilter filter,
      std::string client,
      std::shared_ptr<std::atomic<size_t>> referencedBytes);

  const int64_t id;
  const KvStorePublicationFilter filter;
  // Peer address and peer CN. Example: "[2401:db00::1]:41234 (pss.example)"
  const std::string client;
  const std::chrono::steady_clock::time_point subscriptionTime;
  KvStoreStreamQueue queue;

  /*
   * Guarded by the registry lock. Set from admission until Thrift starts the
   * stream (the generator's first pull), until the handle is destroyed, or
   * until tryAdd() reclaims the slot. A queue that a limit closes keeps its
   * slot: the handler may still hold its KvStore dump.
   */
  bool holdsSnapshotSlot{true};
  // Guarded by the registry lock. When tryAdd() admitted the subscriber.
  std::chrono::steady_clock::time_point admissionTime;
  /*
   * Guarded by the registry lock. Slow-client state of evaluateSlowClients():
   * since when every check saw the queue at or above the threshold; the
   * messages sent at the previous check; the last check that saw a message
   * sent since the one before.
   */
  std::optional<std::chrono::steady_clock::time_point> aboveSlowSince;
  int64_t sentAtLastCheck{0};
  std::optional<std::chrono::steady_clock::time_point> lastProgressTime;
  bool slow{false};
};

/*
 * Why tryAdd() rejected a subscriber, with the counts it saw. Example:
 * {.reason = SNAPSHOTS_IN_FLIGHT, .snapshotsInFlight = 4, .subscribers = 6}
 */
struct KvStoreStreamRejection {
  enum class Reason {
    // The snapshot gate is full after reclaiming the expired slots.
    SNAPSHOTS_IN_FLIGHT,
    // The subscriber cap is reached.
    SUBSCRIBERS,
    // closeAll() ran: the handler is stopping.
    STOPPED,
  };
  Reason reason{Reason::SUBSCRIBERS};
  size_t snapshotsInFlight{0};
  size_t subscribers{0};
};

class KvStoreStreamRegistry;

/*
 * Owns the registration of one subscriber. The stream generator holds it in
 * its frame by value. The destructor removes the subscriber when the stream
 * ends, when the client cancels it, and when the handler fails before it
 * returns the stream.
 */
class KvStoreStreamHandle {
 public:
  KvStoreStreamHandle(
      std::weak_ptr<KvStoreStreamRegistry> registry,
      std::shared_ptr<KvStoreStreamSubscriber> subscriber);
  KvStoreStreamHandle(KvStoreStreamHandle&& other) noexcept = default;
  KvStoreStreamHandle& operator=(KvStoreStreamHandle&& other) = delete;
  KvStoreStreamHandle(const KvStoreStreamHandle&) = delete;
  KvStoreStreamHandle& operator=(const KvStoreStreamHandle&) = delete;
  ~KvStoreStreamHandle();

  KvStoreStreamQueue&
  queue() {
    return subscriber_->queue;
  }

  /*
   * Releases the snapshot slot of the subscriber. The stream generator calls
   * it at its first pull, when Thrift starts the stream: the snapshot (the
   * first response) is queued for write and the client has credit.
   * Idempotent.
   */
  void releaseSnapshotSlot();

 private:
  std::weak_ptr<KvStoreStreamRegistry> registry_;
  std::shared_ptr<KvStoreStreamSubscriber> subscriber_;
};

/*
 * KvStore stream subscribers of the OpenrCtrl handler. The fan-out runs on
 * the control event base; registration and removal run on Thrift threads.
 * A lock guards the subscriber map and the close reason. The handler owns the
 * registry through a shared_ptr, so a handle that outlives the handler
 * removes nothing.
 */
class KvStoreStreamRegistry
    : public std::enable_shared_from_this<KvStoreStreamRegistry> {
 public:
  /*
   * Registers a subscriber that holds a snapshot slot from admission until
   * Thrift starts its stream, when the snapshots in flight and the
   * subscribers are below `limits`. When the snapshot gate is full, first
   * reclaims the slots admitted at least
   * Constants::kKvStoreStreamSnapshotSlotTimeout before `now`; their
   * subscribers stay registered. Returns the rejection and registers nothing
   * otherwise. After closeAll(), returns the STOPPED rejection, so no stream
   * starts after the handler stopped.
   */
  folly::Expected<KvStoreStreamHandle, KvStoreStreamRejection> tryAdd(
      int64_t id,
      KvStorePublicationFilter filter,
      const KvStoreStreamLimits& limits,
      std::string client = "unknown client",
      std::chrono::steady_clock::time_point now =
          std::chrono::steady_clock::now());

  /*
   * Adds a reference to `pub`, or to its filtered copy, to the queue of each
   * matching subscriber. Never blocks, and holds the registry lock only to
   * copy the subscriber list and to remove closed subscribers. Subscribers with
   * the same key filter (KvStorePublicationFilter::sameKeyFilter()) share one
   * copy. Closes a queue that holds messages and would exceed
   * limits.maxQueueBytes. While the referenced bytes exceed
   * limits.maxTotalBytes, closes the queue with the most evictable bytes
   * (KvStoreStreamQueue::evictableBytes()), and stops when no queue has any. A
   * closed queue leaves the registry at once; its handle's removal is then a
   * no-op.
   */
  void fanOut(
      const thrift::Publication& pub, const KvStoreStreamLimits& limits);

  /*
   * One slow-client check at `now`. A stream enters the slow state when every
   * check for at least limits.slowClientDuration saw its queue at or above
   * limits.slowClientBytes, and its client pulled a message within
   * Constants::kKvStoreStreamSlowClientProgressWindow (seen by comparing
   * consecutive checks). It leaves the state when its queue falls below half
   * of limits.slowClientBytes. Closed queues are skipped, and
   * limits.slowClientBytes == 0 disables detection. Also refreshes the gauges
   * subscribers.kvstore.slow_clients, .queue_occupancy_pct_max and
   * .queued_bytes. The handler calls it every
   * Constants::kKvStoreStreamStatsInterval on the ctrl event base.
   */
  void evaluateSlowClients(
      const KvStoreStreamLimits& limits,
      std::chrono::steady_clock::time_point now);

  /*
   * Closes every queue; each stream ends with `reason`. Later tryAdd() calls
   * close their queue with `reason` at once.
   */
  void closeAll(const folly::exception_wrapper& reason);

  /*
   * After closeAll(), waits up to `timeout` until every handle that tryAdd()
   * returned is destroyed, which happens when its stream ends. True when none
   * is left. Counts the streams whose queue a limit closed: they leave the
   * registry at once, and their stream ends at Thrift's next pull. A stream
   * that Thrift paused ends only when Thrift pulls it again or its connection
   * closes.
   */
  bool waitForStreamsToEnd(std::chrono::milliseconds timeout);

  size_t size() const;

  /*
   * Handles that tryAdd() returned and that are not destroyed: the streams
   * that have not ended, including those whose queue a limit closed.
   */
  size_t openStreams() const;

  size_t snapshotsInFlight() const;

  // Snapshot slots that tryAdd() reclaimed from streams Thrift had not started.
  int64_t
  snapshotSlotReclaims() const {
    return snapshotSlotReclaims_.load();
  }

  // Bytes of the messages that queues reference, each message once.
  size_t
  referencedBytes() const {
    return referencedBytes_->load();
  }

  // Streams closed at the per-stream limit and at the total budget.
  int64_t
  overflowDrops() const {
    return overflowDrops_.load();
  }
  int64_t
  budgetDrops() const {
    return budgetDrops_.load();
  }

  /*
   * Streams in the slow state, and entries into it. Removing a slow stream
   * updates slowClients() at once.
   */
  size_t
  slowClients() const {
    return slowClients_.load();
  }
  int64_t
  slowClientEvents() const {
    return slowClientEvents_.load();
  }

  /*
   * One entry per subscriber. total_streamed_msgs counts the messages that
   * pop() handed to the stream (the publisher path counts every publication
   * offered, filtered out or not). last_msg_sent_time is when pop() last
   * handed a message to Thrift (the publisher path: the fan-out time).
   */
  std::vector<thrift::StreamSubscriberInfo> getSubscriberInfo() const;

 private:
  friend class KvStoreStreamHandle;
  struct State;
  /*
   * Removes the subscriber and its slow state; false when it is gone. A
   * snapshot slot that it holds moves to State::closedSlotHolders.
   */
  bool erase(State& state, int64_t id);
  // Releases the snapshot slot of `subscriber`, if it holds one.
  void releaseSlot(State& state, KvStoreStreamSubscriber& subscriber);
  // Releases the slot of a subscriber that erase() removed, if it holds one.
  void releaseClosedSlot(State& state, int64_t id);
  // Sets the slow streams count and its gauge. Under the registry lock.
  void setSlowClients(State& state, size_t slowClients);
  // Posts `streamsEnded_` once closeAll() ran and no handle is left.
  void postIfStreamsEnded(const State& state);
  // The handle destructor: removes the subscriber if registered, and its
  // handle.
  void remove(int64_t id);
  // Removes the subscribers whose queues fanOut() closed.
  void removeClosed(const std::vector<int64_t>& ids);
  void releaseSnapshotSlot(int64_t id);
  KvStoreStreamMessage makeMessage(thrift::Publication&& pub) const;
  // The message for subscribers with `filter`, or std::nullopt for none.
  std::optional<KvStoreStreamMessage> makeFilteredMessage(
      const KvStorePublicationFilter& filter,
      const thrift::Publication& pub) const;
  void enforceTotalBudget(size_t maxTotalBytes);

  struct State {
    folly::F14FastMap<int64_t, std::shared_ptr<KvStoreStreamSubscriber>>
        subscribers;
    size_t snapshotsInFlight{0};
    /*
     * Subscribers that left `subscribers` with their snapshot slot, until the
     * handle releases the slot or tryAdd() reclaims it.
     */
    folly::F14FastMap<int64_t, std::shared_ptr<KvStoreStreamSubscriber>>
        closedSlotHolders;
    // Subscribers in the slow state.
    size_t slowClients{0};
    // Set by closeAll().
    folly::exception_wrapper closeReason;
    // Handles that tryAdd() returned and that are not destroyed.
    size_t openHandles{0};
  };

  folly::Synchronized<State> state_;
  folly::SaturatingSemaphore</* MayBlock= */ true> streamsEnded_;
  std::atomic<int64_t> snapshotSlotReclaims_{0};

  /*
   * Each message charges its bytes at creation; the deleter of its shared_ptr
   * releases them when the last reference drops. Shared with the deleters.
   */
  const std::shared_ptr<std::atomic<size_t>> referencedBytes_{
      std::make_shared<std::atomic<size_t>>(0)};
  std::atomic<int64_t> overflowDrops_{0};
  std::atomic<int64_t> budgetDrops_{0};
  std::atomic<size_t> slowClients_{0};
  std::atomic<int64_t> slowClientEvents_{0};
};

} // namespace openr
