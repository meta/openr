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
#include <vector>

#include <folly/CppAttributes.h>
#include <folly/ExceptionWrapper.h>
#include <folly/Expected.h>
#include <folly/Synchronized.h>
#include <folly/container/F14Map.h>
#include <folly/coro/Task.h>
#include <folly/fibers/Semaphore.h>

#include <openr/if/gen-cpp2/KvStore_types.h>
#include <openr/if/gen-cpp2/OpenrCtrl_types.h>
#include <openr/kvstore/KvStorePublicationFilter.h>

namespace openr {

/*
 * One queued KvStore stream message. The queues of subscribers with the same
 * key filter reference one Publication; `shared` is then true and nothing
 * modifies it once queued. A message that one queue alone references
 * (`shared` false) is moved out by that queue's pop(). `bytes` is its
 * Compact serialized size. Example: about 1100 for a Publication with one
 * 1 KB value.
 */
struct KvStoreStreamMessage {
  std::shared_ptr<thrift::Publication> publication;
  size_t bytes{0};
  bool shared{false};
};

/*
 * Queue of one KvStore stream subscriber. The fan-out pushes without blocking
 * and the stream generator pops. Thread-safe. Open/R reserves no capacity:
 * the queue holds references and grows and shrinks with its entries.
 */
class KvStoreStreamQueue {
 public:
  void push(KvStoreStreamMessage message);

  /*
   * Waits for the next message. Moves the Publication out of a message that
   * is not shared, and copies a shared one. Throws the close reason after
   * close(), and folly::OperationCancelled when the stream is cancelled.
   */
  folly::coro::Task<thrift::Publication> pop();

  // Drops the queued messages. pop() throws `reason` from now on.
  void close(folly::exception_wrapper reason);

  size_t queuedBytes() const;
  size_t queuedMessages() const;
  // Messages that pop() handed to the stream.
  int64_t sentMessages() const;
  // When pop() last handed a message to the stream.
  std::chrono::system_clock::time_point lastSendTime() const;

  /*
   * The Publication of the oldest queued message, or nullptr. Valid until the
   * next pop() or close().
   */
  const thrift::Publication* FOLLY_NULLABLE frontPublicationForTest() const;

 private:
  struct State {
    std::deque<KvStoreStreamMessage> messages;
    size_t bytes{0};
    int64_t sentMessages{0};
    std::chrono::system_clock::time_point lastSendTime;
    folly::exception_wrapper closeReason;
  };

  folly::Synchronized<State> state_;
  // One token per push() and one for close(). co_wait() is cancellable.
  folly::fibers::Semaphore available_{0};
};

struct KvStoreStreamSubscriber {
  KvStoreStreamSubscriber(int64_t id, KvStorePublicationFilter filter);

  const int64_t id;
  const KvStorePublicationFilter filter;
  const std::chrono::steady_clock::time_point subscriptionTime;
  KvStoreStreamQueue queue;
  /*
   * Guarded by the registry lock. Set from admission until Thrift starts the
   * stream (the generator's first pull), or until tryAdd() reclaims the slot.
   */
  bool holdsSnapshotSlot{true};
  // Guarded by the registry lock. When tryAdd() admitted the subscriber.
  std::chrono::steady_clock::time_point admissionTime;
};

// Admission limits. Example: {.maxSnapshotsInFlight = 4, .maxSubscribers = 16}
struct KvStoreStreamLimits {
  size_t maxSnapshotsInFlight{0};
  size_t maxSubscribers{0};
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
   * otherwise. After closeAll(), registers nothing, takes no slot, and
   * returns a handle whose queue is already closed with the closeAll()
   * reason.
   */
  folly::Expected<KvStoreStreamHandle, KvStoreStreamRejection> tryAdd(
      int64_t id,
      KvStorePublicationFilter filter,
      const KvStoreStreamLimits& limits,
      std::chrono::steady_clock::time_point now =
          std::chrono::steady_clock::now());

  /*
   * Adds a reference to `pub`, or to its filtered copy, to the queue of each
   * matching subscriber. Never blocks, and holds the registry lock only to
   * copy the subscriber list. Subscribers with the same key filter
   * (KvStorePublicationFilter::sameKeyFilter()) share one copy.
   */
  void fanOut(const thrift::Publication& pub);

  /*
   * Closes every queue; each stream ends with `reason`. Later tryAdd() calls
   * close their queue with `reason` at once.
   */
  void closeAll(const folly::exception_wrapper& reason);

  size_t size() const;

  size_t snapshotsInFlight() const;

  // Snapshot slots that tryAdd() reclaimed from streams Thrift had not started.
  int64_t
  snapshotSlotReclaims() const {
    return snapshotSlotReclaims_.load();
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
  void remove(int64_t id);
  void releaseSnapshotSlot(int64_t id);

  struct State {
    folly::F14FastMap<int64_t, std::shared_ptr<KvStoreStreamSubscriber>>
        subscribers;
    size_t snapshotsInFlight{0};
    // Set by closeAll().
    folly::exception_wrapper closeReason;
  };

  folly::Synchronized<State> state_;
  std::atomic<int64_t> snapshotSlotReclaims_{0};
};

} // namespace openr
