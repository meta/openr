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
#include <utility>
#include <vector>

#include <folly/logging/xlog.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>

#include <fb303/ServiceData.h>

#include <openr/common/Util.h>
#include <openr/if/gen-cpp2/KvStore_types_custom_protocol.h>

namespace openr {

namespace {

constexpr std::string_view kSubscribersCounter{"subscribers.kvstore"};

size_t
serializedSize(const thrift::Publication& pub) {
  apache::thrift::CompactProtocolWriter writer;
  return pub.serializedSizeZC(&writer);
}

KvStoreStreamMessage
makeMessage(thrift::Publication&& pub) {
  pub.timestamp_ms() = getUnixTimeStampMs();
  auto publication = std::make_shared<thrift::Publication>(std::move(pub));
  const auto bytes = serializedSize(*publication);
  return KvStoreStreamMessage{std::move(publication), bytes};
}

// The message for subscribers with `filter`, or std::nullopt for no message.
std::optional<KvStoreStreamMessage>
makeFilteredMessage(
    const KvStorePublicationFilter& filter, const thrift::Publication& pub) {
  if (filter.keepsAllKeys()) {
    return makeMessage(thrift::Publication(pub));
  }
  if (auto filtered = filter.apply(pub)) {
    return makeMessage(std::move(*filtered));
  }
  return std::nullopt;
}

} // namespace

void
KvStoreStreamQueue::push(KvStoreStreamMessage message) {
  {
    auto state = state_.wlock();
    if (state->closeReason) {
      return;
    }
    state->bytes += message.bytes;
    state->messages.push_back(std::move(message));
  }
  available_.signal();
}

folly::coro::Task<thrift::Publication>
KvStoreStreamQueue::pop() {
  while (true) {
    co_await available_.co_wait();
    KvStoreStreamMessage message;
    {
      auto state = state_.wlock();
      if (state->closeReason) {
        state->closeReason.throw_exception();
      }
      if (state->messages.empty()) {
        continue;
      }
      message = std::move(state->messages.front());
      state->messages.pop_front();
      state->bytes -= message.bytes;
      ++state->sentMessages;
      state->lastSendTime = std::chrono::system_clock::now();
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
  {
    auto state = state_.wlock();
    if (state->closeReason) {
      return;
    }
    state->closeReason = std::move(reason);
    state->messages.clear();
    state->bytes = 0;
  }
  available_.signal();
}

size_t
KvStoreStreamQueue::queuedBytes() const {
  return state_.rlock()->bytes;
}

size_t
KvStoreStreamQueue::queuedMessages() const {
  return state_.rlock()->messages.size();
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
    int64_t id, KvStorePublicationFilter filter)
    : id(id),
      filter(std::move(filter)),
      subscriptionTime(std::chrono::steady_clock::now()) {}

KvStoreStreamHandle::KvStoreStreamHandle(
    std::weak_ptr<KvStoreStreamRegistry> registry,
    std::shared_ptr<KvStoreStreamSubscriber> subscriber)
    : registry_(std::move(registry)), subscriber_(std::move(subscriber)) {}

KvStoreStreamHandle::~KvStoreStreamHandle() {
  if (!subscriber_) {
    return;
  }
  if (auto registry = registry_.lock()) {
    registry->remove(subscriber_->id);
  }
}

KvStoreStreamHandle
KvStoreStreamRegistry::add(int64_t id, KvStorePublicationFilter filter) {
  auto subscriber =
      std::make_shared<KvStoreStreamSubscriber>(id, std::move(filter));
  const bool added = state_.withWLock([&](auto& state) {
    if (state.closeReason) {
      // The handler stopped: the dispatcher no longer feeds the registry.
      subscriber->queue.close(state.closeReason);
      return false;
    }
    state.subscribers.emplace(id, subscriber);
    facebook::fb303::fbData->setCounter(
        kSubscribersCounter, state.subscribers.size());
    return true;
  });
  if (added) {
    XLOGF(INFO, "KvStore stream-{} started.", id);
  } else {
    XLOGF(INFO, "KvStore stream-{} closed at once: the handler stopped.", id);
  }
  return KvStoreStreamHandle(weak_from_this(), std::move(subscriber));
}

void
KvStoreStreamRegistry::remove(int64_t id) {
  state_.withWLock([&](auto& state) {
    state.subscribers.erase(id);
    facebook::fb303::fbData->setCounter(
        kSubscribersCounter, state.subscribers.size());
  });
  XLOGF(INFO, "KvStore stream-{} ended.", id);
}

void
KvStoreStreamRegistry::fanOut(const thrift::Publication& pub) {
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

  for (const auto& group : groups) {
    auto message = makeFilteredMessage(group.members.front()->filter, pub);
    if (!message) {
      continue;
    }
    message->shared = group.members.size() > 1;
    for (size_t i = 0; i + 1 < group.members.size(); ++i) {
      group.members[i]->queue.push(*message);
    }
    group.members.back()->queue.push(std::move(*message));
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
    numStreams = state.subscribers.size();
  });
  XLOGF(INFO, "[Exit] Terminating {} active KvStore stream(s).", numStreams);
}

size_t
KvStoreStreamRegistry::size() const {
  return state_.rlock()->subscribers.size();
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
