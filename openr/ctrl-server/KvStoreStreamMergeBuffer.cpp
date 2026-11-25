/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <openr/ctrl-server/KvStoreStreamMergeBuffer.h>

#include <algorithm>
#include <utility>

#include <folly/logging/xlog.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>

#include <openr/common/Constants.h>
#include <openr/if/gen-cpp2/KvStore_types_custom_protocol.h>

namespace openr {

namespace {

// Serialized bytes of an expired key, on top of the key.
constexpr size_t kExpiredKeyBytes{8};

size_t
serializedSize(const thrift::Value& value) {
  apache::thrift::CompactProtocolWriter writer;
  return value.serializedSizeZC(&writer);
}

// Bytes of an i64 field value, as serializedSize() counts them.
size_t
i64Bytes(int64_t value) {
  apache::thrift::CompactProtocolWriter writer;
  return writer.serializedSizeI64(value);
}

bool
isTtlRefresh(const thrift::Value& value) {
  return !value.value().has_value();
}

} // namespace

bool
KvStoreStreamMergeBuffer::Entry::hasData() const {
  return owned ? owned->value().has_value()
               : shared && shared->value().has_value();
}

KvStoreStreamMergeBuffer::KvStoreStreamMergeBuffer(
    std::shared_ptr<std::atomic<size_t>> budget)
    : budget_(std::move(budget)) {
  static_assert(
      Constants::kKvStoreStreamMergedEntryOverheadBytes >=
          sizeof(decltype(Segment::entries)::value_type),
      "the entry overhead must cover the map slot");
}

KvStoreStreamMergeBuffer::KvStoreStreamMergeBuffer(
    KvStoreStreamMergeBuffer&& other) noexcept
    : budget_(other.budget_),
      segments_(std::move(other.segments_)),
      openSegments_(std::move(other.openSegments_)),
      pins_(std::move(other.pins_)),
      released_(std::move(other.released_)),
      nextSeq_(other.nextSeq_),
      bytes_(std::exchange(other.bytes_, 0)),
      chargedBytes_(std::exchange(other.chargedBytes_, 0)),
      entries_(std::exchange(other.entries_, 0)) {
  // Moved-from containers are valid but unspecified.
  other.segments_.clear();
  other.openSegments_.clear();
  other.pins_.clear();
  other.released_.clear();
}

KvStoreStreamMergeBuffer::~KvStoreStreamMergeBuffer() {
  budget_->fetch_sub(budgetBytes());
}

size_t
KvStoreStreamMergeBuffer::budgetBytes() const {
  return chargedBytes_ +
      entries_ * Constants::kKvStoreStreamMergedEntryOverheadBytes;
}

size_t
KvStoreStreamMergeBuffer::evictableBytes() const {
  size_t bytes{budgetBytes()};
  for (const auto& [_, pin] : pins_) {
    /*
     * use_count() only ranks queues here; nothing reads or moves the
     * Publication based on it.
     */
    if (pin.publication.use_count() == 1) {
      bytes += pin.bytes;
    }
  }
  return bytes;
}

std::vector<std::shared_ptr<thrift::Publication>>
KvStoreStreamMergeBuffer::takeReleased() {
  return std::exchange(released_, {});
}

size_t
KvStoreStreamMergeBuffer::entryBytes(
    const std::string& key, const std::optional<thrift::Value>& value) {
  return key.size() + (value ? serializedSize(*value) : kExpiredKeyBytes);
}

size_t
KvStoreStreamMergeBuffer::entryBytes(
    const std::string& key, const Entry& entry) {
  if (!entry.shared) {
    return entryBytes(key, entry.owned);
  }
  auto bytes = key.size() + serializedSize(*entry.shared);
  if (entry.ttl) {
    // Only the two TTL fields differ from the shared value.
    bytes += i64Bytes(entry.ttl->first) + i64Bytes(entry.ttl->second);
    bytes -=
        i64Bytes(*entry.shared->ttl()) + i64Bytes(*entry.shared->ttlVersion());
  }
  return bytes;
}

size_t
KvStoreStreamMergeBuffer::chargedBytes(
    const std::string& key, const Entry& entry) {
  return entry.shared ? key.size() : entry.bytes;
}

KvStoreStreamMergeBuffer::Segment* FOLLY_NULLABLE
KvStoreStreamMergeBuffer::openSegment(const std::string& area) {
  auto it = openSegments_.find(area);
  if (it == openSegments_.end()) {
    return nullptr;
  }
  return &segments_.at(it->second - segments_.front().seq);
}

bool
KvStoreStreamMergeBuffer::add(const KvStoreStreamMessage& message) {
  auto& pub = *message.publication;
  if (pub.keyVals()->empty() && pub.expiredKeys()->empty()) {
    return false;
  }
  const auto& area = *pub.area();
  auto* segment = openSegment(area);
  // A value folded onto an expiry would hide the expiry from the client.
  const bool meetsExpiry = segment &&
      std::any_of(pub.keyVals()->begin(),
                  pub.keyVals()->end(),
                  [segment](const auto& keyValue) {
                    auto it = segment->entries.find(keyValue.first);
                    return it != segment->entries.end() &&
                        it->second.isExpiry();
                  });
  const bool started = !segment || meetsExpiry;
  if (started) {
    segment = &segments_.emplace_back();
    segment->seq = nextSeq_++;
    segment->area = area;
    openSegments_[area] = segment->seq;
  }
  XCHECK(segment != nullptr);
  auto& target = *segment;
  const auto* shared = message.shared ? &message : nullptr;
  for (auto& [key, value] : *pub.keyVals()) {
    fold(target, key, &value, shared);
  }
  for (const auto& key : *pub.expiredKeys()) {
    fold(target, key, nullptr, nullptr);
  }
  return started;
}

void
KvStoreStreamMergeBuffer::fold(
    Segment& segment,
    const std::string& key,
    thrift::Value* FOLLY_NULLABLE value,
    const KvStoreStreamMessage* FOLLY_NULLABLE message) {
  auto [it, inserted] = segment.entries.try_emplace(key);
  auto& entry = it->second;
  const auto oldBytes = static_cast<int64_t>(entry.bytes);
  const auto oldCharged =
      inserted ? 0 : static_cast<int64_t>(chargedBytes(key, entry));
  if (value && isTtlRefresh(*value) && entry.hasData()) {
    // A TTL refresh keeps the merged value and takes the new TTL.
    if (entry.owned) {
      entry.owned->ttl() = *value->ttl();
      entry.owned->ttlVersion() = *value->ttlVersion();
    } else {
      entry.ttl.emplace(*value->ttl(), *value->ttlVersion());
    }
  } else {
    clearValue(entry);
    if (value && message) {
      entry.shared = value;
      entry.pin = pin(*message);
    } else if (value) {
      // The message is this queue's alone: nothing else reads it.
      entry.owned = std::move(*value);
    }
  }
  entry.bytes = entryBytes(key, entry);
  charge(
      static_cast<int64_t>(entry.bytes) - oldBytes,
      static_cast<int64_t>(chargedBytes(key, entry)) - oldCharged,
      inserted ? 1 : 0);
}

void
KvStoreStreamMergeBuffer::clearValue(Entry& entry) {
  if (entry.pin) {
    unpin(entry.pin);
  }
  entry.owned.reset();
  entry.shared = nullptr;
  entry.pin = nullptr;
  entry.ttl.reset();
}

const thrift::Publication*
KvStoreStreamMergeBuffer::pin(const KvStoreStreamMessage& message) {
  auto& pin = pins_[message.publication.get()];
  if (!pin.publication) {
    pin.publication = message.publication;
    pin.bytes = message.bytes;
  }
  ++pin.entries;
  return message.publication.get();
}

void
KvStoreStreamMergeBuffer::unpin(const thrift::Publication* publication) {
  auto it = pins_.find(publication);
  XCHECK(it != pins_.end());
  if (--it->second.entries == 0) {
    released_.push_back(std::move(it->second.publication));
    pins_.erase(it);
  }
}

thrift::Publication
KvStoreStreamMergeBuffer::pop(size_t maxBytes) {
  auto& segment = segments_.front();
  // A draining segment takes no new updates, so it ends.
  auto open = openSegments_.find(segment.area);
  if (open != openSegments_.end() && open->second == segment.seq) {
    openSegments_.erase(open);
  }
  thrift::Publication pub;
  pub.area() = segment.area;
  size_t poppedCapBytes{0};
  int64_t poppedBytes{0};
  int64_t poppedCharged{0};
  int64_t poppedEntries{0};
  while (!segment.entries.empty()) {
    auto it = segment.entries.begin();
    const auto entryCapBytes =
        it->second.bytes + Constants::kKvStoreStreamMergedEntryOverheadBytes;
    if (poppedEntries > 0 && poppedCapBytes + entryCapBytes > maxBytes) {
      break;
    }
    poppedCapBytes += entryCapBytes;
    poppedBytes += static_cast<int64_t>(it->second.bytes);
    poppedCharged += static_cast<int64_t>(chargedBytes(it->first, it->second));
    ++poppedEntries;
    segment.entries.eraseInto(
        it, [this, &pub](std::string&& key, Entry&& entry) {
          if (entry.owned) {
            pub.keyVals()->emplace(std::move(key), std::move(*entry.owned));
          } else if (entry.shared) {
            thrift::Value value = *entry.shared;
            if (entry.ttl) {
              value.ttl() = entry.ttl->first;
              value.ttlVersion() = entry.ttl->second;
            }
            pub.keyVals()->emplace(std::move(key), std::move(value));
            unpin(entry.pin);
          } else {
            pub.expiredKeys()->push_back(std::move(key));
          }
        });
  }
  if (segment.entries.empty()) {
    segments_.pop_front();
  }
  charge(-poppedBytes, -poppedCharged, -poppedEntries);
  return pub;
}

void
KvStoreStreamMergeBuffer::charge(
    int64_t bytesDelta, int64_t chargedDelta, int64_t entriesDelta) {
  bytes_ += bytesDelta;
  chargedBytes_ += chargedDelta;
  entries_ += entriesDelta;
  const auto budgetDelta = chargedDelta +
      entriesDelta *
          static_cast<int64_t>(
              Constants::kKvStoreStreamMergedEntryOverheadBytes);
  if (budgetDelta > 0) {
    budget_->fetch_add(budgetDelta);
  } else {
    budget_->fetch_sub(-budgetDelta);
  }
}

} // namespace openr
