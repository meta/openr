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

} // namespace

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
      nextSeq_(other.nextSeq_),
      bytes_(std::exchange(other.bytes_, 0)),
      entries_(std::exchange(other.entries_, 0)) {
  // Moved-from containers are valid but unspecified.
  other.segments_.clear();
  other.openSegments_.clear();
}

KvStoreStreamMergeBuffer::~KvStoreStreamMergeBuffer() {
  budget_->fetch_sub(budgetBytes());
}

size_t
KvStoreStreamMergeBuffer::budgetBytes() const {
  return bytes_ + entries_ * Constants::kKvStoreStreamMergedEntryOverheadBytes;
}

size_t
KvStoreStreamMergeBuffer::entryBytes(
    const std::string& key, const std::optional<thrift::Value>& value) {
  return key.size() + (value ? serializedSize(*value) : kExpiredKeyBytes);
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
KvStoreStreamMergeBuffer::add(const thrift::Publication& pub) {
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
                    return it != segment->entries.end() && !it->second.value;
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
  const auto entriesBefore = target.entries.size();
  int64_t bytesDelta{0};
  for (const auto& [key, value] : *pub.keyVals()) {
    bytesDelta += fold(target, key, value);
  }
  for (const auto& key : *pub.expiredKeys()) {
    bytesDelta += fold(target, key, std::nullopt);
  }
  charge(
      bytesDelta, static_cast<int64_t>(target.entries.size() - entriesBefore));
  return started;
}

int64_t
KvStoreStreamMergeBuffer::fold(
    Segment& segment,
    const std::string& key,
    const std::optional<thrift::Value>& value) {
  auto& entry = segment.entries[key];
  const auto oldBytes = static_cast<int64_t>(entry.bytes);
  if (value && !value->value().has_value() && entry.value &&
      entry.value->value().has_value()) {
    // A TTL refresh keeps the merged value and takes the new TTL.
    entry.value->ttl() = *value->ttl();
    entry.value->ttlVersion() = *value->ttlVersion();
  } else {
    entry.value = value;
  }
  entry.bytes = entryBytes(key, entry.value);
  return static_cast<int64_t>(entry.bytes) - oldBytes;
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
  // The cap applies to budget bytes: they leave the budget at this pop.
  size_t poppedBudgetBytes{0};
  size_t poppedBytes{0};
  size_t poppedEntries{0};
  while (!segment.entries.empty()) {
    auto it = segment.entries.begin();
    const auto entryBudgetBytes =
        it->second.bytes + Constants::kKvStoreStreamMergedEntryOverheadBytes;
    if (poppedEntries > 0 && poppedBudgetBytes + entryBudgetBytes > maxBytes) {
      break;
    }
    poppedBudgetBytes += entryBudgetBytes;
    poppedBytes += it->second.bytes;
    ++poppedEntries;
    segment.entries.eraseInto(it, [&pub](std::string&& key, Entry&& entry) {
      if (entry.value) {
        pub.keyVals()->emplace(std::move(key), std::move(*entry.value));
      } else {
        pub.expiredKeys()->push_back(std::move(key));
      }
    });
  }
  if (segment.entries.empty()) {
    segments_.pop_front();
  }
  charge(
      -static_cast<int64_t>(poppedBytes), -static_cast<int64_t>(poppedEntries));
  return pub;
}

void
KvStoreStreamMergeBuffer::charge(int64_t bytesDelta, int64_t entriesDelta) {
  bytes_ += bytesDelta;
  entries_ += entriesDelta;
  const auto budgetDelta = bytesDelta +
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
