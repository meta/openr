/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <string>

#include <folly/CppAttributes.h>
#include <folly/container/F14Map.h>

#include <openr/if/gen-cpp2/KvStore_types.h>

namespace openr {

/*
 * The merged updates of one KvStore stream queue in merge mode. Folds
 * Publications into segments; a segment belongs to one area and holds the
 * latest value of each key, or the expiry of an expired key. Segments pop
 * oldest first. Each area folds into its newest segment, its open segment,
 * except that:
 * - a value (TTL-only or full) never folds onto a queued expiry: the
 *   Publication starts a new segment, so the client receives the expiry
 *   before the re-creation, whatever its version;
 * - pop() closes the segment it takes entries from to new updates, so a
 *   segment only shrinks once it starts to drain.
 * Example: k at version 5, the expiry of k, then k at version 1 give the
 * segments {expired k} and {k: version 1}. A client that applies them like
 * mergeKeyValues() ends with version 1.
 *
 * A TTL refresh onto a queued value keeps the value and takes the new TTL.
 *
 * Two byte counts: bytes() is the serialized size of the entries, which the
 * per-stream limit and the merge thresholds compare; budgetBytes() adds
 * Constants::kKvStoreStreamMergedEntryOverheadBytes per entry as an estimate
 * of the memory the entries take. The buffer charges budgetBytes() to
 * `budget` and releases it as entries leave or the buffer is destroyed. Not
 * thread-safe: the queue lock guards it.
 */
class KvStoreStreamMergeBuffer {
 public:
  explicit KvStoreStreamMergeBuffer(
      std::shared_ptr<std::atomic<size_t>> budget);
  // Takes the entries and their charge; `other` is left empty.
  KvStoreStreamMergeBuffer(KvStoreStreamMergeBuffer&& other) noexcept;
  KvStoreStreamMergeBuffer& operator=(KvStoreStreamMergeBuffer&&) = delete;
  KvStoreStreamMergeBuffer(const KvStoreStreamMergeBuffer&) = delete;
  KvStoreStreamMergeBuffer& operator=(const KvStoreStreamMergeBuffer&) = delete;
  ~KvStoreStreamMergeBuffer();

  /*
   * Folds the keyVals and expiredKeys of `pub`; drops its other fields.
   * Returns true when it started a segment: the area had no open segment, or
   * a value of `pub` meets a queued expiry. A Publication without keys folds
   * nothing.
   */
  [[nodiscard]] bool add(const thrift::Publication& pub);

  /*
   * Moves entries of the oldest segment out as a Publication of its area,
   * until the next entry would take their budget bytes (overhead included)
   * above `maxBytes`; always at least one. Entries left in the segment stay
   * first in line. Requires !empty(). Example: with maxBytes = 1 MiB and
   * three entries of 600 KB, it returns one entry; the next two pops return
   * one each.
   */
  thrift::Publication pop(size_t maxBytes);

  bool
  empty() const {
    return segments_.empty();
  }
  size_t
  segments() const {
    return segments_.size();
  }
  size_t
  entries() const {
    return entries_;
  }
  /*
   * Serialized bytes of every entry (see entryBytes()), without the
   * overhead. Example: 240 for one entry with a 40-byte key and a 200-byte
   * serialized Value.
   */
  size_t
  bytes() const {
    return bytes_;
  }
  /*
   * Bytes charged to the total budget: bytes() plus
   * Constants::kKvStoreStreamMergedEntryOverheadBytes per entry. Example:
   * 240 + 256 = 496 for the entry above.
   */
  size_t budgetBytes() const;

  /*
   * Serialized bytes of one entry: its key plus its Compact serialized Value,
   * or plus 8 for an expiry. Never more than the entry's key and Value take
   * in a serialized Publication.
   */
  static size_t entryBytes(
      const std::string& key, const std::optional<thrift::Value>& value);

 private:
  struct Entry {
    // std::nullopt for an expired key.
    std::optional<thrift::Value> value;
    // entryBytes(): serialized, without the overhead.
    size_t bytes{0};
  };
  struct Segment {
    // Consecutive: segments_[i].seq == segments_.front().seq + i.
    uint64_t seq{0};
    std::string area;
    folly::F14FastMap<std::string, Entry> entries;
  };

  // The open segment of `area`, or nullptr.
  Segment* FOLLY_NULLABLE openSegment(const std::string& area);
  /*
   * Folds `value` (std::nullopt: expiry) into the entry of `key`; returns the
   * change of its serialized bytes.
   */
  int64_t fold(
      Segment& segment,
      const std::string& key,
      const std::optional<thrift::Value>& value);
  /*
   * Applies the change of the serialized bytes and of the entry count, and
   * charges or releases the matching budget bytes.
   */
  void charge(int64_t bytesDelta, int64_t entriesDelta);

  const std::shared_ptr<std::atomic<size_t>> budget_;
  std::deque<Segment> segments_;
  // area -> seq of its open segment.
  folly::F14FastMap<std::string, uint64_t> openSegments_;
  uint64_t nextSeq_{0};
  size_t bytes_{0};
  size_t entries_{0};
};

} // namespace openr
