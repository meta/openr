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
#include <utility>
#include <vector>

#include <folly/CppAttributes.h>
#include <folly/container/F14Map.h>

#include <openr/if/gen-cpp2/KvStore_types.h>

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
 * Values of a shared message stay in its Publication: the entry points to
 * the value, and the buffer holds a reference to the Publication (a pin)
 * while any entry points into it. The queues of subscribers with the same
 * key filter thus keep one copy of each value in merge mode as well. A TTL
 * refresh onto such a value is kept beside it and applied at pop(). Values
 * of an unshared message are moved into the buffer.
 *
 * Two byte counts: bytes() is the serialized size of the entries, which the
 * per-stream limit and the merge thresholds compare; budgetBytes() is the
 * memory the buffer charges to `budget`: the serialized entries it owns,
 * the keys of entries that point into a pinned Publication (the Publication
 * charges its values), and Constants::kKvStoreStreamMergedEntryOverheadBytes
 * per entry. The buffer releases its charge as entries leave or the buffer
 * is destroyed. Not thread-safe: the queue lock guards it.
 */
class KvStoreStreamMergeBuffer {
 public:
  explicit KvStoreStreamMergeBuffer(
      std::shared_ptr<std::atomic<size_t>> budget);
  // Takes the entries, the pins and the charge; `other` is left empty.
  KvStoreStreamMergeBuffer(KvStoreStreamMergeBuffer&& other) noexcept;
  KvStoreStreamMergeBuffer& operator=(KvStoreStreamMergeBuffer&&) = delete;
  KvStoreStreamMergeBuffer(const KvStoreStreamMergeBuffer&) = delete;
  KvStoreStreamMergeBuffer& operator=(const KvStoreStreamMergeBuffer&) = delete;
  ~KvStoreStreamMergeBuffer();

  /*
   * Folds the keyVals and expiredKeys of `message`; drops its other fields.
   * Points to the values of a shared message and moves the values out of an
   * unshared one. Returns true when it started a segment: the area had no
   * open segment, or a value of the message meets a queued expiry. A
   * Publication without keys folds nothing.
   */
  [[nodiscard]] bool add(const KvStoreStreamMessage& message);

  /*
   * Moves entries of the oldest segment out as a Publication of its area,
   * until the next entry would take their serialized bytes plus overhead
   * above `maxBytes`; always at least one. Copies a value that points into a
   * pinned Publication. Entries left in the segment stay first in line.
   * Requires !empty(). Example: with maxBytes = 1 MiB and three entries of
   * 600 KB, it returns one entry; the next two pops return one each.
   */
  thrift::Publication pop(size_t maxBytes);

  /*
   * The pins that no entry uses any more, so the caller frees the
   * Publications after it releases the queue lock.
   */
  std::vector<std::shared_ptr<thrift::Publication>> takeReleased();

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
   * Bytes charged to the total budget. Example: 240 + 256 = 496 for the
   * entry above when the buffer owns its value; 40 + 256 = 296 when the
   * value stays in a pinned Publication.
   */
  size_t budgetBytes() const;
  /*
   * Bytes that destroying the buffer would free: budgetBytes() plus the
   * pinned Publications that nothing else references. Ranks queues for
   * eviction only.
   */
  size_t evictableBytes() const;
  // Publications that entries point into.
  size_t
  pins() const {
    return pins_.size();
  }

  /*
   * Serialized bytes of one entry: its key plus its Compact serialized Value,
   * or plus 8 for an expiry. Never more than the entry's key and Value take
   * in a serialized Publication.
   */
  static size_t entryBytes(
      const std::string& key, const std::optional<thrift::Value>& value);

 private:
  struct Entry {
    // A value the buffer owns.
    std::optional<thrift::Value> owned;
    // A value in a pinned Publication (pins_ key `pin`).
    const thrift::Value* FOLLY_NULLABLE shared{nullptr};
    const thrift::Publication* FOLLY_NULLABLE pin{nullptr};
    // TTL and TTL version of a TTL refresh folded onto `shared`.
    std::optional<std::pair<int64_t, int64_t>> ttl;
    // entryBytes() of the value a client receives, without the overhead.
    size_t bytes{0};

    bool
    isExpiry() const {
      return !owned && !shared;
    }
    bool hasData() const;
  };
  struct Segment {
    // Consecutive: segments_[i].seq == segments_.front().seq + i.
    uint64_t seq{0};
    std::string area;
    folly::F14FastMap<std::string, Entry> entries;
  };
  struct Pin {
    std::shared_ptr<thrift::Publication> publication;
    // KvStoreStreamMessage::bytes of the Publication.
    size_t bytes{0};
    // Entries that point into the Publication.
    size_t entries{0};
  };

  // The open segment of `area`, or nullptr.
  Segment* FOLLY_NULLABLE openSegment(const std::string& area);
  /*
   * Folds the value of `key` (nullptr: expiry) into its entry. `message` is
   * the shared message that holds `value`, or nullptr when `owned` carries
   * the value of an unshared message.
   */
  void fold(
      Segment& segment,
      const std::string& key,
      thrift::Value* FOLLY_NULLABLE value,
      const KvStoreStreamMessage* FOLLY_NULLABLE message);
  // Clears the value of `entry` and drops its pin.
  void clearValue(Entry& entry);
  const thrift::Publication* pin(const KvStoreStreamMessage& message);
  void unpin(const thrift::Publication* publication);
  // Serialized bytes of the value a client receives for `entry`.
  static size_t entryBytes(const std::string& key, const Entry& entry);
  // Bytes `entry` charges to the budget, without the overhead.
  static size_t chargedBytes(const std::string& key, const Entry& entry);
  /*
   * Applies the change of the serialized bytes, the charged bytes and the
   * entry count, and charges or releases the matching budget bytes.
   */
  void charge(int64_t bytesDelta, int64_t chargedDelta, int64_t entriesDelta);

  const std::shared_ptr<std::atomic<size_t>> budget_;
  std::deque<Segment> segments_;
  // area -> seq of its open segment.
  folly::F14FastMap<std::string, uint64_t> openSegments_;
  folly::F14FastMap<const thrift::Publication*, Pin> pins_;
  std::vector<std::shared_ptr<thrift::Publication>> released_;
  uint64_t nextSeq_{0};
  size_t bytes_{0};
  size_t chargedBytes_{0};
  size_t entries_{0};
};

} // namespace openr
