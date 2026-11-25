/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>

#include <openr/common/Constants.h>
#include <openr/common/Util.h>
#include <openr/ctrl-server/KvStoreStreamMergeBuffer.h>

namespace openr {

namespace {

const std::string kArea{"area1"};

thrift::Publication
makeValues(
    const std::vector<std::pair<std::string, int64_t>>& keyVersions,
    const std::string& area = kArea) {
  thrift::Publication pub;
  pub.area() = area;
  for (const auto& [key, version] : keyVersions) {
    pub.keyVals()->emplace(
        key, createThriftValue(version, "node1", std::string("value")));
  }
  return pub;
}

thrift::Publication
makeExpiry(const std::vector<std::string>& keys) {
  thrift::Publication pub;
  pub.area() = kArea;
  pub.expiredKeys() = keys;
  return pub;
}

// TTL refresh of `key` at `version`: no value, a new TTL and TTL version.
thrift::Publication
makeTtlRefresh(const std::string& key, int64_t version, int64_t ttlVersion) {
  thrift::Publication pub;
  pub.area() = kArea;
  pub.keyVals()->emplace(
      key, createThriftValue(version, "node1", std::nullopt, 5000, ttlVersion));
  return pub;
}

// Bytes of a queued message, as KvStoreStreamRegistry counts them.
size_t
messageBytes(const thrift::Publication& pub) {
  apache::thrift::CompactProtocolWriter writer;
  return pub.serializedSizeZC(&writer);
}

constexpr size_t kOverhead{Constants::kKvStoreStreamMergedEntryOverheadBytes};

// Folds `pub` as an unshared message, or as one that other queues share.
bool
add(KvStoreStreamMergeBuffer& buffer,
    thrift::Publication pub,
    bool shared = false) {
  const auto bytes = messageBytes(pub);
  return buffer.add(
      KvStoreStreamMessage{
          std::make_shared<thrift::Publication>(std::move(pub)),
          bytes,
          shared});
}

class KvStoreStreamMergeBufferTest : public ::testing::Test {
 protected:
  std::shared_ptr<std::atomic<size_t>> budget_{
      std::make_shared<std::atomic<size_t>>(0)};
  KvStoreStreamMergeBuffer buffer_{budget_};
};

} // namespace

/*
 * A value never folds onto a queued expiry, so the client receives the
 * expiry before a re-creation at a lower version.
 */
TEST_F(KvStoreStreamMergeBufferTest, ValueAfterExpiryStartsSegment) {
  EXPECT_TRUE(add(buffer_, makeValues({{"k", 5}})));
  EXPECT_FALSE(add(buffer_, makeExpiry({"k"})));
  EXPECT_TRUE(add(buffer_, makeValues({{"k", 1}})));

  ASSERT_EQ(2, buffer_.segments());
  const auto first = buffer_.pop(Constants::kKvStoreStreamMergedPopMaxBytes);
  EXPECT_TRUE(first.keyVals()->empty());
  EXPECT_EQ(std::vector<std::string>{"k"}, *first.expiredKeys());
  const auto second = buffer_.pop(Constants::kKvStoreStreamMergedPopMaxBytes);
  ASSERT_EQ(1, second.keyVals()->size());
  EXPECT_EQ(1, *second.keyVals()->at("k").version());
  EXPECT_TRUE(second.expiredKeys()->empty());
  EXPECT_TRUE(buffer_.empty());
}

// A TTL-only value after an expiry also starts a segment.
TEST_F(KvStoreStreamMergeBufferTest, TtlRefreshAfterExpiryStartsSegment) {
  EXPECT_TRUE(add(buffer_, makeExpiry({"k"})));
  EXPECT_TRUE(add(buffer_, makeTtlRefresh("k", 3, 1)));
  EXPECT_EQ(2, buffer_.segments());
}

// A value and an expiry of other keys fold into the open segment.
TEST_F(KvStoreStreamMergeBufferTest, OtherKeysFoldIntoOpenSegment) {
  EXPECT_TRUE(add(buffer_, makeExpiry({"a"})));
  EXPECT_FALSE(add(buffer_, makeValues({{"b", 1}})));
  EXPECT_FALSE(add(buffer_, makeValues({{"b", 2}})));
  EXPECT_FALSE(add(buffer_, makeExpiry({"c"})));

  ASSERT_EQ(1, buffer_.segments());
  const auto pub = buffer_.pop(Constants::kKvStoreStreamMergedPopMaxBytes);
  ASSERT_EQ(1, pub.keyVals()->size());
  EXPECT_EQ(2, *pub.keyVals()->at("b").version());
  EXPECT_EQ(2, pub.expiredKeys()->size());
}

// A TTL refresh onto a queued value keeps the value and takes the TTL.
TEST_F(KvStoreStreamMergeBufferTest, TtlRefreshKeepsQueuedValue) {
  std::ignore = add(buffer_, makeValues({{"k", 3}}));
  EXPECT_FALSE(add(buffer_, makeTtlRefresh("k", 3, 7)));

  const auto pub = buffer_.pop(Constants::kKvStoreStreamMergedPopMaxBytes);
  const auto& value = pub.keyVals()->at("k");
  EXPECT_EQ("value", value.value().value());
  EXPECT_EQ(3, *value.version());
  EXPECT_EQ(5000, *value.ttl());
  EXPECT_EQ(7, *value.ttlVersion());
}

// A TTL refresh with no queued value stays a TTL refresh.
TEST_F(KvStoreStreamMergeBufferTest, TtlRefreshWithoutQueuedValue) {
  std::ignore = add(buffer_, makeTtlRefresh("k", 3, 1));
  std::ignore = add(buffer_, makeTtlRefresh("k", 3, 2));

  const auto pub = buffer_.pop(Constants::kKvStoreStreamMergedPopMaxBytes);
  const auto& value = pub.keyVals()->at("k");
  EXPECT_FALSE(value.value().has_value());
  EXPECT_EQ(2, *value.ttlVersion());
}

// Each area has its own segments; segments pop in creation order.
TEST_F(KvStoreStreamMergeBufferTest, AreasHaveTheirOwnSegments) {
  EXPECT_TRUE(add(buffer_, makeValues({{"k", 1}}, "area2")));
  EXPECT_TRUE(add(buffer_, makeValues({{"k", 1}}, "area1")));
  EXPECT_FALSE(add(buffer_, makeValues({{"j", 1}}, "area2")));

  ASSERT_EQ(2, buffer_.segments());
  const auto first = buffer_.pop(Constants::kKvStoreStreamMergedPopMaxBytes);
  EXPECT_EQ("area2", *first.area());
  EXPECT_EQ(2, first.keyVals()->size());
  const auto second = buffer_.pop(Constants::kKvStoreStreamMergedPopMaxBytes);
  EXPECT_EQ("area1", *second.area());
  EXPECT_EQ(1, second.keyVals()->size());
}

/*
 * A pop takes entries up to the cap on their budget bytes (overhead
 * included) and leaves the rest first in line. The segment it drains takes
 * no new updates: they start a new segment.
 */
TEST_F(KvStoreStreamMergeBufferTest, PopIsCappedAndClosesSegment) {
  std::ignore = add(buffer_, makeValues({{"k1", 1}, {"k2", 1}, {"k3", 1}}));
  const auto entryBytes = KvStoreStreamMergeBuffer::entryBytes(
      "k1", createThriftValue(1, "node1", std::string("value")));
  const auto entryBudgetBytes = entryBytes + kOverhead;
  ASSERT_EQ(3 * entryBytes, buffer_.bytes());

  const auto first = buffer_.pop(2 * entryBudgetBytes);
  EXPECT_EQ(2, first.keyVals()->size());
  EXPECT_EQ(1, buffer_.segments());
  EXPECT_EQ(entryBytes, buffer_.bytes());

  EXPECT_TRUE(add(buffer_, makeValues({{"k1", 2}})));
  EXPECT_EQ(2, buffer_.segments());
  const auto second = buffer_.pop(2 * entryBudgetBytes);
  ASSERT_EQ(1, second.keyVals()->size());
  EXPECT_EQ(1, *second.keyVals()->begin()->second.version());
  const auto third = buffer_.pop(2 * entryBudgetBytes);
  EXPECT_EQ(2, *third.keyVals()->at("k1").version());
  EXPECT_TRUE(buffer_.empty());
}

// A pop always takes one entry, even above the cap.
TEST_F(KvStoreStreamMergeBufferTest, PopTakesOneEntryAboveCap) {
  std::ignore = add(buffer_, makeValues({{"k1", 1}, {"k2", 1}}));
  EXPECT_EQ(1, buffer_.pop(/*maxBytes=*/1).keyVals()->size());
  EXPECT_EQ(1, buffer_.pop(/*maxBytes=*/1).keyVals()->size());
  EXPECT_TRUE(buffer_.empty());
}

// A Publication without keys starts no segment.
TEST_F(KvStoreStreamMergeBufferTest, PublicationWithoutKeysFoldsNothing) {
  thrift::Publication pub;
  pub.area() = kArea;
  pub.nodeIds() = std::vector<std::string>{"node1"};
  EXPECT_FALSE(add(buffer_, pub));
  EXPECT_TRUE(buffer_.empty());
  EXPECT_EQ(0, buffer_.bytes());
}

/*
 * Entry bytes are serialized: the key plus the serialized Value, or plus 8
 * for an expiry, without the overhead. A value's entry counts less than the
 * message that carries it.
 */
TEST(KvStoreStreamMergeBufferEntryTest, EntryBytesAreSerialized) {
  EXPECT_EQ(3 + 8, KvStoreStreamMergeBuffer::entryBytes("key", std::nullopt));
  const auto value = createThriftValue(1, "node1", std::string(1000, 'x'));
  const auto bytes = KvStoreStreamMergeBuffer::entryBytes("key", value);
  EXPECT_LT(3 + 1000, bytes);
  EXPECT_GT(3 + 1000 + kOverhead, bytes);
  thrift::Publication pub;
  pub.area() = kArea;
  pub.keyVals()->emplace("key", value);
  EXPECT_GT(messageBytes(pub), bytes);
}

/*
 * Folding many distinct small keys keeps the buffer's bytes below the bytes
 * of the folded messages: the overhead, larger than these entries, counts
 * only in the budget.
 */
TEST_F(KvStoreStreamMergeBufferTest, FoldingSmallKeysKeepsSerializedBytes) {
  size_t foldedBytes{0};
  for (int i = 0; i < 1000; ++i) {
    const auto pub = makeValues({{fmt::format("k{:04}", i), 1}});
    foldedBytes += messageBytes(pub);
    std::ignore = add(buffer_, pub);
  }
  EXPECT_EQ(1000, buffer_.entries());
  EXPECT_GT(foldedBytes, buffer_.bytes());
  EXPECT_LT(foldedBytes, buffer_.budgetBytes());
  EXPECT_EQ(buffer_.bytes() + 1000 * kOverhead, budget_->load());
}

/*
 * The budget holds the buffer's serialized bytes plus the overhead of each
 * entry, through replacing a value, popping, moving the buffer and
 * destroying it, and returns to 0.
 */
TEST(KvStoreStreamMergeBufferBudgetTest, BudgetIncludesOverhead) {
  auto budget = std::make_shared<std::atomic<size_t>>(0);
  std::optional<KvStoreStreamMergeBuffer> moved;
  const auto expectBudget = [&budget](const KvStoreStreamMergeBuffer& buffer) {
    EXPECT_EQ(buffer.bytes() + buffer.entries() * kOverhead, budget->load());
    EXPECT_EQ(buffer.budgetBytes(), budget->load());
  };
  {
    KvStoreStreamMergeBuffer buffer(budget);
    std::ignore = add(buffer, makeValues({{"k1", 1}, {"k2", 1}}));
    EXPECT_EQ(2, buffer.entries());
    expectBudget(buffer);

    thrift::Publication bigger;
    bigger.area() = kArea;
    bigger.keyVals()->emplace(
        "k1", createThriftValue(2, "node1", std::string(1000, 'x')));
    std::ignore = add(buffer, bigger);
    EXPECT_EQ(2, buffer.entries());
    expectBudget(buffer);

    std::ignore = buffer.pop(/*maxBytes=*/1);
    EXPECT_EQ(1, buffer.entries());
    expectBudget(buffer);
    ASSERT_LT(0, buffer.bytes());

    const auto bytes = buffer.bytes();
    moved.emplace(std::move(buffer));
    // The move constructor documents that it leaves `buffer` empty.
    EXPECT_EQ(0, buffer.bytes()); // NOLINT(bugprone-use-after-move)
    EXPECT_EQ(0, buffer.budgetBytes()); // NOLINT(bugprone-use-after-move)
    EXPECT_TRUE(buffer.empty()); // NOLINT(bugprone-use-after-move)
    EXPECT_EQ(bytes, moved->bytes());
  }
  expectBudget(*moved);
  std::ignore = moved->pop(Constants::kKvStoreStreamMergedPopMaxBytes);
  EXPECT_TRUE(moved->empty());
  EXPECT_EQ(0, budget->load());
  std::ignore = add(*moved, makeValues({{"k3", 1}}));
  expectBudget(*moved);
  moved.reset();
  EXPECT_EQ(0, budget->load());
}

/*
 * The values of a shared message stay in its Publication: the serialized
 * bytes count the values, the buffer charges the keys and the overhead, and
 * pop() copies the values. The buffer releases the Publication once no
 * entry points into it, also after a move.
 */
TEST_F(KvStoreStreamMergeBufferTest, SharedValuesStayInPublication) {
  const auto pub = makeValues({{"k1", 1}, {"k2", 1}});
  const auto publication = std::make_shared<thrift::Publication>(pub);
  EXPECT_TRUE(buffer_.add(
      KvStoreStreamMessage{publication, messageBytes(pub), /*shared=*/true}));
  const auto entryBytes = KvStoreStreamMergeBuffer::entryBytes(
      "k1", createThriftValue(1, "node1", std::string("value")));

  EXPECT_EQ(2 * entryBytes, buffer_.bytes());
  EXPECT_EQ(2 * (2 + kOverhead), buffer_.budgetBytes());
  EXPECT_EQ(buffer_.budgetBytes(), budget_->load());
  EXPECT_EQ(1, buffer_.pins());

  KvStoreStreamMergeBuffer moved(std::move(buffer_));
  EXPECT_EQ(1, moved.pins());
  const auto first = moved.pop(/*maxBytes=*/1);
  EXPECT_EQ(1, moved.pins());
  EXPECT_TRUE(moved.takeReleased().empty());
  const auto second = moved.pop(/*maxBytes=*/1);
  EXPECT_EQ(0, moved.pins());
  auto released = moved.takeReleased();
  ASSERT_EQ(1, released.size());
  EXPECT_EQ(publication, released.front());
  released.clear();
  EXPECT_EQ(1, publication.use_count());
  EXPECT_EQ(0, budget_->load());

  EXPECT_EQ(pub, *publication);
  auto popped = *first.keyVals();
  popped.insert(second.keyVals()->begin(), second.keyVals()->end());
  EXPECT_EQ(*pub.keyVals(), popped);
}

/*
 * A TTL refresh onto a shared value keeps the value in its Publication.
 * pop() applies the new TTL to its copy, and the entry counts the bytes of
 * that copy.
 */
TEST_F(KvStoreStreamMergeBufferTest, TtlRefreshOnSharedValue) {
  std::ignore = add(buffer_, makeValues({{"k", 3}}), /*shared=*/true);
  EXPECT_FALSE(add(buffer_, makeTtlRefresh("k", 3, 7), /*shared=*/true));
  auto expected = createThriftValue(3, "node1", std::string("value"));
  expected.ttl() = 5000;
  expected.ttlVersion() = 7;

  EXPECT_EQ(1, buffer_.pins());
  EXPECT_EQ(
      KvStoreStreamMergeBuffer::entryBytes("k", expected), buffer_.bytes());
  EXPECT_EQ(1 + kOverhead, budget_->load());
  const auto pub = buffer_.pop(Constants::kKvStoreStreamMergedPopMaxBytes);
  EXPECT_EQ(expected, pub.keyVals()->at("k"));
}

// A value of an unshared message replaces a shared value and its pin.
TEST_F(KvStoreStreamMergeBufferTest, OwnedValueReplacesSharedValue) {
  std::ignore = add(buffer_, makeValues({{"k", 1}}), /*shared=*/true);
  std::ignore = add(buffer_, makeValues({{"k", 2}}));

  EXPECT_EQ(0, buffer_.pins());
  EXPECT_EQ(1, buffer_.takeReleased().size());
  EXPECT_EQ(buffer_.bytes() + kOverhead, budget_->load());
  const auto pub = buffer_.pop(Constants::kKvStoreStreamMergedPopMaxBytes);
  EXPECT_EQ(2, *pub.keyVals()->at("k").version());
}

/*
 * A pinned Publication counts as evictable once nothing else references
 * it, as when the other queues popped it.
 */
TEST_F(KvStoreStreamMergeBufferTest, EvictableBytesCountExclusivePins) {
  const auto pub = makeValues({{"k1", 1}});
  auto other = std::make_shared<thrift::Publication>(pub);
  const auto bytes = messageBytes(pub);
  std::ignore =
      buffer_.add(KvStoreStreamMessage{other, bytes, /*shared=*/true});

  EXPECT_EQ(buffer_.budgetBytes(), buffer_.evictableBytes());
  other.reset();
  EXPECT_EQ(buffer_.budgetBytes() + bytes, buffer_.evictableBytes());
}

} // namespace openr

int
main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  const folly::Init init(&argc, &argv);
  return RUN_ALL_TESTS();
}
