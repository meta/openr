/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <optional>
#include <set>
#include <string>
#include <vector>

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include <openr/common/Util.h>
#include <openr/kvstore/KvStorePublicationFilter.h>
#include <openr/kvstore/KvStorePublisher.h>

using namespace openr;

namespace {

const std::string kArea{"default"};

// A TTL update: no value, only the TTL and its version change.
thrift::Value
createTtlUpdate() {
  return createThriftValue(
      1 /* version */,
      "node1",
      std::nullopt /* data */,
      300000 /* ttl */,
      2 /* ttlVersion */);
}

thrift::Value
createValue(const std::string& data) {
  return createThriftValue(1 /* version */, "node1", data);
}

thrift::PerfEvents
createPerfEvents() {
  thrift::PerfEvent event;
  event.nodeName() = "node1";
  event.eventDescr() = "KVSTORE_UPDATE";
  event.unixTs() = 1000;
  thrift::PerfEvents perfEvents;
  perfEvents.events()->push_back(std::move(event));
  return perfEvents;
}

thrift::KeyDumpParams
createUnfilteredParams() {
  thrift::KeyDumpParams params;
  params.ignoreTtl() = false;
  return params;
}

thrift::KeyDumpParams
createKeyParams(std::vector<std::string> keys) {
  thrift::KeyDumpParams params;
  params.keys() = std::move(keys);
  return params;
}

} // namespace

TEST(KvStorePublisher, OnlyExpiredKeysFilterTest) {
  // dummy settings
  std::set<std::string> selectAreas;

  thrift::KeyDumpParams filter;
  std::vector<std::string> keys = {Constants::kAdjDbMarker.toString()};
  // We only keep the keys with prefix "adj:"
  filter.keys() = keys;

  auto streamAndPublisher =
      apache::thrift::ServerStream<thrift::Publication>::createPublisher();

  auto kvStorePublisher = std::make_unique<KvStorePublisher>(
      selectAreas,
      std::move(filter),
      std::move(streamAndPublisher.second),
      std::chrono::steady_clock::now(),
      0);

  thrift::Publication publication;
  publication.expiredKeys()->emplace_back("adj:test1");
  publication.expiredKeys()->emplace_back("prefix:test2");

  publication.area() = "default";

  kvStorePublisher->publish(publication);

  kvStorePublisher->complete();

  // check the received keys
  const RegexSet prefixMatcher(keys);
  std::move(streamAndPublisher.first)
      .toClientStreamUnsafeDoNotUse()
      .subscribeInline(
          [&prefixMatcher](
              folly::Try<thrift::Publication>&& receivedPublication) {
            if (receivedPublication.hasValue()) {
              // we wil receive one key: adj:test1
              EXPECT_EQ(1, receivedPublication->expiredKeys()->size());
              for (auto& key : *receivedPublication->expiredKeys()) {
                EXPECT_TRUE(prefixMatcher.match(key));
              }
            }
          });
}

TEST(KvStorePublicationFilter, KeepsAllKeys) {
  struct TestCase {
    std::string name;
    thrift::KeyDumpParams params;
    bool expected;
  };
  std::vector<TestCase> testCases;
  // ignoreTtl defaults to true, so default params drop TTL updates.
  testCases.push_back({"default params", thrift::KeyDumpParams{}, false});
  testCases.push_back({"ignoreTtl=false", createUnfilteredParams(), true});
  {
    auto params = createUnfilteredParams();
    params.keys() = std::vector<std::string>{};
    testCases.push_back({"keys set to empty", std::move(params), true});
  }
  {
    auto params = createUnfilteredParams();
    params.keys() = std::vector<std::string>{"adj:"};
    testCases.push_back({"keys set", std::move(params), false});
  }
  {
    auto params = createUnfilteredParams();
    params.originatorIds() = std::set<std::string>{"node1"};
    testCases.push_back({"originatorIds set", std::move(params), false});
  }
  {
    auto params = createUnfilteredParams();
    params.doNotPublishValue() = true;
    testCases.push_back({"doNotPublishValue", std::move(params), false});
  }

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.name);
    const KvStorePublicationFilter filter(
        {} /* selectAreas */, testCase.params);
    EXPECT_EQ(testCase.expected, filter.keepsAllKeys());
  }
}

TEST(KvStorePublicationFilter, MatchesArea) {
  const KvStorePublicationFilter allAreas(
      {} /* selectAreas */, thrift::KeyDumpParams{});
  EXPECT_TRUE(allAreas.matchesArea("spine"));

  const KvStorePublicationFilter spine({"spine"}, thrift::KeyDumpParams{});
  EXPECT_TRUE(spine.matchesArea("spine"));
  EXPECT_FALSE(spine.matchesArea("plane"));
}

TEST(KvStorePublicationFilter, SameKeyFilter) {
  struct TestCase {
    std::string name;
    std::set<std::string> otherAreas;
    thrift::KeyDumpParams other;
    bool expected;
  };
  // Every case compares against keys {"adj:"} in area "spine".
  const auto base = createKeyParams({"adj:"});
  std::vector<TestCase> testCases;
  testCases.push_back({"same params", {"spine"}, base, true});
  testCases.push_back({"other area", {"pod"}, base, true});
  testCases.push_back(
      {"other keys", {"spine"}, createKeyParams({"prefix:"}), false});
  {
    auto params = base;
    params.originatorIds() = std::set<std::string>{"node1"};
    testCases.push_back({"originatorIds", {"spine"}, std::move(params), false});
  }
  {
    auto params = base;
    params.oper() = thrift::FilterOperator::AND;
    testCases.push_back({"oper AND", {"spine"}, std::move(params), false});
  }
  {
    // KvStoreFilters treats an unset oper as OR.
    auto params = base;
    params.oper() = thrift::FilterOperator::OR;
    testCases.push_back({"oper OR", {"spine"}, std::move(params), true});
  }
  {
    auto params = base;
    params.ignoreTtl() = false;
    testCases.push_back({"ignoreTtl", {"spine"}, std::move(params), false});
  }
  {
    auto params = base;
    params.doNotPublishValue() = true;
    testCases.push_back(
        {"doNotPublishValue", {"spine"}, std::move(params), false});
  }
  {
    // Fields that apply() does not read.
    auto params = base;
    params.senderId() = "node2";
    params.keyValHashes() = thrift::KeyVals{};
    testCases.push_back(
        {"senderId and keyValHashes", {"spine"}, std::move(params), true});
  }
  testCases.push_back(
      {"keeps all keys", {"spine"}, createUnfilteredParams(), false});

  const KvStorePublicationFilter filter({"spine"}, base);
  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.name);
    const KvStorePublicationFilter other(testCase.otherAreas, testCase.other);
    EXPECT_EQ(testCase.expected, filter.sameKeyFilter(other));
    EXPECT_EQ(testCase.expected, other.sameKeyFilter(filter));
  }
}

/*
 * Filters that keep all keys are the same key filter whatever their oper;
 * apply() is never used for them.
 */
TEST(KvStorePublicationFilter, SameKeyFilterKeepsAllKeys) {
  auto andParams = createUnfilteredParams();
  andParams.oper() = thrift::FilterOperator::AND;
  const KvStorePublicationFilter all1({"spine"}, createUnfilteredParams());
  const KvStorePublicationFilter all2({} /* selectAreas */, andParams);
  const KvStorePublicationFilter defaultParams(
      {"spine"}, thrift::KeyDumpParams{});
  ASSERT_TRUE(all1.keepsAllKeys());
  ASSERT_TRUE(all2.keepsAllKeys());
  ASSERT_FALSE(defaultParams.keepsAllKeys());

  EXPECT_TRUE(all1.sameKeyFilter(all2));
  EXPECT_FALSE(all1.sameKeyFilter(defaultParams));
  EXPECT_FALSE(defaultParams.sameKeyFilter(all1));
  EXPECT_TRUE(defaultParams.sameKeyFilter(
      KvStorePublicationFilter({"pod"}, thrift::KeyDumpParams{})));
}

TEST(KvStorePublicationFilter, Apply) {
  struct TestCase {
    std::string name;
    thrift::KeyDumpParams params;
    thrift::Publication input;
    std::optional<thrift::Publication> expected;
  };
  std::vector<TestCase> testCases;

  testCases.push_back(
      {"nothing matches",
       createKeyParams({"adj:"}),
       createThriftPublication(
           {{"prefix:node1", createValue("v")}},
           {"prefix:node2"} /* expiredKeys */,
           std::nullopt /* nodeIds */,
           std::nullopt /* keysToUpdate */,
           kArea),
       std::nullopt});

  testCases.push_back(
      {"only expired keys",
       createKeyParams({"adj:"}),
       createThriftPublication(
           {} /* keyVals */,
           {"adj:node1", "prefix:node2"} /* expiredKeys */,
           std::nullopt /* nodeIds */,
           std::nullopt /* keysToUpdate */,
           kArea),
       createThriftPublication(
           {} /* keyVals */,
           {"adj:node1"} /* expiredKeys */,
           std::nullopt /* nodeIds */,
           std::nullopt /* keysToUpdate */,
           kArea)});

  // Default params set ignoreTtl.
  testCases.push_back(
      {"TTL update dropped under ignoreTtl",
       thrift::KeyDumpParams{},
       createThriftPublication(
           {{"adj:node1", createTtlUpdate()}, {"adj:node2", createValue("v")}},
           {} /* expiredKeys */,
           std::nullopt /* nodeIds */,
           std::nullopt /* keysToUpdate */,
           kArea),
       createThriftPublication(
           {{"adj:node2", createValue("v")}},
           {} /* expiredKeys */,
           std::nullopt /* nodeIds */,
           std::nullopt /* keysToUpdate */,
           kArea)});

  testCases.push_back(
      {"only TTL updates under ignoreTtl",
       thrift::KeyDumpParams{},
       createThriftPublication(
           {{"adj:node1", createTtlUpdate()}},
           {} /* expiredKeys */,
           std::nullopt /* nodeIds */,
           std::nullopt /* keysToUpdate */,
           kArea),
       std::nullopt});

  {
    auto params = createUnfilteredParams();
    params.doNotPublishValue() = true;
    testCases.push_back(
        {"value stripped under doNotPublishValue",
         std::move(params),
         createThriftPublication(
             {{"adj:node1", createValue("v")}},
             {} /* expiredKeys */,
             std::nullopt /* nodeIds */,
             std::nullopt /* keysToUpdate */,
             kArea),
         createThriftPublication(
             {{"adj:node1",
               createThriftValueWithoutBinaryValue(createValue("v"))}},
             {} /* expiredKeys */,
             std::nullopt /* nodeIds */,
             std::nullopt /* keysToUpdate */,
             kArea)});
  }

  {
    auto input = createThriftPublication(
        {{"adj:node1", createValue("v")}, {"prefix:node1", createValue("v")}},
        {} /* expiredKeys */,
        std::vector<std::string>{"node1", "node2"} /* nodeIds */,
        std::vector<std::string>{"adj:node3"} /* keysToUpdate */,
        kArea,
        1000 /* timestamp_ms */);
    input.perfEvents() = createPerfEvents();
    testCases.push_back(
        {"nodeIds and tobeUpdatedKeys copied, timestamp_ms unset",
         createKeyParams({"adj:"}),
         std::move(input),
         createThriftPublication(
             {{"adj:node1", createValue("v")}},
             {} /* expiredKeys */,
             std::vector<std::string>{"node1", "node2"} /* nodeIds */,
             std::vector<std::string>{"adj:node3"} /* keysToUpdate */,
             kArea)});
  }

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.name);
    const KvStorePublicationFilter filter(
        {} /* selectAreas */, testCase.params);
    ASSERT_FALSE(filter.keepsAllKeys());

    const auto filtered = filter.apply(testCase.input);

    ASSERT_EQ(testCase.expected.has_value(), filtered.has_value());
    if (filtered.has_value()) {
      EXPECT_EQ(*testCase.expected, *filtered);
      EXPECT_FALSE(filtered->timestamp_ms().has_value());
      EXPECT_FALSE(filtered->perfEvents().has_value());
    }
  }
}

int
main(int argc, char* argv[]) {
  // Parse command line flags
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  const folly::Init init(&argc, &argv);
  FLAGS_logtostderr = true;

  // Run the tests
  return RUN_ALL_TESTS();
}
