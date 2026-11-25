/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <vector>

#include <fmt/format.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>

#include <openr/common/Constants.h>
#include <openr/common/MainUtil.h>
#include <openr/config/Config.h>
#include <openr/tests/utils/Utils.h>

namespace openr {

TEST(MainUtilTest, ConfigureCtrlThriftServerLimits) {
  apache::thrift::ThriftServer server;

  configureCtrlThriftServerLimits(server);

  EXPECT_EQ(
      Constants::kOpenrCtrlThriftMaxConnections, server.getMaxConnections());
  EXPECT_EQ(Constants::kOpenrCtrlThriftMaxRequests, server.getMaxRequests());
  EXPECT_EQ(
      Constants::kOpenrCtrlThriftSocketWriteTimeout,
      server.getSocketWriteTimeout());
  EXPECT_EQ(
      Constants::kOpenrCtrlThriftEgressMemoryLimitBytes,
      server.getEgressMemoryLimit());
  EXPECT_EQ(
      Constants::kOpenrCtrlThriftEgressBackpressureThresholdBytes,
      server.getEgressBufferBackpressureThreshold());
}

TEST(MainUtilTest, KvStoreStreamSendQuotaOverridesCtrlThreshold) {
  apache::thrift::ThriftServer server;

  configureCtrlThriftServerLimits(server);
  configureKvStoreStreamLimits(server);

  EXPECT_EQ(
      Constants::kKvStoreStreamSendQuotaBytes,
      server.getEgressBufferBackpressureThreshold());
  EXPECT_EQ(
      Constants::kOpenrCtrlThriftEgressMemoryLimitBytes,
      server.getEgressMemoryLimit());
  // Streams resume at Thrift's default recovery factor 0.75 (1.5 MiB).
  EXPECT_DOUBLE_EQ(0.75, server.getEgressBufferRecoveryFactor());
}

TEST(MainUtilTest, KvStoreStreamSendQuotaWithoutCtrlLimits) {
  const apache::thrift::ThriftServer defaults;
  apache::thrift::ThriftServer server;

  configureKvStoreStreamLimits(server);

  EXPECT_EQ(
      Constants::kKvStoreStreamSendQuotaBytes,
      server.getEgressBufferBackpressureThreshold());
  EXPECT_DOUBLE_EQ(0.75, server.getEgressBufferRecoveryFactor());
  // The quota leaves the other server limits at their defaults.
  EXPECT_EQ(defaults.getEgressMemoryLimit(), server.getEgressMemoryLimit());
  EXPECT_EQ(defaults.getMaxConnections(), server.getMaxConnections());
}

/*
 * configureCtrlThriftServer() applies the server limits and the send quota
 * with enable_ctrl_thrift_server_limits, and keeps the Thrift defaults
 * without it.
 */
TEST(MainUtilTest, ConfigureCtrlThriftServerAppliesLimitsWithKnob) {
  const apache::thrift::ThriftServer defaults;
  struct TestCase {
    bool ctrlLimits;
    size_t expectedThreshold;
  };
  const std::vector<TestCase> testCases{
      {false /* ctrlLimits */, defaults.getEgressBufferBackpressureThreshold()},
      {true /* ctrlLimits */, Constants::kKvStoreStreamSendQuotaBytes},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(fmt::format("ctrlLimits={}", testCase.ctrlLimits));
    auto tConfig = getBasicOpenrConfig();
    tConfig.enable_ctrl_thrift_server_limits() = testCase.ctrlLimits;
    apache::thrift::ThriftServer server;

    configureCtrlThriftServer(Config(tConfig), server);

    EXPECT_EQ(
        testCase.expectedThreshold,
        server.getEgressBufferBackpressureThreshold());
    EXPECT_EQ(
        testCase.ctrlLimits ? Constants::kOpenrCtrlThriftEgressMemoryLimitBytes
                            : defaults.getEgressMemoryLimit(),
        server.getEgressMemoryLimit());
    EXPECT_EQ(
        testCase.ctrlLimits ? Constants::kOpenrCtrlThriftMaxConnections
                            : defaults.getMaxConnections(),
        server.getMaxConnections());
  }
}

} // namespace openr

int
main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  const folly::Init init(&argc, &argv);
  return RUN_ALL_TESTS();
}
