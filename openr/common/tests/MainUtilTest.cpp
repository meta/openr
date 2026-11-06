/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>

#include <openr/common/Constants.h>
#include <openr/common/MainUtil.h>

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

} // namespace openr

int
main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  const folly::Init init(&argc, &argv);
  return RUN_ALL_TESTS();
}
