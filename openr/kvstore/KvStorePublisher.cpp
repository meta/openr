/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <openr/common/Util.h>
#include <openr/kvstore/KvStorePublisher.h>

namespace openr {

KvStorePublisher::KvStorePublisher(
    std::set<std::string> const& selectAreas,
    thrift::KeyDumpParams filter,
    apache::thrift::ServerStreamPublisher<thrift::Publication>&& publisher,
    std::chrono::steady_clock::time_point subscription_time,
    int64_t total_messages)
    : filter_(selectAreas, std::move(filter)),
      publisher_(std::move(publisher)),
      subscription_time_(subscription_time),
      total_messages_(total_messages) {}

void
KvStorePublisher::publish(const thrift::Publication& pub) {
  if (!filter_.matchesArea(*pub.area())) {
    return;
  }
  if (filter_.keepsAllKeys()) {
    /*
     * No filtering criteria. Accept all updates, TTL updates included. If we
     * don't optimize here, we will have to go through the key values of the
     * publication and copy them one by one.
     */
    thrift::Publication publication(pub);
    publication.timestamp_ms() = getUnixTimeStampMs();
    publisher_.next(std::move(publication));
    return;
  }

  auto publication_filtered = filter_.apply(pub);
  if (publication_filtered) {
    /*
     * There is at least one key value in the publication for the client
     * or there are some expiredKeys
     */
    publication_filtered->timestamp_ms() = getUnixTimeStampMs();
    publisher_.next(std::move(*publication_filtered));
  }
}

} // namespace openr
