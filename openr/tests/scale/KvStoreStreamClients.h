/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <folly/ExceptionWrapper.h>
#include <folly/Try.h>
#include <folly/coro/AsyncScope.h>
#include <folly/executors/IOThreadPoolExecutor.h>

#include <openr/tests/scale/if/gen-cpp2/ScaleTestServer_types.h>

namespace openr {

namespace detail {
struct StreamSubscriber;
struct StreamPoller;
} // namespace detail

/*
 * KvStore stream test clients against the control port of a DUT:
 * subscribers on subscribeAndGetAreaKvStores and pollers on
 * getKvStoreKeyValsFilteredArea. Each client opens its own connection and
 * runs as a coroutine on an IO thread pool. A subscriber reads its stream at
 * the configured rate, stalls on request, and subscribes again after its
 * stream ends. The constructor starts all clients; the destructor cancels
 * and joins them.
 *
 * The clients need no running Open/R, as clients that start on a node before
 * Open/R: each connection attempt resolves the DUT host again (blocking its
 * IO thread), and a failed attempt counts as a CONNECTION end before the
 * next attempt.
 *
 * Thread-safety: the public methods may be called from any thread.
 */
class KvStoreStreamClients {
 public:
  /*
   * Throws std::invalid_argument for an unset or empty DUT host, a DUT port
   * outside 1-65535, or a negative count, read delay, buffer size or poll
   * interval.
   *
   * onStatsChange runs on a client IO thread after each stats change, outside
   * all client locks. Tests wait on it.
   */
  explicit KvStoreStreamClients(
      const thrift::StreamClientsConfig& config,
      std::function<void()> onStatsChange = nullptr);

  // Blocks until every client has stopped.
  ~KvStoreStreamClients();

  KvStoreStreamClients(const KvStoreStreamClients&) = delete;
  KvStoreStreamClients& operator=(const KvStoreStreamClients&) = delete;

  /*
   * Throws std::invalid_argument for an unknown subscriber id or a negative
   * value, and then changes no subscriber.
   */
  void change(const thrift::StreamSubscriberChange& change);

  thrift::StreamClientsStats getStats() const;

  /*
   * The DUT fb303 counters that match `regex`, read on a new connection, e.g.
   * "^subscribers\\.kvstore". Blocks until the DUT answers or the RPC fails.
   */
  folly::Try<std::map<std::string, int64_t>> getDutCounters(
      const std::string& regex);

  /*
   * Maps the exception that ended a stream or failed a subscribe RPC to its
   * cause. Open/R reports its stream closures by message, e.g.
   * "KvStore stream queue limit of 33554432 bytes exceeded".
   */
  static thrift::StreamEndReason classifyEnd(
      const folly::exception_wrapper& ew);

 private:
  const thrift::DutConnection dut_;
  const std::function<void()> onStatsChange_;
  std::vector<std::unique_ptr<detail::StreamSubscriber>> subscribers_;
  std::vector<std::unique_ptr<detail::StreamPoller>> pollers_;
  std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor_;
  folly::coro::CancellableAsyncScope scope_;
};

} // namespace openr
