/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <optional>
#include <set>
#include <string>
#include <vector>

#include <openr/if/gen-cpp2/KvStore_types.h>
#include <openr/kvstore/KvStoreUtil.h>

namespace openr {

/*
 * Area and key filter of one KvStore subscription. Example: areas {"spine"}
 * with filter.keys {"prefix:"} keeps the "prefix:" keys of area "spine".
 *
 * Callers apply it to a publication in three steps: skip the publication
 * unless matchesArea(); use the publication unchanged when keepsAllKeys();
 * otherwise send what apply() returns, if anything.
 */
class KvStorePublicationFilter {
 public:
  KvStorePublicationFilter(
      std::set<std::string> const& selectAreas, thrift::KeyDumpParams filter);

  // An empty area set matches every area.
  bool matchesArea(const std::string& area) const;

  /*
   * True when the filter keeps every key and value of a publication in a
   * matching area. Such subscribers can share the original publication.
   */
  bool
  keepsAllKeys() const {
    return keepsAllKeys_;
  }

  /*
   * The keys and expired keys of `pub` that match the filter, or std::nullopt
   * when none match. Precondition: the caller has checked matchesArea() and
   * uses the original publication when keepsAllKeys(). apply() is no
   * pass-through: it drops perfEvents and timestamp_ms, and it returns
   * std::nullopt for a publication with neither keyVals nor expiredKeys.
   */
  std::optional<thrift::Publication> apply(
      const thrift::Publication& pub) const;

 private:
  static bool computeKeepsAllKeys(const thrift::KeyDumpParams& filter);

  thrift::KeyVals getFilteredKeyVals(const thrift::KeyVals& origKeyVals) const;
  std::vector<std::string> getFilteredExpiredKeys(
      const std::vector<std::string>& origExpiredKeys) const;

  // set of areas whose updates should be published. If empty, publish all
  std::set<std::string> selectAreas_;
  thrift::KeyDumpParams filter_;
  KvStoreFilters keyPrefixFilter_;
  // Computed from filter_, which is initialized first.
  const bool keepsAllKeys_;
};

} // namespace openr
