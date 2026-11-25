/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <openr/kvstore/KvStorePublicationFilter.h>

#include <folly/logging/xlog.h>

#include <openr/common/Util.h>

namespace openr {

KvStorePublicationFilter::KvStorePublicationFilter(
    std::set<std::string> const& selectAreas, thrift::KeyDumpParams filter)
    : selectAreas_(selectAreas),
      filter_(std::move(filter)),
      keyPrefixFilter_(
          *filter_.keys(),
          *filter_.originatorIds(),
          filter_.oper().has_value() ? *filter_.oper()
                                     : thrift::FilterOperator::OR),
      keepsAllKeys_(computeKeepsAllKeys(filter_)) {}

bool
KvStorePublicationFilter::computeKeepsAllKeys(
    const thrift::KeyDumpParams& filter) {
  return (!apache::thrift::is_non_optional_field_set_manually_or_by_serializer(
              filter.keys()) ||
          (*filter.keys()).empty()) &&
      (!apache::thrift::is_non_optional_field_set_manually_or_by_serializer(
           filter.originatorIds()) ||
       (*filter.originatorIds()).empty()) &&
      !*filter.ignoreTtl() && !*filter.doNotPublishValue();
}

bool
KvStorePublicationFilter::matchesArea(const std::string& area) const {
  return selectAreas_.empty() || selectAreas_.count(area);
}

/**
 * A publication object (param) can have multiple key value pairs as follows.
 * pub = {"prefix1": value1, "prefix2": value2, "random-key": random-value}
 * The value objects also contain originator-id.
 *
 * A thrift client can specify filters to be applied on prefixes and
 * originator-ids. Logical operator OR or AND is also specified for combining
 * matches on prefix and originator-id.
 *
 * Suppose filter specified is {"prefix.*"} and logical operator is AND. After
 * applying the filter, the filtered publication = {"prefix1": value1,
 * "prefix2": value2} will be returned to the client (i.e., published) on the
 * stream.
 */
std::optional<thrift::Publication>
KvStorePublicationFilter::apply(const thrift::Publication& pub) const {
  XDCHECK(!keepsAllKeys_) << "use the original publication";
  thrift::Publication publication_filtered;
  publication_filtered.area() = *pub.area();
  publication_filtered.keyVals() = getFilteredKeyVals(*pub.keyVals());
  publication_filtered.expiredKeys() =
      getFilteredExpiredKeys(*pub.expiredKeys());

  if (pub.nodeIds()) {
    publication_filtered.nodeIds() = *pub.nodeIds();
  }

  if (pub.tobeUpdatedKeys()) {
    publication_filtered.tobeUpdatedKeys() = *pub.tobeUpdatedKeys();
  }

  if (publication_filtered.keyVals()->empty() &&
      publication_filtered.expiredKeys()->empty()) {
    return std::nullopt;
  }
  return publication_filtered;
}

thrift::KeyVals
KvStorePublicationFilter::getFilteredKeyVals(
    const thrift::KeyVals& origKeyVals) const {
  /*
   * The value field may be explicitly excluded/ignored by the doNotPublishValue
   * flag
   */
  thrift::KeyVals keyvals;
  for (auto& [key, val] : origKeyVals) {
    if (*filter_.ignoreTtl() && !val.value().has_value()) {
      // ignore TTL updates
      continue;
    }

    if (!keyPrefixFilter_.keyMatch(key, val)) {
      continue;
    }

    if (*filter_.doNotPublishValue() && val.value().has_value()) {
      // Exclude Value.value if it's filtered
      keyvals.emplace(key, createThriftValueWithoutBinaryValue(val));
    } else {
      keyvals.emplace(key, val);
    }
  }

  return keyvals;
}

std::vector<std::string>
KvStorePublicationFilter::getFilteredExpiredKeys(
    const std::vector<std::string>& origExpiredKeys) const {
  std::vector<std::string> expiredKeys;
  for (auto& key : origExpiredKeys) {
    if (!keyPrefixFilter_.keyMatch(key)) {
      continue;
    }

    expiredKeys.emplace_back(key);
  }
  return expiredKeys;
}

} // namespace openr
