# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# Sources owned by the Buck targets in openr/ctrl-server.
set(OPENR_KVSTORE_STREAM_MERGE_BUFFER_SOURCES
  openr/ctrl-server/KvStoreStreamMergeBuffer.cpp
)
set(OPENR_KVSTORE_STREAM_REGISTRY_SOURCES
  openr/ctrl-server/KvStoreStreamRegistry.cpp
)
set(OPENR_CTRL_HANDLER_SOURCES
  openr/ctrl-server/OpenrCtrlHandler.cpp
)

set(OPENR_CTRL_SERVER_EXPECTED_SOURCE_COUNT 3)

# Create the Thrift control-plane handler after all module implementations
# it dispatches to are available. The broad public dependency surface
# follows the module pointers and generated service bases in its header.
macro(openr_add_ctrl_server_library)
  # Buck2 target: //openr/ctrl-server:kvstore-stream-merge-buffer
  openr_add_library(
    NAME openr_kvstore_stream_merge_buffer
    SOURCES ${OPENR_KVSTORE_STREAM_MERGE_BUFFER_SOURCES}
    PRIVATE_DEPENDENCIES
      openr_constants
    PUBLIC_DEPENDENCIES
      kv_store_cpp2
      Folly::folly
      thrift::rpc
  )
  add_library(OpenR::kvstore_stream_merge_buffer
    ALIAS openr_kvstore_stream_merge_buffer)

  # Buck2 target: //openr/ctrl-server:kvstore-stream-registry
  openr_add_library(
    NAME openr_kvstore_stream_registry
    SOURCES ${OPENR_KVSTORE_STREAM_REGISTRY_SOURCES}
    PRIVATE_DEPENDENCIES
      fb303::fb303
      openr_constants
      openr_util
    PUBLIC_DEPENDENCIES
      openr_kvstore_stream_merge_buffer
      openr_kvstore_publication_filter
      openr_ctrl_cpp2
      kv_store_cpp2
      Folly::folly
      thrift::rpc
  )
  add_library(OpenR::kvstore_stream_registry ALIAS openr_kvstore_stream_registry)

  # Buck2 target: //openr/ctrl-server:openr-ctrl-handler
  openr_add_library(
    NAME openr_ctrl_handler
    SOURCES ${OPENR_CTRL_HANDLER_SOURCES}
    PRIVATE_DEPENDENCIES
      fmt::fmt
      ${RE2}
      openr_constants
      openr_lsdb_util
      openr_profiler
      openr_util
      openr_kvstore_util
      openr_log_sample
      glog::glog
    PUBLIC_DEPENDENCIES
      fb303::fb303
      openr_common
      openr_config
      openr_kvstore_stream_registry
      openr_persistent_store
      openr_decision
      openr_dispatcher
      openr_fib
      openr_kvstore
      openr_link_monitor
      openr_monitor
      openr_prefix_manager
      openr_spark
      openr_ctrl_cpp2
      openr_ctrl_cpp_cpp2
      types_cpp2
      Folly::folly
      thrift::rpc
  )
  add_library(OpenR::ctrl_handler ALIAS openr_ctrl_handler)
endmacro()
