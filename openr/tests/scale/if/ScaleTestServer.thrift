/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package "meta.com/openr/tests/scale"

include "thrift/annotation/thrift.thrift"

namespace cpp2 openr.thrift
namespace py3 openr.tests.scale

struct DutConnection {
  1: optional string host;
  2: i32 port = 2018;
}

enum DutRole {
  SPINE = 1,
  LEAF = 2,
}

struct TopologyConfig {
  1: string type = "bbf-simple"; // "bbf-simple" | "bbf-full"
  2: optional DutRole dutRole;
  3: optional i32 numSpines;
  4: optional i32 numLeaves;
  5: optional i32 numSuperSpines;
  6: optional i32 numPods;
  7: optional i32 numSites;
  8: optional i32 numPrefixesPerNode;
  9: optional i32 ecmpWidth;
  // Multi-area: one area name per area. With >= 2 names the base topology is
  // replicated into each named area (self-consistent per area) and the DUT is
  // patched into all of them as an ABR. Unset / empty / a single name keeps the
  // legacy single-area behavior (default area "0").
  10: optional list<string> areas;
  // Derive each node's advertised prefixes from (prefixSeed, nodeName, index)
  // instead of drawing them at random, so an off-box tool can compute the same
  // prefix key names from the topology fields alone, and so re-injecting with
  // the same seed overwrites the previous run's keys instead of adding a fresh
  // disjoint set. Unset or 0 = random prefixes (the historical behaviour).
  11: optional i64 prefixSeed;
}

struct InjectionConfig {
  1: optional bool injectTopology;
  2: optional bool simulateNeighbors;
  3: optional bool enableFakeKvStore;
  // Unset = daemon allocates from its own pool (see --fake_kvstore_base_port).
  4: optional i32 fakeKvStoreBasePort;
  // Unset = disabled (no extra fake keys injected per node).
  5: optional i32 numFakeKeysPerNode;
  6: optional i32 fakeKeyVersionBumpIntervalSec;
  // Required when simulateNeighbors is true.
  7: optional list<string> interfaces;
}

struct ScaleTestConfig {
  1: DutConnection dut;
  2: TopologyConfig topology;
  3: InjectionConfig injection;
}

struct LinkRef {
  1: string localNode;
  2: string remoteNode;
}

// One simulated Spark neighbor's status.
struct SparkNeighborState {
  1: string name;
  2: string state; // SparkNeighState enum name (e.g. "ESTABLISHED")
  3: string dutNode; // DUT node name learned from the DUT's packets
  4: bool failed; // true if the neighbor was failed via downNode/downLink
}

// Aggregate SparkFaker packet stats plus the per-neighbor table. All counters
// are zero and neighbors empty when simulateNeighbors=false for the session.
struct NeighborStats {
  1: i64 hellosSent;
  2: i64 hellosReceived;
  3: i64 handshakesSent;
  4: i64 handshakesReceived;
  5: i64 heartbeatsSent;
  6: i64 heartbeatsReceived;
  7: i64 parseErrors;
  8: i32 neighborsEstablished;
  9: i32 totalNeighbors;
  10: list<SparkNeighborState> neighbors;
}

// Counts from the DUT's currently computed route database, used to confirm the
// DUT converged after topology injection.
struct RouteCounts {
  1: i64 unicastRoutes;
  2: i64 mplsRoutes;
}

struct TestStatus {
  // True iff a session is active (between startTest and stopTest).
  1: bool running;
  // The config the active session was started with. Unset when !running.
  2: optional ScaleTestConfig activeConfig;
  // Names of nodes currently downed via downNode().
  3: list<string> downedNodes;
  // Links currently downed via downLink() (and not subsumed by a downed node).
  4: list<LinkRef> downedLinks;
  // True iff the injector's underlying thrift channel to the DUT is connected.
  5: bool dutConnected;
  // Seconds since the active session started. Unset when !running.
  6: optional i64 elapsedSec;
  // Number of simulated Spark neighbors the active session is driving.
  // Unset when !running or when simulateNeighbors=false.
  7: optional i32 neighborCount;
}

/*
 * KvStore stream test clients. Each subscriber and each poller opens its own
 * connection to the DUT control port (DutConnection, default 2018).
 */

// Identical subscribers on subscribeAndGetAreaKvStores.
struct StreamSubscriberGroup {
  1: i32 count = 1;
  // KeyDumpParams.keys, e.g. ["adj:", "prefix:"]. Empty = all keys.
  2: list<string> keyPrefixes;
  // selectAreas, e.g. ["0"]. Empty = all areas.
  3: set<string> areas;
  // KeyDumpParams.senderId. "{id}" becomes the subscriber id, e.g.
  // "scale-{id}" gives scale-0, scale-1. Unset = no senderId.
  4: optional string senderId;
  5: bool ignoreTtl = true;
  // Wait after each message before the next read, in ms.
  6: i32 readDelayMs = 0;
  // Client stream buffer in messages: the credits granted at subscription
  // (Thrift chunk buffer size). 0 = no credit until the first read.
  7: i32 bufferSize = 100;
  // Wait before a new subscription after a stream end or a failed
  // subscribe, in ms. Negative = no new subscription.
  8: i32 reconnectDelayMs = 2000;
  // Start without reads, as StreamSubscriberChange.stalled.
  9: bool stalled = false;
}

// Identical pollers on getKvStoreKeyValsFilteredArea.
struct StreamPollerGroup {
  1: i32 count = 1;
  // KeyDumpParams.keys. Empty = all keys.
  2: list<string> keyPrefixes;
  3: string area = "0";
  // Wait between the end of one poll and the next, in ms.
  4: i32 intervalMs = 1000;
  // KeyDumpParams.senderId, as StreamSubscriberGroup.senderId.
  5: optional string senderId;
}

// Subscriber ids number the subscribers of all groups from 0 in config
// order; poller ids do the same for pollers.
struct StreamClientsConfig {
  1: list<StreamSubscriberGroup> subscribers;
  2: list<StreamPollerGroup> pollers;
  /*
   * DUT control port, e.g. {host: "::1", port: 2018}. The clients need no
   * running Open/R: each connection attempt resolves the host again, and a
   * failed attempt ends with CONNECTION and waits for the next one.
   * startStreamClients replaces an unset host with the DUT of the active
   * session.
   */
  3: DutConnection dut;
}

// Change to running subscribers. Unset fields keep their value.
struct StreamSubscriberChange {
  // Empty = all subscribers.
  1: list<i32> ids;
  2: optional i32 readDelayMs;
  // true: stop reading, so the client grants no credit. false: read again.
  3: optional bool stalled;
  // Applies from the next subscription.
  4: optional i32 bufferSize;
  // Cancel the current stream and subscribe again without the reconnect
  // delay.
  5: bool reconnect = false;
}

enum StreamEndReason {
  NONE = 0,
  // Subscribe RPC rejected by the DUT admission check.
  LOADSHEDDING = 1,
  // DUT closed the stream at its per-stream queue limit.
  OVERFLOW = 2,
  // DUT closed the stream at its total stream budget.
  BUDGET = 3,
  // Thrift cancelled the stream: no client credit for streamExpireTime.
  CREDIT_TIMEOUT = 4,
  // DUT handler shutdown ("publisher terminated").
  SHUTDOWN = 5,
  // Connection failure or closure.
  CONNECTION = 6,
  // DUT completed the stream without error.
  COMPLETED = 7,
  // This client cancelled the stream: reconnect or stop.
  CLIENT_CANCEL = 8,
  OTHER = 99,
}

enum StreamSubscriberState {
  CONNECTING = 0,
  STREAMING = 1,
  STALLED = 2,
  // Waiting for the reconnect delay.
  WAITING = 3,
  STOPPED = 4,
}

// Byte counts are the Thrift Compact size estimates (serializedSizeZC) that
// Open/R uses for its stream limits.
struct StreamSubscriberStats {
  1: i32 id;
  2: StreamSubscriberState state;
  3: i32 readDelayMs;
  4: i32 bufferSize;
  5: bool stalled;
  // Subscribe RPCs sent, and the ones that returned a snapshot.
  6: i32 subscribeAttempts;
  7: i32 subscriptions;
  // Subscribe attempts after the first one.
  8: i32 reconnects;
  // Last snapshot: all areas, and the subscribe RPC latency.
  9: i64 snapshotBytes;
  10: i64 snapshotKeys;
  11: i64 snapshotLatencyMs;
  // Stream messages over all subscriptions; keys count keyVals and
  // expiredKeys.
  12: i64 messagesReceived;
  13: i64 bytesReceived;
  14: i64 keysReceived;
  15: StreamEndReason lastEndReason;
  16: string lastError;
  // Number of subscriptions and subscribe attempts per end reason.
  17: map<StreamEndReason, i32> endReasons;
}

struct StreamPollerStats {
  1: i32 id;
  2: i64 polls;
  3: i64 errors;
  4: i64 lastBytes;
  5: i64 lastKeys;
  6: i64 lastLatencyMs;
  7: i64 maxLatencyMs;
  8: string lastError;
}

struct StreamClientsStats {
  1: bool running;
  2: list<StreamSubscriberStats> subscribers;
  3: list<StreamPollerStats> pollers;
}

enum SetupErrorReason {
  TOPOLOGY_INVALID = 1,
  DUT_UNREACHABLE = 2,
  PORT_BIND_FAILED = 3,
  INJECTION_FAILED = 4,
  INTERNAL = 99,
}

exception AlreadyRunningError {
  @thrift.ExceptionMessage
  1: string message;
}
exception NotRunningError {
  @thrift.ExceptionMessage
  1: string message;
}
exception UnknownNodeError {
  @thrift.ExceptionMessage
  1: string message;
}
exception UnknownAdjacencyError {
  @thrift.ExceptionMessage
  1: string message;
}
exception StreamClientError {
  @thrift.ExceptionMessage
  1: string message;
}
exception SetupError {
  1: SetupErrorReason reason;
  @thrift.ExceptionMessage
  2: string message;
}

service ScaleTestServer {
  void startTest(1: ScaleTestConfig config) throws (
    1: AlreadyRunningError already,
    2: SetupError setup,
  );
  void stopTest() throws (1: NotRunningError notRunning);
  TestStatus getTestStatus();

  list<string> listNodes() throws (1: NotRunningError notRunning);

  void downNode(1: string nodeName) throws (
    1: NotRunningError notRunning,
    2: UnknownNodeError unknown,
  );
  void upNode(1: string nodeName) throws (
    1: NotRunningError notRunning,
    2: UnknownNodeError unknown,
  );
  void downLink(1: string localNode, 2: string remoteNode) throws (
    1: NotRunningError notRunning,
    2: UnknownNodeError unknown,
    3: UnknownAdjacencyError unknownAdj,
  );
  void upLink(1: string localNode, 2: string remoteNode) throws (
    1: NotRunningError notRunning,
    2: UnknownNodeError unknown,
    3: UnknownAdjacencyError unknownAdj,
  );

  // Bulk variants: down/up a SET of nodes (or links) as ONE coherent KvStore
  // update wave so the DUT sees a single convergence event (e.g. "fail a pod"),
  // not N sequential ones. Validation is atomic — if ANY member is invalid the
  // whole batch is rejected and nothing is applied. Each affected neighbor's adj
  // DB is rebuilt exactly once against the final downed state.
  void downNodes(1: list<string> nodeNames) throws (
    1: NotRunningError notRunning,
    2: UnknownNodeError unknown,
  );
  void upNodes(1: list<string> nodeNames) throws (
    1: NotRunningError notRunning,
    2: UnknownNodeError unknown,
  );
  void downLinks(1: list<LinkRef> links) throws (
    1: NotRunningError notRunning,
    2: UnknownNodeError unknown,
    3: UnknownAdjacencyError unknownAdj,
  );
  void upLinks(1: list<LinkRef> links) throws (
    1: NotRunningError notRunning,
    2: UnknownNodeError unknown,
    3: UnknownAdjacencyError unknownAdj,
  );

  // Flap a single link: downLink -> wait intervalMs -> upLink, repeated `cycles`
  // times. A link flap is bidirectional, so each cycle withdraws/restores BOTH
  // endpoints' adjacencies (reuses the symmetric downLink/upLink path).
  //
  // FIRE-AND-FORGET: the call validates the link synchronously (so bad args /
  // not-running surface immediately) then runs the flap on a background worker
  // and returns right away — the operator can keep issuing commands and watch
  // getTestStatus(), whose downedLinks reflects the in-flight flap. cycles <= 0
  // is a no-op; a negative intervalMs is treated as 0. The link ends UP after a
  // clean flap; if the session is stopped mid-flap it may be left down.
  void flapLink(
    1: string localNode,
    2: string remoteNode,
    3: i32 cycles,
    4: i32 intervalMs,
  ) throws (
    1: NotRunningError notRunning,
    2: UnknownNodeError unknown,
    3: UnknownAdjacencyError unknownAdj,
  );

  // Bulk fire-and-forget flap: flap a SET of links together — each cycle drives
  // the symmetric bulk downLinks/upLinks so all listed links go down (and back
  // up) as one coherent wave. Same fire-and-forget / validation semantics as
  // flapLink.
  void flapLinks(
    1: list<LinkRef> links,
    2: i32 cycles,
    3: i32 intervalMs,
  ) throws (
    1: NotRunningError notRunning,
    2: UnknownNodeError unknown,
    3: UnknownAdjacencyError unknownAdj,
  );

  // Empty regex = use the daemon's default counter set. Without a session,
  // reads the DUT of the stream clients. Empty when neither runs or the
  // read fails.
  map<string, i64> getDutCounters(1: string regexFilter);

  // Structured report of the simulated Spark neighbors: aggregate packet stats
  // plus the per-neighbor table. All-zero / empty when simulateNeighbors is
  // false for the active session.
  NeighborStats getNeighborStats() throws (1: NotRunningError notRunning);

  // Query the DUT's currently computed route database and return route counts.
  // Pull-based equivalent of the legacy post-injection route verification; the
  // operator calls it once the DUT is expected to have converged. Returns zero
  // counts if the injector's channel to the DUT is not connected.
  RouteCounts verifyRoutes() throws (1: NotRunningError notRunning);

  /*
   * KvStore stream test clients (StreamClientsConfig). They run with or
   * without a session: startTest is optional, and stopTest leaves them
   * running. At most one set of clients runs at a time.
   */
  void startStreamClients(1: StreamClientsConfig config) throws (
    1: AlreadyRunningError already,
    2: StreamClientError invalid,
  );
  void changeStreamSubscribers(1: StreamSubscriberChange change) throws (
    1: StreamClientError invalid,
  );
  void stopStreamClients() throws (1: StreamClientError notStarted);
  // running is false when no stream clients run.
  StreamClientsStats getStreamClientsStats();
}
