# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


from __future__ import annotations

import unittest
from typing import Any

import later.unittest
from openr.tests.scale.ScaleTestServer.thrift_types import (
    DutConnection,
    LinkRef,
    StreamClientsConfig,
    StreamClientsStats,
    StreamEndReason,
    StreamPollerGroup,
    StreamPollerStats,
    StreamSubscriberChange,
    StreamSubscriberGroup,
    StreamSubscriberState,
    StreamSubscriberStats,
)
from openr.tests.scale.scripts.scaletest import _parse_args
from openr.tests.scale.scripts.scaletest_stream import (
    build_change,
    build_stream_config,
    format_stream_stats,
    run_scenario,
)


class BuildStreamConfigTest(unittest.TestCase):
    def test_maps_subscriber_and_poller_fields(self) -> None:
        doc = {
            "subscribers": [
                {
                    "count": 3,
                    "keyPrefixes": ["adj:"],
                    "areas": ["0"],
                    "senderId": "scale-{id}",
                    "ignoreTtl": False,
                    "readDelayMs": 5,
                    "bufferSize": 0,
                    "reconnectDelayMs": -1,
                    "stalled": True,
                }
            ],
            "pollers": [
                {"count": 2, "keyPrefixes": ["prefix:"], "area": "1", "intervalMs": 500}
            ],
        }
        expected = StreamClientsConfig(
            subscribers=[
                StreamSubscriberGroup(
                    count=3,
                    keyPrefixes=["adj:"],
                    areas={"0"},
                    senderId="scale-{id}",
                    ignoreTtl=False,
                    readDelayMs=5,
                    bufferSize=0,
                    reconnectDelayMs=-1,
                    stalled=True,
                )
            ],
            pollers=[
                StreamPollerGroup(
                    count=2, keyPrefixes=["prefix:"], area="1", intervalMs=500
                )
            ],
        )
        self.assertEqual(build_stream_config(doc), expected)

    def test_maps_dut(self) -> None:
        config = build_stream_config({"dut": {"host": "dut.example", "port": 2018}})
        self.assertEqual(config.dut, DutConnection(host="dut.example", port=2018))

    def test_absent_fields_keep_idl_defaults(self) -> None:
        config = build_stream_config({"subscribers": [{"count": 1}]})
        self.assertEqual(
            config, StreamClientsConfig(subscribers=[StreamSubscriberGroup(count=1)])
        )

    def test_unknown_field_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown fields: readDelay"):
            build_stream_config({"subscribers": [{"readDelay": 5}]})

    def test_scalar_key_prefixes_raises(self) -> None:
        # A bare scalar would become one key prefix per character.
        with self.assertRaisesRegex(ValueError, r"subscribers\[0\]\.keyPrefixes"):
            build_stream_config({"subscribers": [{"keyPrefixes": "adj:"}]})


class BuildChangeTest(unittest.TestCase):
    def test_unset_options_stay_unset(self) -> None:
        self.assertEqual(
            build_change([1, 2], stalled=True),
            StreamSubscriberChange(ids=[1, 2], stalled=True),
        )


class FormatStreamStatsTest(unittest.TestCase):
    def test_lists_end_reasons_errors_and_counters(self) -> None:
        stats = StreamClientsStats(
            running=True,
            subscribers=[
                StreamSubscriberStats(
                    id=0,
                    state=StreamSubscriberState.STOPPED,
                    lastEndReason=StreamEndReason.LOADSHEDDING,
                    endReasons={StreamEndReason.LOADSHEDDING: 1},
                    lastError="too many subscribers",
                )
            ],
            pollers=[StreamPollerStats(id=0, polls=7)],
        )
        text = format_stream_stats(
            stats, {"subscribers.kvstore.subscribe_rejects.count": 1}
        )
        self.assertIn("LOADSHEDDING=1", text)
        self.assertIn("subscriber 0 last error: too many subscribers", text)
        self.assertIn("subscribers.kvstore.subscribe_rejects.count\t1", text)


class FakeScaleTestClient:
    """Records the ScaleTestServer calls of a scenario."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.stats = StreamClientsStats(running=False)

    async def startStreamClients(self, config: StreamClientsConfig) -> None:
        self.calls.append(("start", config))
        count = sum(group.count for group in config.subscribers)
        self.stats = StreamClientsStats(
            running=True,
            subscribers=[
                StreamSubscriberStats(id=i, subscriptions=1) for i in range(count)
            ],
        )

    async def changeStreamSubscribers(self, change: StreamSubscriberChange) -> None:
        self.calls.append(("change", change))

    async def stopStreamClients(self) -> None:
        self.calls.append(("stop", None))
        self.stats = StreamClientsStats(running=False)

    async def getStreamClientsStats(self) -> StreamClientsStats:
        return self.stats

    async def getDutCounters(self, regex: str) -> dict[str, int]:
        return {}

    async def flapLinks(self, links: list[LinkRef], cycles: int, interval: int) -> None:
        self.calls.append(("flap", (list(links), cycles, interval)))


class RunScenarioTest(later.unittest.TestCase):
    async def test_gate_holds_snapshots_then_resumes(self) -> None:
        client = FakeScaleTestClient()
        await run_scenario(
            client, _parse_args(["stream-scenario", "gate", "--settle", "0"])
        )
        self.assertEqual(
            client.calls,
            [
                (
                    "start",
                    StreamClientsConfig(
                        subscribers=[
                            StreamSubscriberGroup(
                                count=8,
                                areas={"0"},
                                senderId="scale-{id}",
                                stalled=True,
                                bufferSize=0,
                                reconnectDelayMs=-1,
                            )
                        ]
                    ),
                ),
                ("change", StreamSubscriberChange(ids=[], stalled=False)),
                ("stop", None),
            ],
        )

    async def test_stalled_stalls_then_resumes_subscriber_zero(self) -> None:
        client = FakeScaleTestClient()
        await run_scenario(
            client,
            _parse_args(
                ["stream-scenario", "stalled", "--duration", "0", "--settle", "0"]
            ),
        )
        changes = [arg for name, arg in client.calls if name == "change"]
        self.assertEqual(
            changes,
            [
                StreamSubscriberChange(ids=[0], stalled=True),
                StreamSubscriberChange(ids=[0], stalled=False),
            ],
        )

    async def test_churn_flaps_links(self) -> None:
        client = FakeScaleTestClient()
        args = _parse_args(
            ["stream-scenario", "churn", "--links", "a:b", "--duration", "0"]
            + ["--cycles", "2", "--interval", "100", "--settle", "0"]
        )
        await run_scenario(client, args)
        flaps = [arg for name, arg in client.calls if name == "flap"]
        self.assertEqual(flaps, [([LinkRef(localNode="a", remoteNode="b")], 2, 100)])

    async def test_churn_without_links_starts_nothing(self) -> None:
        client = FakeScaleTestClient()
        with self.assertRaisesRegex(ValueError, "--links"):
            await run_scenario(client, _parse_args(["stream-scenario", "churn"]))
        self.assertEqual(client.calls, [])

    async def test_dut_host_names_the_dut(self) -> None:
        client = FakeScaleTestClient()
        args = _parse_args(
            ["stream-scenario", "steady", "--duration", "0", "--settle", "0"]
            + ["--dut-host", "dut.example", "--dut-port", "2018"]
        )
        await run_scenario(client, args)
        config = client.calls[0][1]
        self.assertEqual(config.dut, DutConnection(host="dut.example", port=2018))

    async def test_cold_start_starts_clients_before_open_r(self) -> None:
        client = FakeScaleTestClient()
        args = _parse_args(
            ["stream-scenario", "cold-start", "--duration", "0", "--settle", "0"]
            + ["--pollers", "1"]
        )
        await run_scenario(client, args)
        self.assertEqual([name for name, _ in client.calls], ["start", "stop"])
        config = client.calls[0][1]
        self.assertEqual(
            [group.senderId for group in config.pollers], ["scale-poll-{id}"]
        )

    async def test_keep_leaves_clients_running(self) -> None:
        client = FakeScaleTestClient()
        await run_scenario(
            client, _parse_args(["stream-scenario", "cap", "--settle", "0", "--keep"])
        )
        self.assertEqual([name for name, _ in client.calls], ["start"])
