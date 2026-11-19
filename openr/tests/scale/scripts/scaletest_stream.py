# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


"""KvStore stream test client commands for the scale-test CLI.

The stream clients run in `scale_test_server_d`: subscribers on
subscribeAndGetAreaKvStores and pollers on getKvStoreKeyValsFilteredArea, each
on its own connection to the DUT control port. They target the DUT in their
config, or the DUT of the active session when the config names none. They run
with or without a session and need no running Open/R: a failed connection
counts as a CONNECTION end, and the client retries.

Subcommands:
  stream-start --config <yaml>  Start stream subscribers and pollers.
  stream-change [options]       Change the read delay, stall, buffer size, or
                                reconnect subscribers.
  stream-stop                   Stop the stream clients.
  stream-stats [--counters RE]  Print the client stats and the DUT counters.
  stream-scenario <name>        Run a scenario and print the stats and the DUT
                                counters after each phase.
"""

from __future__ import annotations

import argparse
import asyncio
import shlex
import subprocess
from collections.abc import Awaitable, Callable, Iterable, Mapping
from typing import Any

import yaml
from openr.tests.scale.ScaleTestServer.thrift_types import (
    DutConnection,
    LinkRef,
    StreamClientsConfig,
    StreamClientsStats,
    StreamPollerGroup,
    StreamSubscriberChange,
    StreamSubscriberGroup,
)

# DUT counters of the Open/R KvStore stream protections.
DEFAULT_COUNTER_REGEX = r"^subscribers\.kvstore"
# Open/R admission limits: kvstore_stream_max_subscribers and
# kvstore_stream_max_snapshots_in_flight.
DUT_MAX_SUBSCRIBERS = 16
DUT_MAX_SNAPSHOTS_IN_FLIGHT = 4

Handler = Callable[[Any, argparse.Namespace], Awaitable[int]]


def _str_list(value: Any) -> list[str]:
    # A bare scalar would become a list of characters; require a YAML list.
    if not isinstance(value, list):
        raise ValueError(f"expected a list (got {type(value).__name__})")
    return [str(v) for v in value]


_SUBSCRIBER_FIELDS: dict[str, Callable[[Any], Any]] = {
    "count": int,
    "keyPrefixes": _str_list,
    "areas": lambda v: set(_str_list(v)),
    "senderId": str,
    "ignoreTtl": bool,
    "readDelayMs": int,
    "bufferSize": int,
    "reconnectDelayMs": int,
    "stalled": bool,
}

_POLLER_FIELDS: dict[str, Callable[[Any], Any]] = {
    "count": int,
    "keyPrefixes": _str_list,
    "area": str,
    "intervalMs": int,
    "senderId": str,
}

_DUT_FIELDS: dict[str, Callable[[Any], Any]] = {
    "host": str,
    "port": int,
}


def _build_fields(
    doc: Any, fields: Mapping[str, Callable[[Any], Any]], section: str
) -> dict[str, Any]:
    if not isinstance(doc, Mapping):
        raise ValueError(f"{section} must be a mapping")
    unknown = sorted(set(doc) - set(fields))
    if unknown:
        raise ValueError(f"{section} has unknown fields: {', '.join(unknown)}")
    out = {}
    for key, convert in fields.items():
        if key not in doc:
            continue
        try:
            out[key] = convert(doc[key])
        except ValueError as ex:
            raise ValueError(f"{section}.{key}: {ex}") from ex
    return out


def _groups(doc: Mapping[str, Any], name: str) -> list[Any]:
    groups = doc.get(name) or []
    if not isinstance(groups, list):
        raise ValueError(f"{name} must be a list of groups")
    return groups


def build_stream_config(doc: Mapping[str, Any]) -> StreamClientsConfig:
    """Converts a parsed YAML mapping into a StreamClientsConfig.

    Fields absent from the YAML keep their Thrift IDL defaults.
    """
    unknown = sorted(set(doc) - {"dut", "subscribers", "pollers"})
    if unknown:
        raise ValueError(f"unknown sections: {', '.join(unknown)}")
    dut = DutConnection(**_build_fields(doc.get("dut") or {}, _DUT_FIELDS, "dut"))
    subscribers = [
        StreamSubscriberGroup(
            **_build_fields(group, _SUBSCRIBER_FIELDS, f"subscribers[{i}]")
        )
        for i, group in enumerate(_groups(doc, "subscribers"))
    ]
    pollers = [
        StreamPollerGroup(**_build_fields(group, _POLLER_FIELDS, f"pollers[{i}]"))
        for i, group in enumerate(_groups(doc, "pollers"))
    ]
    return StreamClientsConfig(subscribers=subscribers, pollers=pollers, dut=dut)


def build_change(
    ids: Iterable[int],
    read_delay_ms: int | None = None,
    stalled: bool | None = None,
    buffer_size: int | None = None,
    reconnect: bool = False,
) -> StreamSubscriberChange:
    return StreamSubscriberChange(
        ids=list(ids),
        readDelayMs=read_delay_ms,
        stalled=stalled,
        bufferSize=buffer_size,
        reconnect=reconnect,
    )


def _format_end_reasons(end_reasons: Mapping[Any, int]) -> str:
    if not end_reasons:
        return "-"
    return ",".join(
        f"{reason.name}={count}"
        for reason, count in sorted(end_reasons.items(), key=lambda kv: kv[0].value)
    )


def format_stream_stats(stats: StreamClientsStats, counters: Mapping[str, int]) -> str:
    lines = [f"running: {stats.running}"]
    if stats.subscribers:
        lines.append(
            f"{'SUB':>4} {'STATE':<10} {'STALL':<5} {'DELAY':>5} {'BUF':>4} "
            f"{'SUBS':>4} {'RECON':>5} {'SNAP_BYTES':>11} {'SNAP_KEYS':>9} "
            f"{'SNAP_MS':>7} {'MSGS':>7} {'BYTES':>11} {'KEYS':>8} "
            f"{'LAST_END':<14} END_REASONS"
        )
        for s in stats.subscribers:
            lines.append(
                f"{s.id:>4} {s.state.name:<10} {'yes' if s.stalled else 'no':<5} "
                f"{s.readDelayMs:>5} {s.bufferSize:>4} {s.subscriptions:>4} "
                f"{s.reconnects:>5} {s.snapshotBytes:>11} {s.snapshotKeys:>9} "
                f"{s.snapshotLatencyMs:>7} {s.messagesReceived:>7} "
                f"{s.bytesReceived:>11} {s.keysReceived:>8} "
                f"{s.lastEndReason.name:<14} {_format_end_reasons(s.endReasons)}"
            )
        errors = [s for s in stats.subscribers if s.lastError]
        for s in errors:
            lines.append(f"subscriber {s.id} last error: {s.lastError}")
    if stats.pollers:
        lines.append(
            f"{'POLL':>4} {'POLLS':>6} {'ERRORS':>6} {'LAST_BYTES':>11} "
            f"{'LAST_KEYS':>9} {'LAST_MS':>7} {'MAX_MS':>7}"
        )
        for p in stats.pollers:
            lines.append(
                f"{p.id:>4} {p.polls:>6} {p.errors:>6} {p.lastBytes:>11} "
                f"{p.lastKeys:>9} {p.lastLatencyMs:>7} {p.maxLatencyMs:>7}"
            )
            if p.lastError:
                lines.append(f"poller {p.id} last error: {p.lastError}")
    lines.append("DUT counters:")
    if not counters:
        lines.append("  (none)")
    for key in sorted(counters):
        lines.append(f"  {key}\t{counters[key]}")
    return "\n".join(lines)


async def _print_report(client: Any, regex: str, title: str | None = None) -> None:
    stats = await client.getStreamClientsStats()
    counters = await client.getDutCounters(regex)
    if title:
        print(f"== {title} ==")
    print(format_stream_stats(stats, counters))


async def cmd_stream_start(client: Any, args: argparse.Namespace) -> int:
    with open(args.config) as f:
        doc = yaml.safe_load(f) or {}
    config = build_stream_config(doc)
    await client.startStreamClients(config)
    print(
        f"stream clients started: "
        f"{sum(g.count for g in config.subscribers)} subscribers, "
        f"{sum(g.count for g in config.pollers)} pollers"
    )
    return 0


async def cmd_stream_change(client: Any, args: argparse.Namespace) -> int:
    change = build_change(
        args.ids,
        read_delay_ms=args.read_delay,
        stalled=args.stalled,
        buffer_size=args.buffer,
        reconnect=args.reconnect,
    )
    await client.changeStreamSubscribers(change)
    print(f"changed subscribers: {', '.join(map(str, args.ids)) or 'all'}")
    return 0


async def cmd_stream_stop(client: Any, args: argparse.Namespace) -> int:
    await client.stopStreamClients()
    print("stream clients stopped")
    return 0


async def cmd_stream_stats(client: Any, args: argparse.Namespace) -> int:
    await _print_report(client, args.counters)
    return 0


async def _wait_subscribed(client: Any, timeout_s: float) -> bool:
    """Waits until every subscriber holds a subscription or stopped."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while True:
        stats = await client.getStreamClientsStats()
        if all(
            s.subscriptions > 0 or s.lastEndReason.value != 0 for s in stats.subscribers
        ):
            return True
        if loop.time() >= deadline:
            return False
        await asyncio.sleep(1)


def _subscribers(
    args: argparse.Namespace, count: int, **fields: Any
) -> StreamClientsConfig:
    group = StreamSubscriberGroup(
        count=count,
        keyPrefixes=list(args.key_prefixes),
        areas=set(args.areas),
        senderId="scale-{id}",
        **fields,
    )
    pollers = (
        [
            StreamPollerGroup(
                count=args.pollers,
                keyPrefixes=list(args.key_prefixes),
                senderId="scale-poll-{id}",
            )
        ]
        if args.pollers
        else []
    )
    # No --dut-host: the DUT of the active session.
    dut = (
        DutConnection(host=args.dut_host, port=args.dut_port)
        if args.dut_host
        else DutConnection()
    )
    return StreamClientsConfig(subscribers=[group], pollers=pollers, dut=dut)


async def _start(
    client: Any, args: argparse.Namespace, config: StreamClientsConfig
) -> None:
    await client.startStreamClients(config)
    if not await _wait_subscribed(client, args.settle):
        print(f"warning: not all subscribers subscribed within {args.settle}s")
    await _print_report(client, args.counters, "started")


async def _scenario_steady(client: Any, args: argparse.Namespace) -> None:
    await _start(client, args, _subscribers(args, args.subscribers or 4))
    await asyncio.sleep(args.duration)
    await _print_report(client, args.counters, f"after {args.duration}s")


async def _scenario_restart(client: Any, args: argparse.Namespace) -> None:
    await _start(client, args, _subscribers(args, args.subscribers or 4))
    if args.restart_cmd:
        subprocess.run(shlex.split(args.restart_cmd), check=True)
    else:
        print(f"Restart Open/R on the DUT within {args.duration}s.")
    await asyncio.sleep(args.duration)
    await _print_report(client, args.counters, "after the Open/R restart")


async def _scenario_cold_start(client: Any, args: argparse.Namespace) -> None:
    # The clients start before Open/R serves, as clients that start on a node.
    await client.startStreamClients(_subscribers(args, args.subscribers or 4))
    await asyncio.sleep(args.settle)
    await _print_report(client, args.counters, "started before Open/R")
    if args.start_cmd:
        subprocess.run(shlex.split(args.start_cmd), check=True)
    else:
        print(f"Start Open/R on the DUT within {args.duration}s.")
    await asyncio.sleep(args.duration)
    await _print_report(client, args.counters, "after the Open/R start")


async def _scenario_churn(client: Any, args: argparse.Namespace) -> None:
    if not args.links:
        raise ValueError("churn needs --links localNode:remoteNode ...")
    links = [
        LinkRef(localNode=a, remoteNode=b)
        for a, b in (t.split(":", 1) for t in args.links)
    ]
    await _start(client, args, _subscribers(args, args.subscribers or 4))
    await client.flapLinks(links, args.cycles, args.interval)
    await asyncio.sleep(args.duration)
    await _print_report(client, args.counters, f"after {args.cycles} flap cycles")


async def _scenario_stalled(client: Any, args: argparse.Namespace) -> None:
    await _start(client, args, _subscribers(args, args.subscribers or 4))
    await client.changeStreamSubscribers(build_change([0], stalled=True))
    await asyncio.sleep(args.duration)
    await _print_report(client, args.counters, f"subscriber 0 stalled {args.duration}s")
    # The stalled subscriber reads its end reason after the queued messages.
    await client.changeStreamSubscribers(build_change([0], stalled=False))
    await asyncio.sleep(args.settle)
    await _print_report(client, args.counters, "subscriber 0 resumed")


async def _scenario_cap(client: Any, args: argparse.Namespace) -> None:
    count = args.subscribers or DUT_MAX_SUBSCRIBERS + 4
    await _start(client, args, _subscribers(args, count, reconnectDelayMs=-1))


async def _scenario_gate(client: Any, args: argparse.Namespace) -> None:
    # No buffer and no reads: each admitted subscriber keeps its snapshot slot.
    count = args.subscribers or DUT_MAX_SNAPSHOTS_IN_FLIGHT + 4
    config = _subscribers(args, count, stalled=True, bufferSize=0, reconnectDelayMs=-1)
    await _start(client, args, config)
    await client.changeStreamSubscribers(build_change([], stalled=False))
    await asyncio.sleep(args.settle)
    await _print_report(client, args.counters, "all subscribers resumed")


Scenario = Callable[[Any, argparse.Namespace], Awaitable[None]]


def get_scenarios() -> dict[str, tuple[Scenario, str]]:
    return {
        "steady": (
            _scenario_steady,
            "subscribers and pollers in steady state for --duration",
        ),
        "restart": (
            _scenario_restart,
            "subscribers across an Open/R restart (--restart-cmd or by hand)",
        ),
        "cold-start": (
            _scenario_cold_start,
            "subscribers that start before Open/R (--start-cmd or by hand)",
        ),
        "churn": (
            _scenario_churn,
            "subscribers during link flaps of --links (--cycles, --interval); "
            "needs a session",
        ),
        "stalled": (
            _scenario_stalled,
            "subscriber 0 stalls for --duration, then resumes",
        ),
        "cap": (
            _scenario_cap,
            f"more subscribers than the cap ({DUT_MAX_SUBSCRIBERS}), no retry",
        ),
        "gate": (
            _scenario_gate,
            f"more held snapshots than the gate ({DUT_MAX_SNAPSHOTS_IN_FLIGHT}), "
            "then resume",
        ),
    }


async def run_scenario(client: Any, args: argparse.Namespace) -> None:
    scenario, _ = get_scenarios()[args.scenario]
    try:
        await scenario(client, args)
    finally:
        if not args.keep and (await client.getStreamClientsStats()).running:
            await client.stopStreamClients()


async def cmd_stream_scenario(client: Any, args: argparse.Namespace) -> int:
    await run_scenario(client, args)
    return 0


def stream_handlers() -> dict[str, Handler]:
    return {
        "stream-start": cmd_stream_start,
        "stream-change": cmd_stream_change,
        "stream-stop": cmd_stream_stop,
        "stream-stats": cmd_stream_stats,
        "stream-scenario": cmd_stream_scenario,
    }


def add_stream_parsers(sub: Any) -> None:
    p_start = sub.add_parser(
        "stream-start", help="Start KvStore stream clients from a YAML config"
    )
    p_start.add_argument("--config", required=True, help="Path to YAML config file")

    p_change = sub.add_parser("stream-change", help="Change stream subscribers")
    p_change.add_argument(
        "--ids", type=int, nargs="*", default=[], help="Subscriber ids (default: all)"
    )
    p_change.add_argument("--read-delay", type=int, help="Delay per message (ms)")
    p_change.add_argument("--buffer", type=int, help="Buffer size, next subscription")
    stall = p_change.add_mutually_exclusive_group()
    stall.add_argument(
        "--stall",
        dest="stalled",
        action="store_const",
        const=True,
        help="Stop reading: the client grants no credit",
    )
    stall.add_argument(
        "--resume",
        dest="stalled",
        action="store_const",
        const=False,
        help="Read again",
    )
    p_change.add_argument(
        "--reconnect", action="store_true", help="Cancel the stream, subscribe again"
    )

    sub.add_parser("stream-stop", help="Stop the stream clients")

    for name, help_text in (
        ("stream-stats", "Print stream client stats and DUT counters"),
        ("stream-scenario", "Run a stream scenario"),
    ):
        p = sub.add_parser(name, help=help_text)
        p.add_argument(
            "--counters",
            default=DEFAULT_COUNTER_REGEX,
            help=f"DUT counter regex (default: {DEFAULT_COUNTER_REGEX})",
        )
        if name == "stream-scenario":
            _add_scenario_arguments(p)


def _add_scenario_arguments(p: argparse.ArgumentParser) -> None:
    scenarios = get_scenarios()
    p.add_argument(
        "scenario",
        choices=sorted(scenarios),
        help="; ".join(
            f"{name}: {text}" for name, (_, text) in sorted(scenarios.items())
        ),
    )
    p.add_argument("--subscribers", type=int, help="Subscriber count")
    p.add_argument("--pollers", type=int, default=0, help="Poller count (default: 0)")
    p.add_argument(
        "--key-prefixes", nargs="*", default=[], help="Key prefixes (default: all)"
    )
    p.add_argument("--areas", nargs="*", default=["0"], help="Areas (default: 0)")
    p.add_argument(
        "--duration", type=float, default=60, help="Load phase (s, default: 60)"
    )
    p.add_argument(
        "--settle", type=float, default=10, help="Settle wait (s, default: 10)"
    )
    p.add_argument(
        "--dut-host", help="DUT of the stream clients (default: the session DUT)"
    )
    p.add_argument(
        "--dut-port", type=int, default=2018, help="DUT control port (default: 2018)"
    )
    p.add_argument("--restart-cmd", help="restart: command that restarts Open/R")
    p.add_argument("--start-cmd", help="cold-start: command that starts Open/R")
    p.add_argument("--links", nargs="*", default=[], help="churn: localNode:remoteNode")
    p.add_argument("--cycles", type=int, default=5, help="churn: flap cycles")
    p.add_argument(
        "--interval", type=int, default=1000, help="churn: flap interval (ms)"
    )
    p.add_argument(
        "--keep", action="store_true", help="Leave the stream clients running"
    )
