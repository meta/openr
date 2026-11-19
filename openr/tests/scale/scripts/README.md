# OpenR Scale Test — Setup Scripts

Scripts for preparing a test server and an Arista EOS DUT to run the OpenR
scale test (`ScaleTestServer`). Both ends need matching VLAN sub-interfaces
and ip6tables rules that allow per-neighbor FakeKvStore Thrift traffic.

## Scripts

| Script              | Side        | Purpose                                                                |
| ------------------- | ----------- | ---------------------------------------------------------------------- |
| `setup_vlans.sh`     | Test server | Creates VLAN sub-interfaces, assigns IPs (`.1`), opens ip6tables.      |
| `setup_vlans_dut.sh` | EOS DUT     | Creates matching VLAN sub-interfaces, assigns IPs (`.2`), configures EOS via FastCli, opens ip6tables. |

Both must be run as root. Both are idempotent — re-running won't stack
duplicate ip6tables rules.

## Quick Start (4 VLANs)

On the test server:

```bash
sudo ./setup_vlans.sh eth0 4
# Creates eth0.1..eth0.4, IPv4 10.0.1.1/24..10.0.4.1/24, IPv6 fd00:0:0:1::1/64..
```

On the DUT (EOS — use the Linux interface name, e.g. `et4_15_1` for `Ethernet4/15/1`):

```bash
sudo ./setup_vlans_dut.sh et4_15_1 4
# Creates matching et4_15_1.1..et4_15_1.4, IPs end in .2
```

The setup_vlans.sh script prints an `--interfaces=` line you can paste into
`scale_test_server`:

```
Use with scale_test_server:
  --interfaces=eth0.1,eth0.2,eth0.3,eth0.4
```

## Larger Scale (e.g. 100 VLANs starting at ID 100)

```bash
sudo ./setup_vlans.sh     eth0 100 100   # server
sudo ./setup_vlans_dut.sh et4_15_1 100 100  # DUT
```

## Overriding ip6tables Port Ranges

Both scripts open ip6tables for the FakeKvStore Thrift servers spawned by
`ScaleTestServer` (one per simulated neighbor, starting at
`--fake_kvstore_base_port`, default `3000`).

Defaults:
- `INPUT_PORT_RANGE=3000:3500` (inbound to FakeKvStore servers)
- `OUTPUT_PORT_RANGE=3000:3100` (outbound replies)

Override when running with more neighbors or a different base port:

```bash
INPUT_PORT_RANGE=3000:3999 OUTPUT_PORT_RANGE=3000:3500 \
    sudo ./setup_vlans.sh eth0 800
```

## Cleanup

Each script prints a one-liner at the end to tear down the VLANs it created,
e.g.:

```bash
for i in $(seq 1 4); do sudo ip link delete eth0.$i; done
```

To remove the ip6tables rules:

```bash
sudo ip6tables -D INPUT  -p tcp --dport 3000:3500 -j ACCEPT
sudo ip6tables -D OUTPUT -p tcp --sport 3000:3100 -j ACCEPT
```

## KvStore Stream Clients

`scaletest stream-*` commands run KvStore stream test clients in
`scale_test_server_d`. Each subscriber calls `subscribeAndGetAreaKvStores` and
each poller calls `getKvStoreKeyValsFilteredArea`, on its own connection to the
DUT control port. The clients target the DUT in their config, or the DUT of the
active session when the config names none. They run with or without a session,
next to the session churn (node and link flaps) when one runs. They need no
running Open/R: a failed connection counts as a CONNECTION end, and the client
tries again after its reconnect delay.

```bash
scaletest stream-start --config stream.yaml
scaletest stream-change --ids 0 --stall
scaletest stream-stats
scaletest stream-stop
```

Example `stream.yaml`. Absent fields keep the defaults of
`if/ScaleTestServer.thrift`:

```yaml
dut:                         # optional; default: the session DUT
  host: "::1"
  port: 2018
subscribers:
  - count: 4
    keyPrefixes: ["adj:", "prefix:"]
    areas: ["0"]
    senderId: "scale-{id}"   # {id} becomes the subscriber id
    readDelayMs: 0           # delay per message
    bufferSize: 100          # credits per subscription; 0 = none until the first read
    reconnectDelayMs: 2000   # negative = wait for stream-change --reconnect
pollers:
  - count: 2
    keyPrefixes: ["adj:"]
    area: "0"
    intervalMs: 1000
    senderId: "poll-{id}"
```

`stream-stats` prints per subscriber the state, subscriptions, reconnects,
snapshot bytes, keys and latency, messages, bytes and keys received, and the
stream end reasons (LOADSHEDDING, OVERFLOW, BUDGET, CREDIT_TIMEOUT, SHUTDOWN,
CONNECTION, COMPLETED, CLIENT_CANCEL). It also prints the DUT counters that
match `--counters` (default `^subscribers\.kvstore`).

`stream-scenario <name>` starts the clients, runs one test, prints the stats
and DUT counters after each phase, and stops the clients. `--dut-host` and
`--dut-port` name the DUT; without them, the scenario uses the session DUT:

| Scenario  | Test                                                                  |
| --------- | --------------------------------------------------------------------- |
| `steady`  | Subscribers and pollers for `--duration`.                             |
| `restart` | Subscribers across an Open/R restart (`--restart-cmd` or by hand).    |
| `cold-start` | Subscribers that start before Open/R (`--start-cmd` or by hand).   |
| `churn`   | Subscribers during flaps of `--links` (`--cycles`, `--interval`). Needs a session. |
| `stalled` | Subscriber 0 stalls for `--duration`, then resumes.                   |
| `cap`     | 20 subscribers against the subscriber cap of 16, with no retry.       |
| `gate`    | 8 stalled subscribers with no buffer hold their snapshot slots against the gate of 4, then resume. |
