# Kalshi order book collector

This repository contains a Rust service that records the order books of live, non-multivariate Kalshi markets. The service discovers markets over REST, spreads their subscriptions across WebSocket connections, maintains one book per market, and writes every book change in FIX SBE. It also writes a full snapshot of each live book about once a minute, so that a consumer can recover state without replaying the whole file. The sections below describe the data path, the rules that decide when a book is trusted, the reasoning behind the main design choices, and the results obtained against production Kalshi. Instructions for building and running the service are in [CONTRIBUTIONS.md](CONTRIBUTIONS.md).

## Data path

![Data flow from a market provider through the provider contracts, tracker, socket manager, collectors, failover and queue to the SBE writer](docs/diagrams/architecture.svg)

`MarketTracker` pages through Kalshi's market list and passes each page on as it arrives, because a full scan takes several minutes. Pages are requested gzip-compressed, which shrinks a page of 1,000 markets from about 2.1 MB to about 130 KB. Once the scan ends, it polls every 30 seconds for markets that opened, closed or settled, and repeats the full scan hourly. It also tracks listed markets that have not opened, and follows each at its opening time. `SocketManager` places each market on the least-loaded socket and opens another socket when every existing one is full. A feed never opens more than its share of the provider's connection limit. Beyond that limit, sockets exceed their target size, because coverage takes priority over an even spread. A socket task that panics is restarted with its markets at the next poll, or at the next page of the first scan.

Each `KalshiStream` owns one signed WebSocket connection. It reconnects after a failure, with a delay that grows while connections keep failing, and replaces a healthy connection every six hours. Incoming frames pass to `Normaliser`, which checks sequence numbers and converts Kalshi's format into exchange-neutral events. Kalshi quotes YES and NO bids, so a NO bid at price *p* is recorded as a sell at 1 − *p*.

`Collector` applies those events to the books held for its socket. Its output passes through a bounded queue to a single writer thread, which encodes records with the supplied `AlignedSbeWriter`. On SIGINT or SIGTERM the service closes its sockets, drains the queue and finishes the file.

## Redundancy

Every market runs on two independent feeds by default, each with its own sockets and its own book, so a lost socket does not leave a market without a live copy. `Failover` sits between the feeds and the output queue and merges them. Kalshi's exchange clock is the same on every connection, so it identifies each change. The copy delivered first is recorded and later copies are discarded, so a stalled connection loses no change while another feed keeps up. Changes that share a clock are told apart by their order on the feed, and a snapshot taken between them does not disturb that order.

One feed owns each market and supplies its minute snapshots. When an owner's connection ends or its book loses continuity, it gives up the market. The next feed to produce an event takes over by recording a full snapshot of its book, followed by its deltas, so a consumer never receives deltas against a book it has not seen. A feed behind the record waits up to five seconds to catch up before taking over, because an earlier book would move a consumer back in time. A standby's snapshot is recorded only when it is ahead of the record, or level with it while the owner is behind. In either case, the standby becomes the owner. A clock that moves backwards, which happens only during exchange maintenance, restarts the market from a snapshot. `--feeds` sets the number of copies, and each copy adds its own sockets and traffic.

## Adding a provider

`src/provider.rs` holds the contracts that separate the core from any exchange. `MarketDirectory` lists markets and what changed, `MarketStream` runs one connection that feeds books, and `FeedHandler` receives what a stream produces. `SocketManager`, `MarketTracker`, `Collector`, `Failover`, `FeedRouter` and the output depend only on these and on exchange-neutral domain types. A new provider implements `MarketDirectory` and `MarketStream` in its own module and is chosen in `app.rs`. `tests/provider.rs` runs a fake provider through the real tracker and manager to test that boundary.

## When a book is trusted

A market starts in the awaiting-snapshot state, in which nothing is published. The first snapshot makes its book live, and only a live book produces deltas and minute snapshots. Removing a market retires it, so that frames already in flight are not mistaken for corruption.

A live book returns to awaiting a snapshot if a sequence gap appears, a frame cannot be read, a delta contradicts the book, or the book stays crossed for more than five seconds. The collector then requests a replacement snapshot. `ResyncWatch` decides when replacements have stopped arriving for every market on a socket. The connection is then rebuilt, since sequence numbers carry meaning only within one connection. A single overdue market is retried without affecting the other markets on its socket. The output format has no gap record, so a consumer sees a pause for that market followed by a fresh snapshot.

## Design decisions

The collector for each socket owns that socket's books. A lost message can only affect the books on its own connection, so recovery requires no state shared between sockets.

Publishing stops as soon as continuity is uncertain. This accepts a pause in the record to avoid recording a book that may be wrong. The choice follows from the purpose of the data, which is later analysis of what the market showed.

Markets are removed when Kalshi lists them as closed or settled. An hourly full scan catches anything the polls missed. A scan may remove a market only if the scan is complete and of plausible size and the market was placed before the scan began. Neither a failed scan nor an exchange fault can therefore be mistaken for markets disappearing.

The output queue blocks when full, so records are never discarded silently. The cost is that a stalled writer delays socket reads and therefore receipt times. Receipt time is taken once per frame before parsing, and each record carries both that time and the exchange time. Kalshi documents its timestamps as optional, so a delta without one uses the message's sending time, and the local time if that is missing too. Each socket starts its minute snapshots at a random offset, so that sockets do not all pause reading together. Order is guaranteed within a market and is not guaranteed across markets. A record is queued while the failover lock is held, which preserves that order, so a full queue also delays the other feeds.

The supplied reader accepts only 64 MiB chunks, so the writer emits whole chunks. A consumer of stdout therefore receives records a chunk at a time, not as each change happens, and the final partial chunk arrives at shutdown. Receipt times are taken before this buffering, so the delay does not affect what is recorded.

The REST client refuses redirects and insecure URLs, because a redirect would carry signed headers to another host.

## Usage

The program needs Rust and a Kalshi API key. Copy the example environment file, then set the key ID, private key path and the Kalshi URLs in `.env`:

```bash
cp .env.example .env
```

Install the program, which puts `engineering-challenge` on the PATH through `~/.cargo/bin`, then confirm that Kalshi accepts the key:

```bash
make install
```

```bash
engineering-challenge check
```

`make help` lists the Make targets and the program's help. Each command also has its own help:

```bash
engineering-challenge --help
```

```bash
engineering-challenge collect --help
```

### Recording

`collect` records all open markets until stopped with Ctrl-C, SIGINT or SIGTERM. It then closes its sockets, drains its queue and finishes the file.

Record to a file:

```bash
engineering-challenge collect --output out.sbe
```

Record to stdout, the default, with logs kept separate on stderr:

```bash
engineering-challenge collect > out.sbe 2> collect.log
```

Try a small recording first:

```bash
engineering-challenge collect --max-markets 100 --output small.sbe
```

Run a single feed, without redundancy, to halve sockets and traffic:

```bash
engineering-challenge collect --feeds 1 --output out.sbe
```

Use smaller sockets, so that each lost connection affects fewer markets:

```bash
engineering-challenge collect --markets-per-socket 1000 --output out.sbe
```

### Inspecting

`verify` decodes every record in a file and reports the number of snapshots, updates and distinct markets. It needs no credentials:

```bash
engineering-challenge verify out.sbe
```

`discover` scans the market list and reports how many markets are open, with a sample:

```bash
engineering-challenge discover
```

`watch` follows one market and logs its best bid and ask. Replace the ticker with a real one from `discover`:

```bash
engineering-challenge watch MARKET-TICKER
```

Set `RUST_LOG` for more detail, such as REST page timings:

```bash
RUST_LOG=info,engineering_challenge::kalshi::rest=debug engineering-challenge discover
```

### Make targets

`make smoke` is the quickest end-to-end check. It records 100 markets for a minute and decodes the result:

```bash
make smoke
```

Run every local check, the same as CI:

```bash
make check
```

Repeat the whole evaluation, which takes about an hour, then print its tables:

```bash
make experiments
```

```bash
make summary
```

The experiments can also run one at a time with `make baseline`, `make socket-size`, `make backlog` and `make link`. The last two use macOS tools. Logs go to `results/`, and `make clean-results` deletes them.

## Why a command-line program

The collector is a long-running process whose product is a recording. The challenge asks for output on stdout and for shutdown on SIGINT, and both are native to a command-line program, which also composes with pipes, process supervisors and containers without further code. An HTTP API would suit a system that answers requests, but nothing here queries the collector while it runs. Adding one would require a server, an authentication surface and a second way of stopping the process, and no consumer needs them. The subcommands call `App`, so an API could later be built on the same core if a consumer requires one.

## Module guide

| Module | Responsibility |
|---|---|
| `provider.rs` | The contracts every provider implements |
| `discovery.rs` | Which markets to follow |
| `manager.rs` | Which socket holds each market |
| `resync.rs` | What to do about unanswered snapshot requests |
| `router.rs` | One feed's route to the writer |
| `kalshi/rest.rs`, `kalshi/auth.rs`, `kalshi/directory.rs` | HTTP, retries, Ed25519 signing and the market list |
| `kalshi/stream.rs`, `kalshi/session.rs` | The life and timers of a connection, and its state |
| `kalshi/wire.rs`, `kalshi/normalise.rs` | Kalshi's message format |
| `domain.rs`, `collector.rs` | Exchange-neutral book rules |
| `failover.rs` | Which feed is recorded for each market |
| `output.rs` | SBE mapping and the single writer |
| `transport.rs` | Backoff, jitter and URL safety |
| `app.rs`, `types.rs` | Wiring, configuration and shutdown |

## Evaluation

The evaluation asks whether the service records Kalshi books in a decodable form at scale, and what limits how many markets it can follow.

### Method

All runs used one Mac on a home connection against production Kalshi. The experiments ran as three sets. Sets A and B fetched market pages uncompressed, and set C requested them gzip-compressed. Set A ran on the afternoon of 6 November 2026. Set B ran overnight from 6 to 7 November, after a change to how the merge orders changes that share a clock. Set C ran on the evening of 7 November on the current code. The long run ran once, on 5 November, without compression. Other software shared the machine, and its five-minute load average stayed between about 4 and 10 during set A, about 5 and 24 during set B, and about 3 and 15 during set C. Each run started with no state and stopped on SIGINT. A baseline run scanned markets with no sockets open. Size runs varied markets per socket. A backlog run sampled throughput, unread socket bytes and CPU every 10 to 15 seconds. A link run measured the connection with `networkQuality` while idle and again during collection. A connection loss is any connection that ended and was replaced. A market is placed once it is handed to a socket. In set C every run finished a full scan, so its first table reports the minutes the first scan took instead of markets placed. All runs with sockets open used a single feed. Every experiment except the long run can be repeated with `make experiments`, and `make summary` prints these tables from the logs. [CONTRIBUTIONS.md](CONTRIBUTIONS.md) describes the targets.

### Results

**Table 1.** Markets placed and connection losses by run.

| Run | Duration (min) | Markets per socket | Set A placed | Set A losses | Set B placed | Set B losses | Set C full scan (min) | Set C losses |
|---|---|---|---|---|---|---|---|---|
| Long | 50 | 2,000 | 51,000 | 65 | not run | not run | not run | not run |
| Baseline | 5 | no sockets | 0 | 0 | 0 | 0 | 2.6 | 0 |
| Size | 15 | 2,000 | 100,000 | 23 | 58,000 | 11 | 5.4 | 107 |
| Size | 15 | 500 | 72,000 | 12 | 59,000 | 8 | 12.6 | 512 |
| Backlog | 22 | 1,000 | 141,000 | 29 | 80,000 | 21 | 6.8 | 185 |
| Link | 15 | 1,000 | 84,000 | 33 | 73,000 | 29 | 7.5 | 276 |

Each set C full scan covered about 187,000 markets.

**Table 2.** Connection losses by cause. Server closed includes connections that ended without a TLS close. A timeout is a send or handshake that passed its time limit.

| Run | Reset | Server closed | Snapshots unanswered | Silence | Timeout |
|---|---|---|---|---|---|
| Long | 62 | 2 | 1 | 0 | 0 |
| Set A, size 2,000 | 12 | 8 | 3 | 0 | 0 |
| Set A, size 500 | 12 | 0 | 0 | 0 | 0 |
| Set A, backlog | 11 | 16 | 2 | 0 | 0 |
| Set A, link | 15 | 3 | 0 | 15 | 0 |
| Set B, size 2,000 | 9 | 2 | 0 | 0 | 0 |
| Set B, size 500 | 7 | 1 | 0 | 0 | 0 |
| Set B, backlog | 19 | 2 | 0 | 0 | 0 |
| Set B, link | 20 | 2 | 1 | 6 | 0 |
| Set C, size 2,000 | 106 | 0 | 0 | 1 | 0 |
| Set C, size 500 | 292 | 35 | 0 | 75 | 109 |
| Set C, backlog | 180 | 4 | 1 | 0 | 0 |
| Set C, link | 96 | 8 | 0 | 106 | 66 |

One set C loss at 500 markets per socket, a connection with no announced subscription, fits none of these causes.

**Table 3.** Mean page time in seconds by position in the scan. In set C, pages 43 onwards include later scans.

| Set | Pages | No sockets | 2,000 per socket | 500 per socket |
|---|---|---|---|---|
| A | 1 to 20 | 8.3 | 7.7 | 4.0 |
| A | 21 to 42 | 7.3 | 4.9 | 9.8 |
| A | 43 onwards | no data | 10.7 | 19.9 |
| B | 1 to 20 | 9.2 | 13.9 | 7.9 |
| B | 21 to 42 | 7.7 | 14.7 | 14.1 |
| B | 43 onwards | no data | 18.2 | 24.6 |
| C | 1 to 20 | 1.2 | 0.9 | 0.6 |
| C | 21 to 42 | 0.8 | 1.4 | 0.9 |
| C | 43 onwards | 0.8 | 1.4 | 5.1 |

**Table 4.** Link measurements with `networkQuality`.

| Measure | Set A idle | Set A collecting | Set B idle | Set B collecting | Set C idle | Set C collecting |
|---|---|---|---|---|---|---|
| Download (Mbit/s) | 33.0 | 26.7 | 28.0 | 27.7 | 26.9 | 28.0 |
| Download responsiveness (ms) | 264 | 644 | 297 | 484 | 261 | 646 |
| Upload responsiveness (ms) | 1,043 | 2,466 | 1,366 | 1,650 | 1,888 | 644 |
| Idle latency (ms) | 29 | 71 | 27 | 39 | 18 | 51 |

The long run recorded 960 MiB, including 1,593,029 snapshots, all decoded by the supplied reader, with memory at or below 140 MB. In the backlog runs, CPU averaged 14.7%, 11.9% and 23.6% of one core in sets A, B and C, with peaks of 30.5%, 19.7% and 36.4%. Unread socket bytes were zero in 82, 87 and 79 of 89 samples. Traffic reached 40 Mbit/s in set A and 21 Mbit/s in set B. The set C byte counter exceeded the measured link capacity, so its traffic figure is not reported. In the set A and B link runs, all silence losses fell within 40 seconds, around the end of the speed test. In the set C link run, they were spread over the six minutes before it. One REST request was retried across the timed runs, in the set A link run. Every run exited cleanly.

### Discussion

Compressed pages changed coverage more than any other factor. Without compression, no run with sockets open completed a scan, and the furthest reached 141,000 markets. With compression, every run completed a scan of about 187,000 markets, in 5.4 to 12.6 minutes with sockets open and 2.6 minutes without. Page times fell by a factor of about 5 to 15. The service recorded data that the supplied reader decoded, and in set C it held every market on the list at once.

Following every market raised connection losses from 8 to 33 per run without compression to 107 to 512 with it. Set C carried more than twice as many markets as sets A and B, so more traffic and more sockets were exposed to each fault. Silence and timeout losses, which were rare before, made up most losses in the set C link run and over a third at 500 markets per socket. Those runs also had the slowest later pages. This pattern is consistent with a saturated link, and the silence losses at the end of the set A and B speed tests show that a saturated link can stall sockets. The run at 500 markets per socket had fewer losses than the run at 2,000 in sets A and B but nearly five times as many in set C, so socket size cannot be credited with either result. The same backlog configuration placed 141,000 markets in set A and 80,000 in set B, so a single run does not give a typical scan rate. The mostly empty backlog suggests that socket reads keep up, even at full coverage.

These results come from a single machine and network, and most configurations ran once per set. The two uncompressed runs at 2,000 markets per socket differed in scan speed by a factor of more than six, so small differences between runs should not be interpreted. Market count, page depth and elapsed time rose together and were not separated. A repeat from a well-connected machine is the next step.

The automated tests in `tests/` cover book rules, recovery, failover, routing, the provider contracts, normalising, discovery and output. One test rebuilds a book from the record alone across a failover and compares it with the live feed's book.
