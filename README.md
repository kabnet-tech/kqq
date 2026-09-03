# K-Quick-Query (kqq) — SQL for Streams

[![CI](https://github.com/kabnet-tech/kqq/actions/workflows/ci.yml/badge.svg)](https://github.com/kabnet-tech/kqq/actions/workflows/ci.yml)
[![Release](https://github.com/kabnet-tech/kqq/actions/workflows/release.yml/badge.svg)](https://github.com/kabnet-tech/kqq/actions/workflows/release.yml)
[![GitHub Release](https://img.shields.io/github/v/release/kabnet-tech/kqq?logo=github)](https://github.com/kabnet-tech/kqq/releases)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
![Platform](https://img.shields.io/badge/platform-linux%20%7C%20macOS%20%7C%20Windows%20%7C%20FreeBSD-lightgrey)
![Zig](https://img.shields.io/badge/zig-0.16.0-orange)

**SQL for streams.** Query NDJSON, JSON, CSV, and LLM token streams with SQL —
at memory speeds, not disk speeds.

```bash
cat logs.ndjson | kqq 'select service, count(), avg(latency_ms) group by service'
```

## jq vs kqq

Same query — group 1M rows by a field and compute `count()` + `avg()` — on the
same machine. **Run it yourself:**

```bash
# 1. Generate 1M rows
python3 -c "import json,random; random.seed(42); [print(json.dumps({'service': f'svc-{i%50}', 'latency_ms': random.randint(1,100)})) for i in range(1000000)]" > /tmp/bench.ndjson

# 2. Time kqq
time kqq 'select service, count(), avg(latency_ms) group by service' < /tmp/bench.ndjson > /dev/null

# 3. Time jq
time jq -s 'group_by(.service) | map({service: .[0].service, count: length, avg: (map(.latency_ms) | add / length)})' < /tmp/bench.ndjson > /dev/null
```

Measured on this repo's dev machine (jq 1.8.2, kqq 0.9.0 ReleaseFast):

| Metric | jq | kqq | |
|---|---|---|---|
| Time | ~7.7 s | ~0.27 s | **~28× faster** |
| Peak memory | ~690 MB | ~1 MB | **~700× less** |
| Input size | buffers everything | streams, O(1) per group | |

| Problem | jq | kqq |
|---|---|---|
| "First 5 errors in a 2 GB log" | scans everything | early-stop in milliseconds |
| CSV export | `@csv` ceremony | `--csv` flag |
| Learning curve | functional language | SQL |

kqq is **not** a jq replacement. It's purpose-built for the 80% of queries that
are filter → project → aggregate → export. For recursive descent, custom
functions, or Turing-complete transforms, use jq.

kqq is a single static binary that filters, projects, and aggregates streaming
data in one pass. It never buffers the full input, so it crunches files far
larger than RAM with O(1) memory per group. If you can write a `WHERE` clause,
you already know kqq.

## Install

### Download a binary (recommended)

**One-line install** (Linux, macOS, FreeBSD — detects your platform, verifies
the SHA256 checksum, installs to `/usr/local/bin` or `~/.local/bin`):

```bash
curl -fsSL https://raw.githubusercontent.com/kabnet-tech/kqq/main/scripts/install.sh | sh
```

Or install manually — static, zero-dependency binaries for Linux (x86_64,
aarch64, ARM, RISC-V, POWER), macOS (Intel, Apple Silicon), Windows, and
FreeBSD are on [GitHub Releases](https://github.com/kabnet-tech/kqq/releases):

```bash
# Linux x86_64 — v0.9.0
curl -fLO https://github.com/kabnet-tech/kqq/releases/download/v0.9.0/kqq-0.9.0-linux-x86_64.tar.gz
tar xzf kqq-0.9.0-linux-x86_64.tar.gz && sudo mv kqq /usr/local/bin/
kqq --version
```

<details>
<summary>Other platforms</summary>

```bash
# macOS Apple Silicon
curl -fLO https://github.com/kabnet-tech/kqq/releases/download/v0.9.0/kqq-0.9.0-macos-aarch64.tar.gz
tar xzf kqq-0.9.0-macos-aarch64.tar.gz && sudo mv kqq /usr/local/bin/

# Windows (PowerShell) — x86_64
# Download kqq-0.9.0-windows-x86_64.zip from Releases and add kqq.exe to PATH
```

See the [release page](https://github.com/kabnet-tech/kqq/releases) for all 15
platforms, including FreeBSD and glibc/musl Linux variants. SHA256 checksums
are published with every release.
</details>

### Build from source (requires Zig 0.16.0)

```bash
git clone https://github.com/kabnet-tech/kqq
cd kqq
zig build -Doptimize=ReleaseFast
cp zig-out/bin/kqq ~/.local/bin/    # or anywhere on $PATH
```

## Quick Start

```bash
# Filter + project
cat logs.ndjson | kqq 'select name, score where active = true order by score desc limit 10'

# Aggregation — single pass, O(1) memory per group
cat logs.ndjson | kqq 'select service, count(), avg(duration_ms) group by service order by count desc'

# Scoped queries into nested arrays
cat data.json | kqq '[users.*] select email where role = "admin"'

# CSV export
cat data.ndjson | kqq 'select dept, count() group by dept' --csv > report.csv

# Split routing: matches to stdout, non-matches to a file, one pass
cat events.ndjson | kqq 'where level = "ERROR"' --reject clean.ndjson > errors.ndjson

# LLM stream processing (Ollama / OpenAI / Anthropic)
curl -s localhost:11434/api/generate -d '{...}' | kqq --api ollama 'select name, price where price < 100' --csv
```

## kqq + DuckDB

kqq is the gate, DuckDB is the brain: kqq streams and shrinks the input with
O(1) memory, DuckDB does the heavy relational analytics on what's left.

```bash
# Pre-filter a huge stream, then analyze — DuckDB never sees the 99.9% you don't want
cat 2M-rows.ndjson | kqq 'select service, latency_ms where status = "error"' --ndjson \
  | duckdb -c "SELECT service, avg(latency_ms) FROM read_json_auto('/dev/stdin') GROUP BY service;"
```

Filter to NDJSON, aggregate to CSV, or pipe straight through `/dev/stdin` —
six verified patterns in **[docs/duckdb.md](docs/duckdb.md)**.

## Documentation

| Doc | Contents |
|---|---|
| [docs/KQQ-SQL-REFERENCE.md](docs/KQQ-SQL-REFERENCE.md) | Every operator, function, clause, and CLI flag — with verified examples |
| [docs/duckdb.md](docs/duckdb.md) | kqq → DuckDB integration patterns |
| [docs/limits.md](docs/limits.md) | What kqq deliberately does not do (not Turing-complete) |

## Development

```bash
zig build                        # debug build
zig build -Doptimize=ReleaseFast # release build
zig build test --summary all     # unit + integration tests
python3 tests/cli_tests.py       # end-to-end CLI tests
```

Requires Zig 0.16.0. No other dependencies.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
