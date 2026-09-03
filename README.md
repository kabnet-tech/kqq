# kqq — K-Quick-Query

**SQL for streams.** Query NDJSON, JSON, CSV, and LLM token streams with SQL —
at memory speeds, not disk speeds.

```bash
cat logs.ndjson | kqq 'select service, count(), avg(latency_ms) group by service'
```

## jq vs kqq

Same query — group 1M rows by a field and compute `count()` + `avg()` — on the
same machine:

| Metric | jq | kqq | |
|---|---|---|---|
| Time | ~8 s | ~0.5 s | **16× faster** |
| Peak memory | ~1 GB | ~7 MB | **150× less** |
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

Static, zero-dependency binaries for Linux (x86_64, aarch64, ARM, RISC-V,
POWER), macOS (Intel, Apple Silicon), Windows, and FreeBSD are on
[GitHub Releases](https://github.com/kabnet-tech/kqq/releases).

```bash
# Linux x86_64 — v0.9.0
curl -LO https://github.com/kabnet-tech/kqq/releases/download/v0.9.0/kqq-0.9.0-linux-x86_64.tar.gz
tar xzf kqq-0.9.0-linux-x86_64.tar.gz && sudo mv kqq /usr/local/bin/
kqq --version
```

<details>
<summary>Other platforms</summary>

```bash
# macOS Apple Silicon
curl -LO https://github.com/kabnet-tech/kqq/releases/download/v0.9.0/kqq-0.9.0-macos-aarch64.tar.gz
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

## Query Language

```sql
select dept, count(), avg(salary) as avg_sal
where active = true
group by dept
having count() > 2
order by avg_sal desc
limit 10
```

- **Operators**: `= != > < >= <= like contains starts_with ends_with in
  not in is null is not null has() matches` (regex), `between [lo and hi]`
- **Functions**: `upper lower len trim concat substr replace lpad rpad split
  round floor ceil abs to_number format coalesce isnull ifhas type_of keys
  values to_entries date_part epoch_*` and more
- **Expressions**: arithmetic (`salary * 0.1`), `case when ... then ... end`,
  `--arg name value` variable injection
- **Clauses**: `select` (with `as` aliases, `* add`, `* remove`, `distinct`,
  `expand`), `where`, `group by`, `having`, `order by` (aliases and ordinals),
  `limit`, `into '<file>'`

## Input / Output

- **Input**: NDJSON (default), JSON arrays/objects, CSV, TSV (`--delim`, `--header`)
- **Output**: JSON (default), NDJSON (`--ndjson`), CSV (`--csv`), TSV (`--tsv`), raw (`--raw`)
- **Streaming**: aggregate queries emit once at end of stream (SQL semantics);
  non-aggregate queries emit per record with true early-stop on `limit`

## Performance

Single-pass streaming aggregation with O(1) memory per group. `limit N`
without `order by` stops reading input the moment N matches are found —
"first 5 errors in a 2 GB log" returns in milliseconds, not minutes.

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