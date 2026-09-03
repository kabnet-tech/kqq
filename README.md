# kq

**SQL for streams.** A fast, single-binary query tool for NDJSON and JSON.

```bash
cat logs.ndjson | kq 'select service, count(), avg(latency_ms) group by service'
```

kq filters, projects, aggregates, and exports newline-delimited JSON using SQL
syntax you already know. It streams — never buffering the full input — so it
aggregates files far larger than available RAM with O(1) memory per group.

## Why kq?

| Problem | jq | kq |
|---|---|---|
| Group 1M rows by a field | slurps all of RAM (~1 GB) | streaming, ~7 MB |
| "First 5 errors in a 2 GB log" | scans everything | early-stop in milliseconds |
| CSV export | `@csv` ceremony | `--csv` flag |
| Learning curve | functional language | SQL |

kq is **not** a jq replacement. It's purpose-built for the 80% of queries that
are filter → project → aggregate → export. For recursive descent, custom
functions, or Turing-complete transforms, use jq.

## Install

### Build from source (requires Zig 0.16.0)

```bash
git clone https://github.com/kck8/kq
cd kq
zig build -Doptimize=ReleaseFast
cp zig-out/bin/kq ~/.local/bin/    # or anywhere on $PATH
```

### Download a release binary

Grab a static binary from
[GitHub Releases](https://github.com/kck8/kq/releases) for Linux (x86_64,
aarch64, ARM, RISC-V, POWER), macOS (Intel, Apple Silicon), Windows, and
FreeBSD — all zero-dependency.

```bash
# Linux x86_64 example
curl -LO https://github.com/kck8/kq/releases/download/v0.8.0/kq-0.8.0-linux-x86_64.tar.gz
tar xzf kq-0.8.0-linux-x86_64.tar.gz && sudo mv kq /usr/local/bin/
```

## Quick Start

```bash
# Filter + project
cat logs.ndjson | kq 'select name, score where active = true order by score desc limit 10'

# Aggregation — single pass, O(1) memory per group
cat logs.ndjson | kq 'select service, count(), avg(duration_ms) group by service order by count desc'

# Scoped queries into nested arrays
cat data.json | kq '[users.*] select email where role = "admin"'

# CSV export
cat data.ndjson | kq 'select dept, count() group by dept' --csv > report.csv

# Split routing: matches to stdout, non-matches to a file, one pass
cat events.ndjson | kq 'where level = "ERROR"' --reject clean.ndjson > errors.ndjson

# LLM stream processing (Ollama / OpenAI / Anthropic)
curl -s localhost:11434/api/generate -d '{...}' | kq --api ollama 'select name, price where price < 100' --csv
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

Single-pass streaming aggregation with O(1) memory per group. On 1M-row
NDJSON, `group by` + `count()` + `avg()` runs in ~0.5 s using ~7 MB of RAM;
jq needs ~8 s and ~1 GB for the same query. `limit N` without `order by`
stops reading input the moment N matches are found.

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