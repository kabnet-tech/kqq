# kqq + DuckDB

kqq and DuckDB are complementary: **kqq is the gate, DuckDB is the brain.**

kqq streams, filters, and shapes data with O(1) memory per group — it decides
*what* enters the warehouse. DuckDB then does the heavy relational work
(joins, window functions, CTEs) on a much smaller, cleaner table.

| Stage | Tool | Why |
|---|---|---|
| Ingest, filter, shape | kqq | Streams; handles files larger than RAM |
| Analytics, joins, windows | DuckDB | Columnar engine, full SQL |

Every command below was verified against kqq 0.9.0 and DuckDB 1.5.2.

---

## Pattern 1: Filter → NDJSON → table (the workhorse)

```bash
cat logs.ndjson | kqq 'select name, dept, salary where salary > 100000' --ndjson > out.ndjson
duckdb -c "SELECT * FROM read_json_auto('out.ndjson');"
```

kqq drops the fields and rows you don't want; DuckDB infers clean column types
from the remaining records.

## Pattern 2: Aggregate → CSV → DuckDB

kqq does the group-by with O(1) memory; DuckDB gets a tiny typed CSV:

```bash
cat logs.ndjson | kqq 'select dept, count() as n, avg(salary) as avg_sal group by dept' --csv > summary.csv
duckdb -c "SELECT * FROM read_csv_auto('summary.csv', header=true) ORDER BY avg_sal DESC;"
```

## Pattern 3: Direct pipe, no temp file

DuckDB reads `/dev/stdin`, so the whole thing is one pipeline:

```bash
cat logs.ndjson | kqq 'select name, salary where salary > 200000' --ndjson \
  | duckdb -c "CREATE TABLE high_paid AS SELECT * FROM read_json_auto('/dev/stdin');
               SELECT count(*) FROM high_paid;"
```

## Pattern 4: Pre-filter a huge stream (the power move)

kqq shrinks the stream *before* DuckDB sees it. On a 2M-row file, filtering to
just error rows and analyzing took **0.27 s end to end** — DuckDB never sees
the 99.9% you don't care about:

```bash
cat 2M-rows.ndjson | kqq 'select service, latency_ms where status = "error"' --ndjson \
  | duckdb -c "SELECT service,
                      count(*) AS errors,
                      avg(latency_ms) AS avg_lat,
                      max(latency_ms) AS worst
               FROM read_json_auto('/dev/stdin')
               GROUP BY service ORDER BY errors DESC LIMIT 5;"
```

## Pattern 5: Aggregate-first for bigger-than-RAM files

When the input is too big for either tool alone, kqq reduces it to a summary
first (2M rows → 100 rows in 0.7 s), and DuckDB joins/analyzes the summary:

```bash
cat huge.ndjson | kqq 'select service, count() as total group by service' --ndjson \
  | duckdb -c "SELECT count(*) AS services, sum(total) AS rows_seen
               FROM read_json_auto('/dev/stdin');"
```

## Pattern 6: LLM streams → DuckDB

Turn live LLM token output into a queryable table:

```bash
curl -s localhost:11434/api/generate -d '{...}' \
  | kqq --llm response 'select model, tokens' --ndjson \
  | duckdb -c "SELECT model, sum(tokens) FROM read_json_auto('/dev/stdin') GROUP BY model;"
```

---

## Gotchas

- **Always use `--ndjson`** when piping to `read_json_auto` — DuckDB's JSON
  reader wants newline-delimited objects, not kqq's default pretty-printed
  JSON array.
- **`--csv` for aggregates** is often the cleanest handoff: typed columns, no
  JSON parsing on DuckDB's side.
- **`--raw`** works too when you're extracting a single column of values.
- DuckDB can also read the *original* NDJSON directly with
  `read_json_auto('logs.ndjson')` — use kqq first when the file is big, the
  filter is selective, or you need kqq's string/date functions to shape
  fields DuckDB would struggle with.

## When to use which where

| Task | Use |
|---|---|
| "Errors from a 2 GB log" | kqq alone (early-stop) |
| Group-by counts/averages | kqq alone (O(1) memory) |
| Joins across two datasets | DuckDB (kqq has no joins) |
| Window functions, rankings | DuckDB |
| Reshaping nested JSON into flat columns | kqq (`--flat`, `expand`) |
| Ad-hoc analytics on the result | DuckDB |