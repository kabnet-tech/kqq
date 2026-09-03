# kqq Limits

kqq is deliberately **not Turing-complete**. It's a query language, not a
programming language — that's why it's fast and predictable. This page
documents what it **cannot** do, so you know where its lane ends.

## What kqq cannot do

- **No joins** — one input stream at a time; no cross-referencing two files
- **No subqueries or nested selects** — a query is a single flat pipeline
- **No user-defined functions** — the built-in function list is the whole list
- **No variables or state between records** — each record is evaluated
  independently (except `group by` accumulators)
- **No recursion or loops** — `case when` is the only branching construct
- **No window functions** — `lag`, `rank`, running totals, etc.
- **No writes back** — output goes to stdout or a file; no in-place editing

## The one memory caveat: `order by` buffers

Streaming O(1) memory applies to `where`, `group by`, and `limit`. Sorting is
the exception: `order by` needs the full match set in memory (~500 MB for 1M
rows, measured). A top-N of a huge stream is the one query that costs RAM.

## What to do instead

If your problem needs any of these, it's out of kqq's lane — pipe kqq's output
into the tool that does have it:

- **Joins, window functions, ad-hoc analytics** → [DuckDB](duckdb.md) —
  kqq pre-filters the stream, DuckDB does the relational math
- **Recursive descent, custom functions, Turing-complete transforms** → jq
- **Anything else** → awk, or your language of choice

kqq composes well in Unix pipelines: it shapes and shrinks the stream, and the
next tool thinks about what's left.

## Syntax gotchas (verified against kqq 0.9.0)

These are the mistakes the parser will catch, collected from real usage:

- **HAVING references output columns only** — `having count() > 2` is a parse
  error. Use the alias (`having n > 2`) or bare `count` when `count()` has no
  alias.
- **SELECT expressions require an alias** — `select upper(name)` is a parse
  error; write `select upper(name) as u`.
- **IN lists use parentheses with quoted strings** — `in ('a', 'b')`, not
  square brackets.
- **`between` uses square brackets** — `between [lo and hi]`.
- **Unknown functions silently return `null`** — there's no error to tip you
  off; check spelling against the [SQL reference](KQQ-SQL-REFERENCE.md).