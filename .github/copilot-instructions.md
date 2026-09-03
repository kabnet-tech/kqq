# Copilot Instructions for kqq

## What this project is

kqq is a streaming SQL query tool for NDJSON/JSON, written in Zig 0.16.0 with
zero dependencies. Single static binary. The core value proposition is
single-pass streaming aggregation with O(1) memory per group — never buffer
the full input.

## Build & test commands

```bash
zig build                          # debug build → zig-out/bin/kqq
zig build -Doptimize=ReleaseFast   # release build (ALWAYS use for benchmarks)
zig build test --summary all       # unit + integration tests (8 suites)
python3 tests/cli_tests.py         # end-to-end CLI tests (63 tests)
```

Run the full test suite after every change. Both layers must pass.

## Architecture (read before changing code)

Data flows: **parse → AST → execute → emit**

- `src/query.zig` — the parser. Produces the AST. ~2000 lines.
- `src/query/ast.zig` — AST type definitions (`Expr`, `WhereClause`,
  `SelectField`, `GroupByKey`, `Query`) plus `cloneExpr`.
- `src/query/tokenizer.zig` — token lexer.
- `src/stream_exec.zig` — streaming executors, including `aggregateGroupBy`
  (the heart of GROUP BY). ~2600 lines.
- `src/core/where.zig` — WHERE evaluation against Records.
- `src/core/raw_where.zig` — fast-path WHERE on raw JSON bytes (skips parse).
- `src/core/expr.zig` — expression evaluator + built-in functions.
- `src/core/record.zig` — Record type (arena-allocated key/value pairs).
- `src/main.zig` — CLI flag parsing and dispatch.

### Critical invariants

1. **Streaming first.** Nothing may buffer the whole input. Aggregation state
   must be O(groups), not O(records). If a change needs to collect all
   records, it needs a strong justification.
2. **WHERE has 5 evaluation paths** (streaming, raw pushdown, scoped, flat,
   native). Semantic fixes for WHERE/aliases belong at **parse time** in
   `query.zig` so all paths benefit — do not patch individual evaluators.
3. **HAVING references output column names** (aliases included); WHERE
   references input field names. Never alias-resolve HAVING keys.
4. **Memory discipline**: records are arena-per-record; every allocation path
   needs an `errdefer`/`deinit` counterpart. Match the existing patterns.
5. **WHERE leaf conditions with `lhs_expr != null`** have the synthetic key
   `"__expr__"` — skip them in any key-rewriting pass.

## Conventions

- Zig 0.16.0 APIs (`std.process.Init`, `std.Io.Reader/Writer`). Do not
  "modernize" to other styles or downgrade to older Zig idioms.
- No new dependencies, ever. stdlib-only Zig.
- Tests live in two layers: `src/integration_tests.zig` (Zig, uses
  `std.testing.allocator` so leaks fail tests) and `tests/cli_tests.py`
  (subprocess end-to-end). Parser/executor changes need coverage in at least
  one; **bug fixes need a regression test that fails without the fix**.
- Test data facts (used in assertions): `testdata/employees.ndjson` has 20
  employees, 6 departments, engineering=9; `testdata/events.ndjson` has 12
  events (INFO=5, WARN=2, ERROR=5).
- Commit messages: imperative mood, explain *why* in the body when non-obvious.

## Gotchas

- **Debug builds are ~17× slower.** Never benchmark a debug build; the
  performance docs assume ReleaseFast.
- **Cross-compiling overwrites `zig-out/bin/kqq`** with a foreign-arch binary.
  Re-run `zig build` before native tests or benchmarks, or you'll see
  `OSError: Exec format error`.
- `head` in a pipe makes kqq exit with `error: WriteFailed` (SIGPIPE) — that's
  expected, not a bug.
- `build.zig` hardcodes the version string (line ~174) and
  `tests/cli_tests.py` asserts it (`kqq X.Y.Z`) — bump all three places
  (`build.zig.zon` too) when releasing.
- The package fingerprint in `build.zig.zon` is derived from the package
  name; if you rename the package, Zig will tell you the new fingerprint.

## CI

- `ci.yml`: test job (Linux + macOS, ReleaseFast build + both test layers +
  smoke test) and a 19-target cross-compile matrix. All must pass.
- `release.yml`: fires on `v*` tags, builds 15 platform binaries, publishes a
  GitHub Release with SHA256 checksums. Asset names are `kqq-<version>-<platform>`.