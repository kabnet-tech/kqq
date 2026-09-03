# Contributing to kq

Thanks for your interest in contributing! kq is a small, focused tool and
contributions are welcome — bug reports, bug fixes, documentation, and
well-scoped features.

## Getting started

1. Fork the repo and clone your fork.
2. Install [Zig 0.16.0](https://ziglang.org/download/) (the only dependency).
3. Build and run the tests:

```bash
zig build                        # debug build
zig build test --summary all     # unit + integration tests
python3 tests/cli_tests.py       # end-to-end CLI tests
```

## Reporting bugs

Open a [bug report](https://github.com/kck8/kq/issues/new?template=bug_report.md)
and include:

- The exact `kq` invocation (query string and flags)
- A minimal sample of input data (1–3 lines is usually enough)
- Expected output vs actual output
- Your `kq --version` and OS

If the output is wrong rather than an error, include what `jq` produces for
the same data if you can — it helps pin down semantics.

## Suggesting features

Open a [feature request](https://github.com/kck8/kq/issues/new?template=feature_request.md).
Good feature requests explain the *workload* you're trying to handle, not just
the syntax you want. kq deliberately stays small: the bar for new syntax is
"common enough that data engineers hit it weekly on NDJSON streams."

## Code guidelines

- **Zig 0.16.0** — the codebase tracks the current Zig release; `std.process.Init`
  style APIs are used throughout.
- **Streaming first** — anything that buffers the whole input needs a strong
  justification. Aggregation state should be O(groups), not O(records).
- **Memory discipline** — records use arena-per-record patterns; check
  `deinit` paths on error branches.
- **Tests with every change** — parser/executor changes need coverage in
  `src/integration_tests.zig` (Zig) and/or `tests/cli_tests.py` (end-to-end).
  Bug fixes need a regression test that fails without the fix.
- **No new dependencies** — kq is stdlib-only Zig by design.

## Pull requests

1. Create a feature branch from `main`.
2. Make your change with tests.
3. Run the full suite locally (both commands above).
4. Keep PRs focused — one logical change per PR.
5. Write PR descriptions that explain *why*, not just *what*.

CI runs the test suite on Linux and macOS plus a 19-target cross-compile
matrix. All of it must pass before merge.

## License

By contributing, you agree that your contributions will be licensed under
the Apache License 2.0 that covers this project.