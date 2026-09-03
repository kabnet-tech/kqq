# Security Policy

## Supported versions

Security fixes are made to the latest release and the `main` branch.
Older releases do not receive security patches — please upgrade.

## Reporting a vulnerability

**Do not open a public issue for security vulnerabilities.**

Instead, use GitHub's private vulnerability reporting:
[Report a vulnerability](https://github.com/kabnet-tech/kqq/security/advisories/new)

Include:

- A description of the issue and its impact
- Steps to reproduce (a sample input file and the `kqq` invocation are ideal)
- The version you tested against (`kqq --version`)

You can expect an initial response within 7 days. Please allow up to 90 days
for a fix before public disclosure; we will credit reporters in the release
notes unless you prefer to remain anonymous.

## Scope

kqq is a command-line data processing tool. In-scope concerns include:

- Memory-safety issues in the Zig code (panics, UB, crashes on crafted input)
- Path traversal or unintended file writes via query syntax (`into`, `--tee`,
  `--reject`, `--out`)
- Injection through query strings when embedded in shell scripts
- Excessive resource consumption from pathological inputs (quadratic behavior,
  unbounded memory)

Out of scope: issues that require an attacker to already control the full
command line in ways any CLI tool would be exposed to.

## Design notes

- kqq is written in memory-safe-by-default Zig with explicit allocator
  discipline; no unsafe constructs are used.
- Release binaries are fully static (musl) on Linux and link no third-party
  libraries on any platform.
- The tool never makes network connections on its own; network access only
  happens when the user pipes from `curl`/similar.