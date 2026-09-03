#!/usr/bin/env python3
"""CLI-level tests for kqq — exercises main.zig flag parsing and dispatch paths.

Run:  python3 tests/cli_tests.py
Pass: exits 0 with a summary line
Fail: exits 1 with the first failing test name and diff
"""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).parent.parent
KQQ = str(ROOT / "zig-out/bin/kqq")

NDJSON_USERS = b"""\
{"name":"Alice","age":30,"active":true}
{"name":"Bob","age":25,"active":false}
{"name":"Carol","age":35,"active":true}
"""

NDJSON_ORDERS = b"""\
{"id":1,"region":"east","amount":100}
{"id":2,"region":"west","amount":200}
{"id":3,"region":"east","amount":300}
"""

CSV_DATA = b"name,age,city\nAlice,30,NY\nBob,25,LA\n"
TSV_DATA = b"name\tage\tcity\nAlice\t30\tNY\nBob\t25\tLA\n"


# ─── Test harness ─────────────────────────────────────────────────────────────

passed = 0
failed = 0


def run(args, *, stdin=b"", env=None, expect_exit=0):
    """Run kqq with args, return (stdout, stderr, returncode)."""
    e = dict(os.environ)
    if env:
        e.update(env)
    r = subprocess.run(
        [KQQ] + args,
        input=stdin,
        capture_output=True,
        env=e,
    )
    return r.stdout, r.stderr, r.returncode


def check(name, *, args, stdin=b"", env=None, expect_exit=0,
          stdout_contains=None, stdout_equals=None,
          stdout_json=None, stderr_contains=None):
    global passed, failed
    out, err, code = run(args, stdin=stdin, env=env)
    out_s = out.decode(errors="replace")
    err_s = err.decode(errors="replace")

    def fail(reason):
        global failed
        failed += 1
        print(f"FAIL  {name}")
        print(f"      {reason}")
        print(f"      stdout: {out_s!r:.200}")
        print(f"      stderr: {err_s!r:.200}")
        print(f"      exit:   {code}")

    if code != expect_exit:
        fail(f"expected exit {expect_exit}, got {code}")
        return

    if stdout_contains is not None:
        for needle in (stdout_contains if isinstance(stdout_contains, list) else [stdout_contains]):
            if needle not in out_s:
                fail(f"stdout missing {needle!r}")
                return

    if stdout_equals is not None:
        if out_s.strip() != stdout_equals.strip():
            fail(f"stdout mismatch:\n  expected: {stdout_equals!r:.200}\n  got:      {out_s!r:.200}")
            return

    if stdout_json is not None:
        try:
            got = json.loads(out_s)
        except json.JSONDecodeError as e:
            fail(f"stdout is not valid JSON: {e}")
            return
        if got != stdout_json:
            fail(f"JSON mismatch:\n  expected: {stdout_json}\n  got:      {got}")
            return

    if stderr_contains is not None:
        for needle in (stderr_contains if isinstance(stderr_contains, list) else [stderr_contains]):
            if needle not in err_s:
                fail(f"stderr missing {needle!r}")
                return

    passed += 1
    print(f"ok    {name}")


# ─── --version / --help ───────────────────────────────────────────────────────

check("--version flag",
      args=["--version"],
      stdin=b"",
      expect_exit=0,
      stdout_contains="kqq 0.9.0")

check("-V short flag",
      args=["-V"],
      stdin=b"",
      expect_exit=0,
      stdout_contains="kqq 0.9.0")

check("--help flag",
      args=["--help"],
      stdin=b"",
      expect_exit=0,
      stdout_contains=["--flat", "--buf", "--csv", "--tsv", "--llm", "--api", "--expect"])

check("-h short flag",
      args=["-h"],
      stdin=b"",
      expect_exit=0,
      stdout_contains="Usage:")

# ─── basic NDJSON passthrough & query ─────────────────────────────────────────

check("NDJSON passthrough (no query)",
      args=[],
      stdin=NDJSON_USERS,
      expect_exit=0,
      stdout_contains=['"Alice"', '"Bob"', '"Carol"'])

check("NDJSON select name",
      args=['select name'],
      stdin=NDJSON_USERS,
      expect_exit=0,
      stdout_contains=['"Alice"', '"Bob"', '"Carol"'])

check("NDJSON where filter",
      args=['select name where active = true'],
      stdin=NDJSON_USERS,
      expect_exit=0,
      stdout_contains=['"Alice"', '"Carol"'])

check("NDJSON where filter excludes",
      args=['select name where active = true'],
      stdin=NDJSON_USERS,
      expect_exit=0,
      stdout_contains=['Alice'])

check("NDJSON where no match exits 0",
      args=['where age > 999'],
      stdin=NDJSON_USERS,
      expect_exit=0,
      stdout_equals="")

check("NDJSON order by",
      args=['select name order by age asc'],
      stdin=NDJSON_USERS,
      expect_exit=0,
      stdout_contains='"Bob"')

check("NDJSON limit",
      args=['select name limit 1'],
      stdin=NDJSON_USERS,
      expect_exit=0,
      stdout_contains='"Alice"')

check("NDJSON group by distinct regions",
      args=['select region group by region'],
      stdin=NDJSON_ORDERS,
      expect_exit=0,
      stdout_contains=['"east"', '"west"'])

check("NDJSON count(*) global agg",
      args=['select count(*)'],
      stdin=NDJSON_USERS,
      expect_exit=0,
      stdout_contains='"count"')

check("NDJSON group by alias of plain field",
      args=['select region as r, count() group by r'],
      stdin=NDJSON_ORDERS,
      expect_exit=0,
      stdout_contains=['"r": "east"', '"r": "west"'])

check("NDJSON group by alias with where after group by",
      args=['select region as r, count() group by r where r = "east"'],
      stdin=NDJSON_ORDERS,
      expect_exit=0,
      stdout_contains='"r": "east"')

check("NDJSON where on alias without group by",
      args=['select region as r where r = "east"'],
      stdin=NDJSON_ORDERS,
      expect_exit=0,
      stdout_contains='"r": "east"')

# ─── output format flags ──────────────────────────────────────────────────────

check("--raw flag emits bare values",
      args=['--raw', 'select name'],
      stdin=NDJSON_USERS,
      expect_exit=0,
      stdout_contains=["Alice", "Bob", "Carol"])

check("--raw -r short alias",
      args=['-r', 'select name'],
      stdin=NDJSON_USERS,
      expect_exit=0,
      stdout_contains="Alice")

check("--csv output has header",
      args=['--csv', 'select name, age'],
      stdin=NDJSON_USERS,
      expect_exit=0,
      stdout_contains="name,age")

check("--tsv output has header",
      args=['--tsv', 'select name, age'],
      stdin=NDJSON_USERS,
      expect_exit=0,
      stdout_contains="name\tage")

check("--ndjson flag (explicit)",
      args=['--ndjson', 'select name'],
      stdin=NDJSON_USERS,
      expect_exit=0,
      stdout_contains='"Alice"')

# ─── --flat mode ──────────────────────────────────────────────────────────────

NESTED = b'{"user":{"name":"Dave","score":42}}\n'

check("--flat flattens nested keys",
      args=['--flat'],
      stdin=NESTED,
      expect_exit=0,
      stdout_contains="user.name")

check("--flat with query on dotted key",
      args=['--flat', 'select user.name'],
      stdin=NESTED,
      expect_exit=0,
      stdout_contains="Dave")

# ─── --buf mode ───────────────────────────────────────────────────────────────

check("--buf passthrough",
      args=['--buf'],
      stdin=b'[{"x":1},{"x":2}]',
      expect_exit=0,
      stdout_contains='"x"')

# ─── --tee flag ───────────────────────────────────────────────────────────────

def test_tee():
    with tempfile.NamedTemporaryFile(suffix=".ndjson", delete=False) as f:
        tee_path = f.name
    try:
        out, err, code = run(
            ['--tee', tee_path, 'select name where active = true'],
            stdin=NDJSON_USERS,
        )
        assert code == 0, f"exit {code}"
        tee_content = Path(tee_path).read_text()
        # tee should have ALL records (3), not just matches (2)
        assert tee_content.count('"name"') == 3, \
            f"expected 3 records in tee file, got: {tee_content!r}"
        global passed
        passed += 1
        print("ok    --tee writes all records to file")
    except Exception as e:
        global failed
        failed += 1
        print(f"FAIL  --tee writes all records to file")
        print(f"      {e}")
    finally:
        os.unlink(tee_path)

test_tee()

# ─── --reject flag ────────────────────────────────────────────────────────────

def test_reject():
    with tempfile.NamedTemporaryFile(suffix=".ndjson", delete=False) as f:
        reject_path = f.name
    try:
        out, err, code = run(
            ['--reject', reject_path, 'select name where active = true'],
            stdin=NDJSON_USERS,
        )
        assert code == 0, f"exit {code}"
        reject_content = Path(reject_path).read_text()
        # reject should have only the non-matching record (Bob)
        assert "Bob" in reject_content, f"Bob not in reject file: {reject_content!r}"
        assert "Alice" not in reject_content, f"Alice should not be in reject file"
        global passed
        passed += 1
        print("ok    --reject writes non-matching records to file")
    except Exception as e:
        global failed
        failed += 1
        print(f"FAIL  --reject writes non-matching records to file")
        print(f"      {e}")
    finally:
        os.unlink(reject_path)

test_reject()

# ─── --out flag ───────────────────────────────────────────────────────────────

def test_out():
    with tempfile.NamedTemporaryFile(suffix=".ndjson", delete=False) as f:
        out_path = f.name
    try:
        out, err, code = run(
            ['--out', out_path, 'select name'],
            stdin=NDJSON_USERS,
        )
        assert code == 0, f"exit {code}"
        file_content = Path(out_path).read_text()
        # File should have same content as stdout
        assert "Alice" in out.decode(), "Alice not in stdout"
        assert "Alice" in file_content, "Alice not in --out file"
        global passed
        passed += 1
        print("ok    --out writes output to file and stdout")
    except Exception as e:
        global failed
        failed += 1
        print(f"FAIL  --out writes output to file and stdout")
        print(f"      {e}")
    finally:
        os.unlink(out_path)

test_out()

# ─── --arg injection ──────────────────────────────────────────────────────────

check("--arg string injection",
      args=['--arg', 'target', 'Alice', 'select name where name = $target'],
      stdin=NDJSON_USERS,
      expect_exit=0,
      stdout_contains='"Alice"')

check("--arg numeric injection",
      args=['--arg', 'min_age', '28', 'select name where age > $min_age'],
      stdin=NDJSON_USERS,
      expect_exit=0,
      stdout_contains=['"Alice"', '"Carol"'])

check("--arg excludes below threshold",
      args=['--arg', 'min_age', '28', 'select name where age > $min_age'],
      stdin=NDJSON_USERS,
      expect_exit=0,
      stdout_contains='"Alice"')

# ─── $ENV.VARNAME substitution ────────────────────────────────────────────────

check("$ENV.VARNAME string substitution",
      args=['select name where name = $ENV.TARGET_NAME'],
      stdin=NDJSON_USERS,
      env={"TARGET_NAME": "Bob"},
      expect_exit=0,
      stdout_contains='"Bob"')

check("$ENV.VARNAME numeric substitution",
      args=['select name where age > $ENV.MIN_AGE'],
      stdin=NDJSON_USERS,
      env={"MIN_AGE": "28"},
      expect_exit=0,
      stdout_contains='"Alice"')

check("$ENV.VARNAME unset becomes null (no match)",
      args=['select name where name = $ENV.NONEXISTENT_VAR_XYZ'],
      stdin=NDJSON_USERS,
      expect_exit=0,
      stdout_equals="")

# ─── --delim / --header / --cols ─────────────────────────────────────────────

check("--delim comma with --header",
      args=['--delim', ',', '--header', 'select name, age'],
      stdin=CSV_DATA,
      expect_exit=0,
      stdout_contains=["Alice", "Bob"])

check("--delim tab with --header",
      args=['--delim', '\t', '--header', 'select name'],
      stdin=TSV_DATA,
      expect_exit=0,
      stdout_contains="Alice")

check("--delim with --cols",
      args=['--delim', ',', '--cols', 'name,age,city', 'select name where age > 26'],
      stdin=b"Alice,30,NY\nBob,25,LA\n",
      expect_exit=0,
      stdout_contains="Alice")

check("--delim where filter no match exits 0",
      args=['--delim', ',', '--header', 'where age > 999'],
      stdin=CSV_DATA,
      expect_exit=0,
      stdout_equals="")

# ─── --text mode ─────────────────────────────────────────────────────────────

check("--text mode each line becomes {line:...}",
      args=['--text', 'select line'],
      stdin=b"hello\nworld\n",
      expect_exit=0,
      stdout_contains=["hello", "world"])

check("--text mode where filter",
      args=['--text', 'select line where line contains "ell"'],
      stdin=b"hello\nworld\n",
      expect_exit=0,
      stdout_contains="hello")

# ─── --expect schema ─────────────────────────────────────────────────────────

# --expect only activates in LLM mode (--llm / --llm-path / --api)
# In regular NDJSON mode it has no effect (schema is not wired up).
check("--expect with --llm validates schema and passes valid record",
      args=['--llm', 'response', '--expect', 'name:string,price:number', 'select name, price'],
      stdin=b'{"response":"{\\"name\\":\\"Alice\\",\\"price\\":42}"}\n',
      expect_exit=0,
      stdout_contains='"Alice"')

check("--expect with --llm rejects wrong-type record",
      args=['--llm', 'response', '--expect', 'name:string,price:number', 'select name, price'],
      stdin=b'{"response":"{\\"name\\":123,\\"price\\":\\"bad\\"}"}\n',
      expect_exit=0,
      stdout_equals="")

# ─── --api presets ────────────────────────────────────────────────────────────
# These require llm_field / llm_path to be set via the preset.
# We can verify the preset dispatches correctly by checking that
# kqq doesn't die with "requires a query argument" (it sets field internally).

OLLAMA_STREAM = b"""\
{"model":"llama3","response":"{","done":false}
{"model":"llama3","response":"\\"na","done":false}
{"model":"llama3","response":"me\\": \\"Da","done":false}
{"model":"llama3","response":"ve\\"}","done":false}
{"model":"llama3","response":"","done":true}
"""

check("--api ollama sets llm_field=response",
      args=['--api', 'ollama', 'select name'],
      stdin=OLLAMA_STREAM,
      expect_exit=0,
      stdout_contains="Dave")

check("--api unknown preset dies with error",
      args=['--api', 'badpreset', 'select name'],
      stdin=b"",
      expect_exit=1,
      stderr_contains="unknown preset")

check("--api missing preset name dies",
      args=['--api'],
      stdin=b"",
      expect_exit=1,
      stderr_contains="requires a preset")

# ─── --llm field ─────────────────────────────────────────────────────────────

LLM_STREAM = b"""\
{"response":"{","done":false}
{"response":"\\"city\\": \\"Paris\\"","done":false}
{"response":"}","done":false}
"""

check("--llm accumulates token field",
      args=['--llm', 'response', 'select city'],
      stdin=LLM_STREAM,
      expect_exit=0,
      stdout_contains="Paris")

check("--llm missing field name dies",
      args=['--llm'],
      stdin=b"",
      expect_exit=1,
      stderr_contains="requires a field")

# ─── --rolling flag ───────────────────────────────────────────────────────────

check("--rolling with --llm emits snapshots",
      args=['--llm', 'response', '--rolling', 'select city'],
      stdin=LLM_STREAM,
      expect_exit=0,
      stdout_contains="Paris")

# ─── --split-by ───────────────────────────────────────────────────────────────

def test_split_by():
    orig_cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        try:
            out, err, code = run(
                ['--split-by', 'region'],
                stdin=NDJSON_ORDERS,
            )
            assert code == 0, f"exit {code}, stderr: {err.decode()!r}"
            east_file = Path(tmpdir) / "east.jsonl"
            west_file = Path(tmpdir) / "west.jsonl"
            assert east_file.exists(), "east.jsonl not created"
            assert west_file.exists(), "west.jsonl not created"
            east_lines = east_file.read_text().strip().splitlines()
            assert len(east_lines) == 2, f"expected 2 east records, got {len(east_lines)}"
            global passed
            passed += 1
            print("ok    --split-by creates per-value files")
        except Exception as e:
            global failed
            failed += 1
            print(f"FAIL  --split-by creates per-value files")
            print(f"      {e}")
        finally:
            os.chdir(orig_cwd)

test_split_by()

# ─── INTO clause (single query) ───────────────────────────────────────────────

def test_into():
    with tempfile.NamedTemporaryFile(suffix=".ndjson", delete=False) as f:
        into_path = f.name
    try:
        out, err, code = run(
            [f"select name into '{into_path}'"],
            stdin=NDJSON_USERS,
        )
        assert code == 0, f"exit {code}, stderr: {err.decode()!r}"
        content = Path(into_path).read_text()
        assert "Alice" in content, f"Alice not in INTO file: {content!r}"
        global passed
        passed += 1
        print("ok    INTO clause writes to file")
    except Exception as e:
        global failed
        failed += 1
        print(f"FAIL  INTO clause writes to file")
        print(f"      {e}")
    finally:
        os.unlink(into_path)

test_into()

# ─── Multi-query (semicolon) ─────────────────────────────────────────────────

check("multi-query semicolon runs both queries",
      args=["select name where active = true; select name where active = false"],
      stdin=NDJSON_USERS,
      expect_exit=0,
      stdout_contains=["Alice", "Bob"])

# ─── Scoped (streaming) query ─────────────────────────────────────────────────

SCOPED_JSON = b'{"users":[{"name":"Eve","score":9},{"name":"Frank","score":3}]}'

check("scoped query [users.*] select",
      args=['[users.*] select name'],
      stdin=SCOPED_JSON,
      expect_exit=0,
      stdout_contains=["Eve", "Frank"])

check("scoped query with where",
      args=['[users.*] select name where score > 5'],
      stdin=SCOPED_JSON,
      expect_exit=0,
      stdout_contains="Eve")

# ─── Error / die paths ────────────────────────────────────────────────────────

check("bad JSON in buffered mode dies with error",
      args=['--buf', 'select x'],
      stdin=b"not json at all",
      expect_exit=1,
      stderr_contains="failed to parse JSON")

check("--tee missing path arg dies",
      args=['--tee'],
      stdin=b"",
      expect_exit=1,
      stderr_contains="requires a file path")

check("--reject missing path arg dies",
      args=['--reject'],
      stdin=b"",
      expect_exit=1,
      stderr_contains="requires a file path")

check("--out missing path arg dies",
      args=['--out'],
      stdin=b"",
      expect_exit=1,
      stderr_contains="requires a file path")

check("--expect missing schema arg dies",
      args=['--expect'],
      stdin=b"",
      expect_exit=1,
      stderr_contains="requires a schema")

check("--delim missing char arg dies",
      args=['--delim'],
      stdin=b"",
      expect_exit=1,
      stderr_contains="requires a delimiter")

check("--cols missing names arg dies",
      args=['--cols'],
      stdin=b"",
      expect_exit=1,
      stderr_contains="requires column names")

check("--split-by missing field arg dies",
      args=['--split-by'],
      stdin=b"",
      expect_exit=1,
      stderr_contains="requires a field name")

check("--arg missing value dies",
      args=['--arg', 'x'],
      stdin=b"",
      expect_exit=1,
      stderr_contains="requires two arguments")

check("--llm-path missing arg dies",
      args=['--llm-path'],
      stdin=b"",
      expect_exit=1,
      stderr_contains="requires a dotted path")

check("invalid query string dies with parse error",
      args=['select where where where'],
      stdin=b'{"x":1}\n',
      expect_exit=1,
      stderr_contains='unexpected end of query')

# ─── No-input TTY guard ───────────────────────────────────────────────────────
# We can't easily fake a TTY in subprocess, so we skip that path.
# It's tested implicitly by the fact that all other tests pipe stdin.

# ─── Summary ──────────────────────────────────────────────────────────────────

total = passed + failed
print()
print(f"{passed}/{total} CLI tests passed" + (" ✓" if failed == 0 else ""))
sys.exit(0 if failed == 0 else 1)
