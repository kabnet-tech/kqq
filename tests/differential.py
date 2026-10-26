#!/usr/bin/env python3
"""Differential tests: run the same queries through kqq and DuckDB, compare.

Generates seeded random NDJSON / CSV data (nulls, missing fields, negative and
fractional numbers, integers beyond 2^53, strings with escapes, quotes, commas
and non-ASCII text), runs each kqq query and its SQL equivalent, and compares
the results — as ordered lists when the query sorts, as multisets otherwise,
with a relative tolerance for floating-point aggregates.

Run:   python3 tests/differential.py [--seeds N] [--rows N]
Needs: pip install duckdb   (exits 0 with a notice when it is missing)

Known, deliberate semantic differences are avoided rather than papered over:
kqq skips records whose GROUP BY key is *missing* (DuckDB groups them as NULL),
`len()` counts bytes (DuckDB `strlen`), `upper()` is ASCII-only, and `//` is
kqq's coalesce operator.
"""

import argparse
import csv
import json
import math
import os
import random
import subprocess
import sys
import tempfile
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("differential: duckdb not installed (pip install duckdb) — skipped")
    sys.exit(0)

ROOT = Path(__file__).parent.parent
KQQ = os.environ.get("KQQ_BIN") or str(ROOT / "zig-out/bin/kqq")

GROUPS = ["alpha", "beta", "gamma", "delta", "eps", "zeta"]
NAMES = ["ann", "bob", "Cleo", "dé jà", "eve, jr.", 'say "hi"', "back\\slash",
         "tab\there", "line\nbreak", "日本", "😀 smile", "", "zed"]


def make_rows(rng, n):
    rows = []
    for i in range(n):
        r = {"id": i, "grp": rng.choice(GROUPS)}
        roll = rng.random()
        if roll < 0.08:
            pass  # v missing
        elif roll < 0.15:
            r["v"] = None
        elif roll < 0.6:
            r["v"] = rng.randint(-1000, 1000)
        else:
            r["v"] = round(rng.uniform(-500, 500), rng.choice([0, 1, 3, 6]))
        r["w"] = rng.choice([0, 0.5, 1, 2, 3.25, -4, 10, 100])
        r["name"] = rng.choice(NAMES)
        r["flag"] = rng.random() < 0.5
        r["big"] = rng.choice([2**53 + 1, -(2**53) - 3, 12345678901234567890 % 2**63, i])
        rows.append(r)
    return rows


def write_ndjson(path, rows, rng):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            # Exercise \\u escapes in the input: some lines escape non-ASCII.
            f.write(json.dumps(r, ensure_ascii=rng.random() < 0.5) + "\n")


def write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
        w.writerow(["id", "grp", "v", "name"])
        for r in rows:
            v = r.get("v")
            w.writerow([r["id"], r["grp"], "" if v is None else v, r["name"]])


# (kqq args, DuckDB SQL over table t, ordered?)
NDJSON_CASES = [
    (["select id, v where v > 50"],
     "SELECT id, v FROM t WHERE v > 50", False),
    (["SELECT id, name WHERE name = 'dé jà' OR flag = true AND v < 0"],
     "SELECT id, name FROM t WHERE name = 'dé jà' OR (flag = true AND v < 0)", False),
    (["select id where v is null"],
     "SELECT id FROM t WHERE v IS NULL", False),
    (["select id where v between [-10 and 10] and not (grp = 'beta')"],
     "SELECT id FROM t WHERE v BETWEEN -10 AND 10 AND NOT (grp = 'beta')", False),
    (["select id where grp in ('alpha', 'zeta') and name like 'b%'"],
     "SELECT id FROM t WHERE grp IN ('alpha', 'zeta') AND name LIKE 'b%'", False),
    (["select grp, count() as n, count(v) as nv, sum(v) as s, avg(v) as a, "
      "min(v) as mn, max(v) as mx group by grp"],
     "SELECT grp, count(*) AS n, count(v) AS nv, sum(v) AS s, avg(v) AS a, "
     "min(v) AS mn, max(v) AS mx FROM t GROUP BY grp", False),
    (["select grp, stddev(v) as sd, variance(v) as var group by grp"],
     "SELECT grp, stddev_pop(v) AS sd, var_pop(v) AS var FROM t GROUP BY grp", False),
    (["select grp, sum(v * 2) as s, max(w + 1) as m, avg(v - w) as d group by grp"],
     "SELECT grp, sum(v * 2) AS s, max(w + 1) AS m, avg(v - w) AS d FROM t GROUP BY grp", False),
    (["select count() as n, count(v) as nv, sum(v) as s, avg(v * w) as a"],
     "SELECT count(*) AS n, count(v) AS nv, sum(v) AS s, avg(v * w) AS a FROM t", False),
    # (kqq does not order booleans, so this one is compared as a multiset)
    (["select grp, flag, count() as n group by grp, flag having n > 2 order by n desc, grp"],
     "SELECT grp, flag, count(*) AS n FROM t GROUP BY grp, flag HAVING count(*) > 2", False),
    (["select distinct grp"],
     "SELECT DISTINCT grp FROM t", False),
    # Each group has both flag values, so LIMIT 4 cuts between groups: the
    # row set is determined even though kqq leaves ties on `grp` unordered.
    (["select distinct grp, flag order by grp limit 4"],
     "SELECT grp, flag FROM (SELECT DISTINCT grp, flag FROM t) ORDER BY grp, flag LIMIT 4", False),
    (["select id, v / w as r, v % 7 as m where v is not null"],
     "SELECT id, v / w AS r, v % 7 AS m FROM t WHERE v IS NOT NULL", False),
    (["select id, v order by v desc, id limit 7 where v is not null"],
     "SELECT id, v FROM t WHERE v IS NOT NULL ORDER BY v DESC, id LIMIT 7", True),
    (["select name, big"],
     "SELECT name, big FROM t", False),
    (["select id, case when v > 0 then 'pos' when v < 0 then 'neg' else 'zero' end as sign "
      "where v is not null"],
     "SELECT id, CASE WHEN v > 0 THEN 'pos' WHEN v < 0 THEN 'neg' ELSE 'zero' END AS sign "
     "FROM t WHERE v IS NOT NULL", False),
    (["select grp, count(*) as n group by grp order by n desc, grp limit 3"],
     "SELECT grp, count(*) AS n FROM t GROUP BY grp ORDER BY n DESC, grp LIMIT 3", True),
]

CSV_CASES = [
    (["--delim", ",", "--header", "select grp, count() as n, sum(v) as s group by grp"],
     "SELECT grp, count(*) AS n, sum(v) AS s FROM c GROUP BY grp", False),
    (["--delim", ",", "--header", "select id, name where v > 0"],
     "SELECT id, name FROM c WHERE v > 0", False),
]

def run_kqq(args, data_path):
    with open(data_path, "rb") as f:
        r = subprocess.run([KQQ] + args, stdin=f, capture_output=True, timeout=60)
    if r.returncode != 0:
        raise RuntimeError(f"kqq exited {r.returncode}: {r.stderr.decode(errors='replace')}")
    return [json.loads(line) for line in r.stdout.decode().splitlines() if line.strip()]


def run_duck(con, sql):
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def norm(v):
    """Normalise a value for comparison."""
    if isinstance(v, bool) or v is None or isinstance(v, str):
        return v
    if isinstance(v, int):
        return v
    if hasattr(v, "__float__"):  # Decimal, float
        f = float(v)
        if not math.isfinite(f):
            return None  # JSON has no Infinity/NaN; kqq emits null
        if f.is_integer() and abs(f) < 2**53:
            return int(f)
        return f
    return v


def same_value(a, b):
    a, b = norm(a), norm(b)
    if isinstance(a, (int, float)) and isinstance(b, (int, float)) and not isinstance(a, bool) and not isinstance(b, bool):
        if isinstance(a, int) and isinstance(b, int):
            return a == b
        return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-9)
    return a == b


def same_row(a, b):
    return a.keys() == b.keys() and all(same_value(a[k], b[k]) for k in a)


def sort_key(row):
    return json.dumps({k: (round(norm(v), 6) if isinstance(norm(v), float) else norm(v))
                       for k, v in row.items()}, sort_keys=True, default=str)


def compare(kqq_rows, duck_rows, ordered):
    if not ordered:
        kqq_rows = sorted(kqq_rows, key=sort_key)
        duck_rows = sorted(duck_rows, key=sort_key)
    if len(kqq_rows) != len(duck_rows):
        return f"row count {len(kqq_rows)} (kqq) vs {len(duck_rows)} (duckdb)"
    for i, (a, b) in enumerate(zip(kqq_rows, duck_rows)):
        if not same_row(a, b):
            return f"row {i}: kqq {a!r} vs duckdb {b!r}"
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", type=int, default=5)
    ap.add_argument("--rows", type=int, default=400)
    opts = ap.parse_args()

    failures = 0
    checks = 0
    with tempfile.TemporaryDirectory() as tmp:
        for seed in range(opts.seeds):
            rng = random.Random(seed)
            rows = make_rows(rng, opts.rows)
            nd = os.path.join(tmp, f"t{seed}.ndjson")
            cv = os.path.join(tmp, f"c{seed}.csv")
            write_ndjson(nd, rows, rng)
            write_csv(cv, rows)

            con = duckdb.connect()
            con.execute("SET error_on_division_by_zero = false")  # NULL, as kqq
            con.execute(
                "CREATE TABLE t AS SELECT * FROM read_json(?, format='newline_delimited', "
                "columns={'id': 'BIGINT', 'grp': 'VARCHAR', 'v': 'DOUBLE', 'w': 'DOUBLE', "
                "'name': 'VARCHAR', 'flag': 'BOOLEAN', 'big': 'HUGEINT'})", [nd])
            con.execute(
                "CREATE TABLE c AS SELECT * FROM read_csv(?, header=true, quote='\"', escape='\"', "
                "columns={'id': 'BIGINT', 'grp': 'VARCHAR', 'v': 'DOUBLE', 'name': 'VARCHAR'})", [cv])

            for cases, path in ((NDJSON_CASES, nd), (CSV_CASES, cv)):
                for args, sql, ordered in cases:
                    checks += 1
                    label = f"seed {seed}: kqq {' '.join(args)!r}"
                    try:
                        got = run_kqq(args, path)
                    except Exception as e:  # noqa: BLE001 — report and keep going
                        failures += 1
                        print(f"FAIL  {label}\n      {e}")
                        continue
                    want = run_duck(con, sql)
                    diff = compare(got, want, ordered)
                    if diff:
                        failures += 1
                        print(f"FAIL  {label}\n      {diff}")
            con.close()

    print(f"\n{checks - failures}/{checks} differential checks passed" + (" ✓" if failures == 0 else ""))
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
