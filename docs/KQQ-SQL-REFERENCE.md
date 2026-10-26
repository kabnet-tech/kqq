# kqq SQL Reference

Complete reference for the kqq query language. Every example below was
verified against kqq 0.9.0.

```
STDIN | kqq [OPTIONS] ['<query>']
```

A query is a single flat pipeline:

```
[scope]  select <fields>  where <expr>  group by <f>  having <expr>  order by <f> asc|desc  limit N  into '<file>'
```

Clauses are usually written in that order (SQL style), but after `select`
they may come in any order, each at most once (`group by` may also precede
`select`). Only `select` is required — or just a `where` to filter whole
records.

Keywords and function names are case-insensitive (`SELECT`, `Where`,
`ORDER BY`, `Upper(...)`); field names are case-sensitive, like JSON keys,
and a keyword can be used as a field name (`select type, desc`). The one
exception: lowercase `count` without parentheses is shorthand for `count()`;
`Count` or `COUNT` alone is a field. A number in an expression is a literal
(`select 2 * price as p`, `where 2 * price > 300`).
Every token must be understood: anything left over — a misspelled clause, a
missing comma, an unknown function — is a parse error (exit 1), never
silently ignored.

---

## SELECT

Projects fields from each record. Multiple fields are comma-separated.

```bash
echo '{"name":"Alice","age":30}' | kqq 'select name'
# {"name": "Alice"}
```

| Form | Meaning |
|---|---|
| `select name, age` | Project named fields (top-level or dotted paths) |
| `select *` | Copy the whole record |
| `select * add <expr> as col` | Copy all fields, then append a computed column |
| `select * remove c` | Copy all fields, then drop a column |
| `select distinct f` | Emit each unique value of `f` once |
| `select expand(arr)` | Unnest an array field into one output row per element |
| `select <expr> as alias` | Any expression; `as` names the output column |
| `select f as "my col"` | An alias may be quoted, or be a keyword (`as count`) |

```bash
echo '{"a":1,"b":2}' | kqq 'select * add a + b as total'
# {"a": 1, "b": 2, "total": 3}

echo '{"a":1,"b":2,"c":3}' | kqq 'select * remove c'
# {"a": 1, "b": 2}

echo '{"arr":[1,2]}' | kqq 'select expand(arr) as v'
# {"v": 1}   {"v": 2}
```

Dotted paths reach into nested objects:

```bash
echo '{"a":{"b":{"c":5}}}' | kqq 'select a.b.c'
# {"c": 5}
```

> Expressions in SELECT require an alias: `select upper(name) as u`.
> A bare `select upper(name)` is a parse error.

---

## WHERE

Filters input records. References **input** field names (not output aliases).

```bash
echo '{"name":"Alice","age":30}' | kqq 'select name where age > 25'
```

Combine conditions with `and` / `or`, group with parentheses.

### Comparison

| Operator | Example | Meaning |
|---|---|---|
| `=` | `level = "ERROR"` | equals |
| `!=` | `status != "ok"` | not equals |
| `>`, `<`, `>=`, `<=` | `price >= 100` | numeric comparison |
| `like` | `name like "A%"` | glob match (`%` any run, `_` one char) |
| `contains` | `msg contains "timeout"` | substring |
| `not contains` | `msg not contains "debug"` | inverse substring |
| `starts_with` | `path starts_with "/api"` | prefix |
| `ends_with` | `file ends_with ".json"` | suffix |
| `in` | `level in ('ERROR', 'WARN')` | membership in a list |
| `not in` | `region not in ('eu', 'ap')` | inverse membership |
| `is null` | `parent is null` | value is null or field missing |
| `is not null` | `parent is not null` | value exists and is non-null |
| `has(f)` | `where has(parent)` | key exists (even if its value is null) |
| `matches` | `name matches "^[A-Z][a-z]+$"` | regex match |
| `not matches` | `name not matches "^test"` | inverse regex |
| `between [lo and hi]` | `age between [25 and 35]` | inclusive range |

Lists for `in` / `not in` use parentheses with quoted strings:
`level in ('ERROR', 'WARN')`.

### Variables in queries

```bash
# --arg binds $name to a string value
kqq --arg maxp 100 'select price where price > $maxp'

# $ENV.VAR reads from the process environment (numeric values stay numeric)
MAXP=100 kqq 'select price where price < $ENV.MAXP'
```

---

## GROUP BY / Aggregates

Single-pass streaming aggregation — O(1) memory per group.

```bash
cat logs.ndjson | kqq 'select service, count() as n, avg(latency_ms) as p50 group by service'
```

| Aggregate | Meaning |
|---|---|
| `count()`, `count(*)`, bare `count` | Number of records in the group |
| `count(x)` | Number of records where `x` is present and not null |
| `sum(f)` | Sum of `f` |
| `avg(f)` | Arithmetic mean of `f` |
| `min(f)` / `max(f)` | Extremes of `f` |
| `stddev(f)` | Standard deviation of `f` |
| `variance(f)` | Variance of `f` |

Aggregates skip null and missing values (`count()` excepted), and their
argument may be any expression: `sum(price * qty)`, `max(len(name))`.
Without `as`, an aggregate column is named after the function (`sum`,
`count`, ...); when two would share a name, the later one takes its field's
name or its position as a suffix (`count(), count(n)` → `count`, `count_n`). Aggregate columns appear in SELECT order, after the group keys;
group keys are always included. `stddev` and `variance` are the population
forms (DuckDB `stddev_pop` / `var_pop`).

```bash
kqq 'select region, count(discount) as discounted, sum(price * qty) as revenue group by region'
```

### HAVING

Filters **groups** after aggregation. Reference the output column (alias or
`count`):

```bash
# alias form
kqq 'select dept, count() as n group by dept having n > 2'

# count form (when count() has no alias, its column is named "count")
kqq 'select dept, count() group by dept having count > 2'
```

> `having count() > 2` (with parentheses) is **not** valid — HAVING references
> output column names, so use the alias or bare `count`.

### ORDER BY

Works on output columns (aliases) and ordinals:

```bash
kqq 'select dept, count() as n group by dept order by n desc'   # by alias
kqq 'select dept, count() as n group by dept order by 2 desc'   # by ordinal
```

> Note: `order by` buffers the full match set in memory (~500 MB for 1M rows).
> `where`, `group by`, and `limit` are true streaming paths.

---

## Expressions

Arithmetic (`+ - * /`), comparison, and `case when` work in SELECT and WHERE:

```bash
echo '{"name":"Alice","salary":100}' | kqq \
  'select upper(name) as u, salary * 2 as dbl, case when salary > 50 then "high" else "low" end as band'
# {"u": "ALICE", "dbl": 200, "band": "high"}
```

`case when` is the only branching construct — there are no loops or recursion.

A computed column that cannot be evaluated — a missing field, a non-numeric
operand, division or modulo by zero — is `null`, and the column is still
emitted; a result too large for a double is printed as `null` as well (JSON
has no infinity). Unknown function names are a parse error.

### Numbers

Numbers are IEEE doubles for arithmetic and comparison, but values are
passed through exactly as written: an integer such as `9007199254740993`
(beyond 2^53) comes out with the same digits. Very large and very small
magnitudes are printed in exponent form (`1e300`, `1.5e-10`).

---

## Functions

All functions are built-in; there are no user-defined functions.

### String

| Function | Example → Result |
|---|---|
| `upper(s)` | `upper("a")` → `"A"` |
| `lower(s)` | `lower("A")` → `"a"` |
| `len(s)` | `len("abc")` → `3` |
| `trim(s)` | trims whitespace |
| `concat(a, b, ...)` | `concat("a", "b")` → `"ab"` |
| `substr(s, start, len)` | substring |
| `replace(s, from, to)` | `replace("aa", "a", "b")` → `"bb"` |
| `lpad(s, len, ch)` | `lpad("5", 3, "0")` → `"005"` |
| `rpad(s, len, ch)` | right-pad |
| `split(s, delim)` | `split("a,b", ",")` → `["a","b"]` |
| `format("tpl {f}")` | `format("hi {name}")` → interpolates fields |

### Math / Type

| Function | Meaning |
|---|---|
| `round(n)`, `floor(n)`, `ceil(n)`, `abs(n)` | numeric ops |
| `to_number(x)` | string → number |
| `to_str(n)` | number/bool → string |
| `type_of(v)` | `"string" \| "number" \| "boolean" \| "null" \| "array" \| "object"` |

### Null handling

| Function | Meaning |
|---|---|
| `coalesce(a, b, ...)` | first non-null argument |
| `isnull(f, default)` | SELECT sugar: emits `default` when field is null/missing |
| `ifhas(f, then, else)` | `then` if field exists, else `else` |

### Object introspection

| Function | Meaning |
|---|---|
| `keys()` / `keys(f)` | JSON array of top-level (or nested object's) keys |
| `values()` / `values(f)` | array of values |
| `to_entries()` / `to_entries(f)` | `[{"key":k,"value":v}, ...]` |

### Date / time (UTC only)

| Function | Meaning |
|---|---|
| `now()` | current UTC ISO string, e.g. `2026-09-03T12:00:00Z` |
| `now_epoch()` / `now_ms()` | current epoch seconds / milliseconds |
| `from_epoch(n)` / `from_epoch_ms(n)` | epoch → ISO string |
| `to_epoch(s)` / `to_epoch_ms(s)` | ISO string (or number) → epoch |
| `date_part(ts, part)` | part: `year` `month` `day` `hour` `minute` `second` `epoch` |
| `epoch_min(n)` / `epoch_hour(n)` / `epoch_day(n)` / `epoch_week(n)` | `n * 60 / 3600 / 86400 / 604800` — pure-epoch date arithmetic |

```bash
echo '{"ts":"2026-09-03T12:00:00Z"}' | kqq 'select date_part(ts, "year") as yr, to_epoch(ts) as ep'
# {"yr": 2026, "ep": 1788436800}
```

---

## Scopes

`[pattern]` before SELECT descends into nested arrays/objects. Each match
becomes a record:

```bash
# All elements of the "users" array
kqq '[users.*] select email where role = "admin"'

# Nested glob
kqq '[services.*] select name where cpu_pct > 50'
```

---

## INTO

Writes output to a file instead of stdout:

```bash
kqq "select a into '/tmp/out.json'"
```

---

## CLI flags

### Input

| Flag | Meaning |
|---|---|
| *(default)* | NDJSON input (one JSON object per line; a JSON array with one element per line also works) |
| `--flat` | Flatten nested JSON to dot-path keys before querying |
| `--buf` | Buffer entire input (disables streaming) |
| `--text`, `-T` | Each stdin line is a plain text record (field: `line`) |
| `--delim <c>` | Delimited input (e.g. `--delim ','`) |
| `--header` | First delimited row is a header |
| `--cols <names>` | Column names for delimited input, e.g. `'a,b,c'` |

JSON input is parsed strictly (RFC 8259). In NDJSON input a malformed or
truncated record — `{"a":2,,}`, a missing closing brace — is skipped and
reading resumes with the next line; kqq exits 0 and prints
`kqq: warning: N record(s) skipped (malformed JSON)`. Row-returning queries
read one record per line; aggregate and GROUP BY queries also accept records
pretty-printed over several lines (nested lines indented, as JSON
pretty-printers do) — there, a line starting with `{` in column 0 always
begins a new record. In a single JSON document (`[scope.*]` queries) a malformed or
truncated part that kqq reads is an error (exit 1); kqq stops reading at the
end of the scope's array.
`\uXXXX` escapes, including surrogate pairs, are decoded to UTF-8.

Delimited input follows RFC 4180 quoting: a field in double quotes may
contain the delimiter, newlines and doubled quotes (`"Smith, John"`,
`"say ""hi"""`). Cells that look like plain decimal numbers become numbers
(quoted or not); an empty unquoted cell is `null`, an empty quoted cell `""`.
A quote that is never closed runs to the end of the input, with a warning.
| `--llm <field>` | LLM token accumulation: extract `<field>` from each NDJSON envelope, concatenate fragments, query each completed JSON object in real time |
| `--llm-path <p>` | Dotted path for nested extraction (e.g. `choices.0.delta.content`) |
| `--api <preset>` | API protocol preset: `ollama` \| `openai` \| `anthropic` |

### Output

| Flag | Meaning |
|---|---|
| *(default)* | NDJSON: one JSON object per line (`--buf` / `--flat` instead print a single pretty-printed JSON document) |
| `--ndjson` | NDJSON (the default, made explicit) |
| `--csv` / `--tsv` | CSV / TSV with header row |
| `--raw`, `-r` | Bare values, one per line |
| `--rolling` | Emit rolling aggregate snapshot after each object |
| `--out <file>` | Write output to file |
| `--count`, `-c` | Print the number of emitted records instead of the records |

### Routing / utility

| Flag | Meaning |
|---|---|
| `--tee <file>` | Copy every input record (matches AND rejects) to file |
| `--reject <file>` | Write non-matching records to file (split routing in one pass) |
| `--split-by <f>` | Fan-out: one output file per unique value of field `f` |
| `--expect <spec>` | Schema validation, e.g. `name:string,price:number` |
| `--arg <n> <v>` | Bind `$n` to value `v` in the query |
| `--flat` | Flatten nested JSON to dot-path keys before querying |
| `--buf` | Force buffered (non-streaming) mode |
| `--help`, `-h` / `--version`, `-V` | Help / version |

---

## Query grammar (informal)

```
query      := [scope] [group_by] select clause*      -- each clause at most once, any order
            | [scope] clause*
clause     := where | group_by | having | order_by | limit | into
scope      := '[' path '.*' ']'
select     := 'select' ['distinct'] field (',' field)* ('add' field | 'remove' ident (',' ident)*)*
field      := '*'
            |  expr ('as' alias)?                     -- alias required unless expr is a field
            | ('count' | 'count' '(' ['*'] ')' | 'count' '(' expr ')') ('as' alias)?
            | ('sum'|'avg'|'min'|'max'|'stddev'|'variance') '(' expr ')' ('as' alias)?
            | 'expand' '(' path ')' ('as' alias)?
where      := 'where' condition (('and'|'or') condition)*
condition  := ident op value | ident 'between' '[' lo 'and' hi ']'
group_by   := 'group' 'by' ident
having     := 'having' condition
order_by   := 'order' 'by' (ident|number) ('asc'|'desc')?
limit      := 'limit' N
into       := 'into' string
expr       := literal | ident | ident '(' args ')' | expr binop expr
            | 'case' 'when' cond 'then' expr ('else' expr)? 'end'
```

See [Limits](limits.md) in the README for what the language
deliberately does not do.