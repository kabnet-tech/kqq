const std = @import("std");
const kqq = @import("kqq");
const kqq_query = @import("kqq_query");
const kqq_stream_exec = @import("kqq_stream_exec");
const kqq_record_source = @import("kqq_record_source");
const kqq_writers = @import("kqq_writers");
const kqq_build_options = @import("kqq_build_options");

const NullCountWriterCtx = kqq_writers.NullCountWriterCtx;
const null_count_vtable = kqq_writers.null_count_vtable;
const TeeWriterCtx = kqq_writers.TeeWriterCtx;
const tee_vtable = kqq_writers.tee_vtable;
const sanitizeFilename = kqq_writers.sanitizeFilename;

fn die(msg: []const u8) noreturn {
    var buf: [512]u8 = undefined;
    const e = std.debug.lockStderr(&buf);
    defer std.debug.unlockStderr();
    e.file_writer.interface.print("error: {s}\n", .{msg}) catch {};
    e.file_writer.interface.flush() catch {};
    std.process.exit(1);
}

/// Print a warning to stderr (does not exit).
fn warn(comptime fmt: []const u8, args: anytype) void {
    var buf: [512]u8 = undefined;
    const e = std.debug.lockStderr(&buf);
    defer std.debug.unlockStderr();
    e.file_writer.interface.print("kqq: warning: " ++ fmt ++ "\n", args) catch {};
    e.file_writer.interface.flush() catch {};
}

/// Map a Zig executor error tag to a human-readable message and die.
fn dieErr(err: anyerror) noreturn {
    const msg: []const u8 = switch (err) {
        error.UnexpectedEof => "input ended mid-record (truncated JSON?)",
        error.BadJson => "malformed JSON in input",
        error.BadLiteral => "malformed JSON literal (expected true/false/null)",
        error.BadNumber => "malformed number in JSON input",
        error.OutOfMemory => "out of memory",
        error.NoScopeInStreamMode => "query has no [scope.*] pattern — remove or add a scope",
        error.AutoScopeNotSupportedInStreamMode => "use --buf for [*] auto-scope queries",
        error.NoGroupByKey => "GROUP BY requires at least one key",
        error.NoLlmField => "LLM mode requires --llm <field> or --llm-path <path>",
        error.NoScope => "scoped executor requires a scope pattern",
        error.InvalidSchema => "invalid --expect schema (format: name:type, ...)",
        else => @errorName(err),
    };
    die(msg);
}

fn dieQuery(query: []const u8, fail_pos: ?usize, parse_err: anyerror) noreturn {
    var buf: [2048]u8 = undefined;
    const e = std.debug.lockStderr(&buf);
    defer std.debug.unlockStderr();
    const w = &e.file_writer.interface;
    if (fail_pos) |pos| {
        const col = @min(pos, query.len);
        // Brief error summary on the first line.
        switch (parse_err) {
            error.UnexpectedChar => {
                if (col < query.len) {
                    w.print("error: unexpected character '{c}' at column {d}\n", .{ query[col], col + 1 }) catch {};
                } else {
                    w.print("error: unexpected end of query at column {d}\n", .{col + 1}) catch {};
                }
            },
            error.UnexpectedToken => {
                // Find the token text starting at col.
                var end = col;
                while (end < query.len and query[end] != ' ' and query[end] != '\t') end += 1;
                const tok_text = query[col..end];
                if (tok_text.len > 0) {
                    w.print("error: unexpected token '{s}' at column {d}\n", .{ tok_text, col + 1 }) catch {};
                } else {
                    w.print("error: unexpected end of query at column {d}\n", .{col + 1}) catch {};
                }
            },
            else => {
                w.print("error: failed to parse query (column {d})\n", .{col + 1}) catch {};
            },
        }
        // Show the query with a caret.
        w.writeAll("  ") catch {};
        w.writeAll(query) catch {};
        w.writeByte('\n') catch {};
        w.writeAll("  ") catch {};
        var ii: usize = 0;
        while (ii < col) : (ii += 1) w.writeByte(' ') catch {};
        w.writeAll("^\n") catch {};
    } else {
        w.print("error: failed to parse query: {s}\n", .{query}) catch {};
    }
    w.flush() catch {};
    std.process.exit(1);
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    // Collect argv using 0.16.0 Args.toSlice
    const args = try init.minimal.args.toSlice(init.arena.allocator());

    // Parse flags and positional args
    // Usage:
    //   kqq [--flat] [--buf] [--ndjson] ['query']
    //   --flat        : flatten JSON to dot-path keys before querying
    //   --buf         : buffer entire input (disables streaming)
    //   --ndjson      : input is newline-delimited JSON (one object per line)
    //   --raw/-r      : emit bare values (unquoted strings, one per line)
    //   --csv         : emit CSV output (with header row)
    //   --tsv         : emit TSV output (with header row)
    //   --tee <file>  : copy every input record to <file> (matches AND rejects)
    //   --reject <f>  : write non-matching records to <file>
    //   --llm <field> : LLM token accumulation mode — extract <field> from each
    //                   NDJSON envelope, concatenate fragments, detect complete
    //                   JSON objects via brace-depth tracking, query each in real-time
    //   --api <preset>: API protocol preset (ollama, openai, anthropic)
    //   --llm-path <p>: dotted path for nested field extraction (e.g. choices.0.delta.content)
    //   --expect <s>  : schema validation (e.g. name:string,price:number)
    //   --split-by <f>: fan-out: write each record to a separate file based on field value
    var flat_mode = false;
    var buf_mode = false;
    var raw_mode = false;
    var out_format = kqq_stream_exec.OutputFormat.ndjson;
    var query_src: ?[]const u8 = null;
    var tee_path: ?[]const u8 = null;
    var reject_path: ?[]const u8 = null;
    var llm_field: ?[]const u8 = null;
    var rolling_mode = false;
    var api_mode: ?kqq_stream_exec.ApiMode = null;
    var llm_path: ?[]const u8 = null;
    var expect_schema: ?[]const u8 = null;
    var text_mode = false;
    var delim_char: ?u8 = null;
    var delim_header = false;
    var delim_cols: ?[]const u8 = null;
    var split_by_field: ?[]const u8 = null;
    var out_path: ?[]const u8 = null;
    var count_mode = false;

    // --arg name value pairs: collected into a list, applied to query string before parsing
    var arg_names = std.array_list.Managed([]const u8).init(allocator);
    var arg_values = std.array_list.Managed([]const u8).init(allocator);
    defer arg_names.deinit();
    defer arg_values.deinit();
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            const help =
                \\Usage: STDIN | kqq [OPTIONS] ['<query>']
                \\
                \\Options:
                \\  --flat            Flatten nested JSON to dot-path keys
                \\  --buf             Force buffered (non-streaming) mode
                \\  --ndjson          NDJSON output: one object per line, no array wrapper
                \\  --raw, -r         Bare values, one per line (no JSON wrapper)
                \\  --csv             CSV output with header row
                \\  --tsv             TSV output with header row
                \\  --text, -T        Treat each stdin line as a plain text record
                \\  --delim <c>       Delimited input (e.g. --delim ',')
                \\  --header          First delimited row is a header
                \\  --cols <names>    Column names for delimited input (e.g. 'a,b,c')
                \\  --tee <file>      Copy all records (matches + rejects) to file
                \\  --reject <file>   Write non-matching records to file
                \\  --split-by <f>    Fan-out: one file per unique value of field f
                \\  --arg <n> <v>     Bind $n to string value v in the query
                \\  --llm <field>     LLM token accumulation mode (e.g. --llm response)
                \\  --llm-path <p>    Dotted path extraction (e.g. choices.0.delta.content)
                \\  --api <preset>    API preset: ollama | openai | anthropic
                \\  --rolling         Emit rolling aggregate snapshot after each object
                \\  --expect <spec>   Schema validation (e.g. name:string,price:number)
                \\  --help, -h        Show this help
                \\  --version, -V     Show version
                \\
                \\Query syntax:
                \\  [scope]  select <fields>  where <expr>  order by <f> asc|desc  limit N  group by <f>
                \\
                \\Examples:
                \\  cat logs.ndjson | kqq 'select name, score where active = true order by score desc limit 10'
                \\  cat data.json   | kqq '[users.*] select email where role = "admin"'
                \\  curl .../api/generate ... | kqq --api ollama 'select name where price < 100'
                \\  cat file.csv    | kqq --delim , --header 'select name, city where age > 30'
                \\
            ;
            var buf: [512]u8 = undefined;
            var w = std.Io.File.stdout().writer(io, &buf);
            w.interface.writeAll(help) catch {};
            w.interface.flush() catch {};
            std.process.exit(0);
        } else if (std.mem.eql(u8, arg, "--version") or std.mem.eql(u8, arg, "-V")) {
            var buf: [64]u8 = undefined;
            var w = std.Io.File.stdout().writer(io, &buf);
            w.interface.print("kqq {s}\n", .{kqq_build_options.version}) catch {};
            w.interface.flush() catch {};
            std.process.exit(0);
        } else if (std.mem.eql(u8, arg, "--flat")) {
            flat_mode = true;
        } else if (std.mem.eql(u8, arg, "--buf")) {
            buf_mode = true;
        } else if (std.mem.eql(u8, arg, "--ndjson")) {
            out_format = .ndjson;
        } else if (std.mem.eql(u8, arg, "--raw") or std.mem.eql(u8, arg, "-r")) {
            raw_mode = true;
            out_format = .raw;
        } else if (std.mem.eql(u8, arg, "--csv")) {
            out_format = .csv;
        } else if (std.mem.eql(u8, arg, "--tsv")) {
            out_format = .tsv;
        } else if (std.mem.eql(u8, arg, "--text") or std.mem.eql(u8, arg, "-T")) {
            text_mode = true;
        } else if (std.mem.eql(u8, arg, "--delim")) {
            i += 1;
            if (i >= args.len) die("--delim requires a delimiter character (e.g. --delim ',')");
            const d = args[i];
            if (d.len == 0) die("--delim: delimiter must be a single character");
            delim_char = d[0];
        } else if (std.mem.eql(u8, arg, "--header")) {
            delim_header = true;
        } else if (std.mem.eql(u8, arg, "--cols")) {
            i += 1;
            if (i >= args.len) die("--cols requires column names (e.g. --cols 'name,age,city')");
            delim_cols = args[i];
        } else if (std.mem.eql(u8, arg, "--tee")) {
            i += 1;
            if (i >= args.len) die("--tee requires a file path argument");
            tee_path = args[i];
        } else if (std.mem.eql(u8, arg, "--reject")) {
            i += 1;
            if (i >= args.len) die("--reject requires a file path argument");
            reject_path = args[i];
        } else if (std.mem.eql(u8, arg, "--llm")) {
            i += 1;
            if (i >= args.len) die("--llm requires a field name argument (e.g. --llm response)");
            llm_field = args[i];
        } else if (std.mem.eql(u8, arg, "--rolling")) {
            rolling_mode = true;
        } else if (std.mem.eql(u8, arg, "--api")) {
            i += 1;
            if (i >= args.len) die("--api requires a preset name (ollama, openai, anthropic)");
            const preset = args[i];
            if (std.mem.eql(u8, preset, "ollama")) {
                api_mode = .ollama;
            } else if (std.mem.eql(u8, preset, "openai")) {
                api_mode = .openai;
            } else if (std.mem.eql(u8, preset, "anthropic")) {
                api_mode = .anthropic;
            } else {
                die("--api: unknown preset (use ollama, openai, or anthropic)");
            }
        } else if (std.mem.eql(u8, arg, "--llm-path")) {
            i += 1;
            if (i >= args.len) die("--llm-path requires a dotted path (e.g. choices.0.delta.content)");
            llm_path = args[i];
        } else if (std.mem.eql(u8, arg, "--expect")) {
            i += 1;
            if (i >= args.len) die("--expect requires a schema (e.g. name:string,price:number)");
            expect_schema = args[i];
        } else if (std.mem.eql(u8, arg, "--split-by")) {
            i += 1;
            if (i >= args.len) die("--split-by requires a field name (e.g. --split-by region)");
            split_by_field = args[i];
        } else if (std.mem.eql(u8, arg, "--out")) {
            i += 1;
            if (i >= args.len) die("--out requires a file path argument");
            out_path = args[i];
        } else if (std.mem.eql(u8, arg, "--arg")) {
            i += 1;
            if (i + 1 >= args.len) die("--arg requires two arguments: --arg <name> <value>");
            const aname = args[i];
            i += 1;
            const aval = args[i];
            arg_names.append(aname) catch die("out of memory");
            arg_values.append(aval) catch die("out of memory");
        } else if (std.mem.eql(u8, arg, "--count") or std.mem.eql(u8, arg, "-c")) {
            count_mode = true;
        } else {
            query_src = arg;
        }
    }

    // ── Apply API preset defaults ──────────────────────────────────────
    // When --api is set, infer --llm field and --llm-path unless overridden.
    if (api_mode) |am| {
        if (llm_field == null and llm_path == null) {
            switch (am) {
                .ollama => {
                    llm_field = "response";
                },
                .openai => {
                    llm_path = "choices.0.delta.content";
                },
                .anthropic => {
                    llm_path = "delta.text";
                },
            }
        }
    }

    // When stdin is a terminal (not a pipe) and no LLM/text mode was requested,
    // the user almost certainly forgot to pipe data in. Print a hint and exit.
    const stdin_is_tty = std.Io.File.stdin().isTty(io) catch false;
    if (stdin_is_tty and llm_field == null and llm_path == null and api_mode == null and
        !text_mode and delim_char == null)
    {
        var buf: [256]u8 = undefined;
        var ew = std.Io.File.stderr().writer(io, &buf);
        ew.interface.writeAll("kqq: no input. Pipe data in, or use --help for usage.\n") catch {};
        ew.interface.flush() catch {};
        std.process.exit(1);
    }

    var stdout_buf: [65536]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(io, &stdout_buf);

    // ── Side-channel file writers (--tee / --reject) ───────────────────────
    // Each is a buffered file writer; we unconditionally declare storage and
    // only initialise / use it when the corresponding path was given.
    // The `interface` pointer is set to null when the flag was not provided.
    var tee_file: std.Io.File = undefined;
    var tee_fbuf: [65536]u8 = undefined;
    var tee_fw: std.Io.File.Writer = undefined;
    var tee_iface: ?*std.Io.Writer = null;

    var reject_file: std.Io.File = undefined;
    var reject_fbuf: [65536]u8 = undefined;
    var reject_fw: std.Io.File.Writer = undefined;
    var reject_iface: ?*std.Io.Writer = null;

    if (tee_path) |p| {
        tee_file = std.Io.Dir.cwd().createFile(io, p, .{}) catch die("cannot open --tee file");
        tee_fw = tee_file.writer(io, &tee_fbuf);
        tee_iface = &tee_fw.interface;
    }
    defer if (tee_iface != null) {
        tee_fw.interface.flush() catch {};
        tee_file.close(io);
    };

    if (reject_path) |p| {
        reject_file = std.Io.Dir.cwd().createFile(io, p, .{}) catch die("cannot open --reject file");
        reject_fw = reject_file.writer(io, &reject_fbuf);
        reject_iface = &reject_fw.interface;
    }
    defer if (reject_iface != null) {
        reject_fw.interface.flush() catch {};
        reject_file.close(io);
    };

    // ── --out file: tee output to both stdout and a seekable file ─────────
    // DuckDB requires a seekable file (not an anonymous pipe) for read_ndjson().
    // --out <file> writes output to both stdout and the named file so that:
    //   kqq 'filter' --ndjson --out /tmp/clean.ndjson < source.ndjson
    //   duckdb -c "SELECT ... FROM read_ndjson('/tmp/clean.ndjson')"
    var out_file: std.Io.File = undefined;
    var out_fbuf: [65536]u8 = undefined;
    var out_fw: std.Io.File.Writer = undefined;
    var tee_ctx: TeeWriterCtx = undefined;
    var tee_buf: [0]u8 = .{};
    var primary_out: *std.Io.Writer = &stdout_writer.interface;

    if (out_path) |op| {
        out_file = std.Io.Dir.cwd().createFile(io, op, .{}) catch die("cannot open --out file");
        out_fw = out_file.writer(io, &out_fbuf);
        tee_ctx = .{
            .a = &stdout_writer.interface,
            .b = &out_fw.interface,
            .writer = .{ .vtable = &tee_vtable, .buffer = &tee_buf },
        };
        primary_out = &tee_ctx.writer;
    }
    defer if (out_path != null) {
        out_fw.interface.flush() catch {};
        out_file.close(io);
    };

    // ── --count/-c: discard output, count emitted records ────────────────
    // Route primary_out to a null-count writer that counts '\n' bytes (one per
    // NDJSON record). After all execution paths return we print the count.
    var nc_buf: [0]u8 = .{};
    var nc_ctx = NullCountWriterCtx{
        .writer = .{ .vtable = &null_count_vtable, .buffer = &nc_buf },
    };
    if (count_mode) primary_out = &nc_ctx.writer;
    defer if (count_mode) {
        var cnt_buf: [32]u8 = undefined;
        const cnt_str = std.fmt.bufPrint(&cnt_buf, "{d}\n", .{nc_ctx.count}) catch "0\n";
        stdout_writer.interface.writeAll(cnt_str) catch {};
        stdout_writer.interface.flush() catch {};
    };

    // ── Apply $ENV and --arg substitutions ───────────────────────────────
    // Replace $ENV.VARNAME and $name tokens with their values before parsing.
    //   $ENV.HOME       → reads HOME from the process environment
    //   $ENV.PRICE      → reads PRICE from the process environment
    //   --arg dept "x"  → $dept becomes "x"
    // Numeric env values are emitted unquoted so comparisons work directly:
    //   where price < $ENV.MAX_PRICE   (MAX_PRICE=100 → `where price < 100`)
    // String values are emitted quoted.
    const has_dollar = query_src != null and std.mem.indexOfScalar(u8, query_src.?, '$') != null;
    if ((arg_names.items.len > 0 or has_dollar) and query_src != null) {
        var q_buf = std.array_list.Managed(u8).init(allocator);
        defer q_buf.deinit();
        const src = query_src.?;
        var pos: usize = 0;
        while (pos < src.len) {
            if (src[pos] == '$') {
                // Find the end of the variable name (alphanumeric + _)
                var end = pos + 1;
                while (end < src.len and (std.ascii.isAlphanumeric(src[end]) or src[end] == '_')) : (end += 1) {}
                const vname = src[pos + 1 .. end];

                // ── $ENV.VARNAME ──────────────────────────────────────────
                if (std.mem.eql(u8, vname, "ENV") and end < src.len and src[end] == '.') {
                    // Consume the dot, then read the env var name
                    var env_end = end + 1;
                    while (env_end < src.len and (std.ascii.isAlphanumeric(src[env_end]) or src[env_end] == '_')) : (env_end += 1) {}
                    const env_key = src[end + 1 .. env_end];
                    const env_val = init.environ_map.get(env_key);
                    if (env_val) |env_v| {
                        // Emit unquoted if the value is a bare number, quoted otherwise
                        const is_number = blk: {
                            if (env_v.len == 0) break :blk false;
                            _ = std.fmt.parseFloat(f64, env_v) catch break :blk false;
                            break :blk true;
                        };
                        if (is_number) {
                            q_buf.appendSlice(env_v) catch die("out of memory");
                        } else {
                            q_buf.append('"') catch die("out of memory");
                            for (env_v) |c| {
                                if (c == '"') q_buf.appendSlice("\\\"") catch die("out of memory") else if (c == '\\') q_buf.appendSlice("\\\\") catch die("out of memory") else q_buf.append(c) catch die("out of memory");
                            }
                            q_buf.append('"') catch die("out of memory");
                        }
                    } else {
                        // Env var not set — emit null so the query sees a typed null
                        q_buf.appendSlice("null") catch die("out of memory");
                    }
                    pos = env_end;
                    continue;
                }

                // ── --arg $name ───────────────────────────────────────────
                var found = false;
                for (arg_names.items, 0..) |n, idx| {
                    if (std.mem.eql(u8, n, vname)) {
                        const v = arg_values.items[idx];
                        // If the value looks like a bare number, emit it unquoted so
                        // numeric comparisons (>, <, =) work. Otherwise emit quoted.
                        const is_num = v.len > 0 and (std.fmt.parseFloat(f64, v) catch null) != null;
                        if (is_num) {
                            q_buf.appendSlice(v) catch die("out of memory");
                        } else {
                            q_buf.append('"') catch die("out of memory");
                            for (v) |c| {
                                if (c == '"') q_buf.appendSlice("\\\"") catch die("out of memory") else if (c == '\\') q_buf.appendSlice("\\\\") catch die("out of memory") else q_buf.append(c) catch die("out of memory");
                            }
                            q_buf.append('"') catch die("out of memory");
                        }
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    // Unknown variable — emit as-is so the parser gives a useful error
                    q_buf.appendSlice(src[pos..end]) catch die("out of memory");
                }
                pos = end;
            } else {
                q_buf.append(src[pos]) catch die("out of memory");
                pos += 1;
            }
        }
        query_src = q_buf.toOwnedSlice() catch die("out of memory");
    }

    // ── MULTI-QUERY INTO PATH ─────────────────────────────────────────────
    // When the query string contains ';', parse as multiple queries.
    // Each query may have an INTO 'path' clause; those without go to stdout.
    // Input is buffered once, then each query runs over the same data.
    if (query_src) |qs| {
        if (std.mem.indexOfScalar(u8, qs, ';') != null) {
            const queries = kqq_query.parseMulti(allocator, qs) catch {
                die("failed to parse multi-query");
            };
            defer {
                for (queries) |q| q.deinit(allocator);
                allocator.free(queries);
            }
            if (queries.len == 0) die("empty multi-query");

            // Buffer the entire input once
            var stdin_rbuf0: [65536]u8 = undefined;
            var stdin_r0 = std.Io.File.stdin().reader(io, &stdin_rbuf0);
            const input_data = stdin_r0.interface.allocRemaining(allocator, .limited(256 * 1024 * 1024)) catch die("failed to read stdin");
            defer allocator.free(input_data);

            for (queries) |q| {
                // Determine writer: INTO file or stdout
                var into_file: std.Io.File = undefined;
                var into_fbuf: [65536]u8 = undefined;
                var into_fw: std.Io.File.Writer = undefined;
                var use_into = false;
                if (q.into) |into_path| {
                    into_file = std.Io.Dir.cwd().createFile(io, into_path, .{}) catch die("cannot open INTO file");
                    into_fw = into_file.writer(io, &into_fbuf);
                    use_into = true;
                }
                defer if (use_into) {
                    into_fw.interface.flush() catch {};
                    into_file.close(io);
                };

                const out_writer: *std.Io.Writer = if (use_into)
                    &into_fw.interface
                else
                    primary_out;

                const opts = kqq_stream_exec.ExecOptions{
                    .raw = raw_mode,
                    .format = out_format,
                    .tee_writer = tee_iface,
                    .reject_writer = reject_iface,
                };

                // Create a fresh reader over the same buffered data
                var sub_reader = std.Io.Reader.fixed(input_data);

                if (q.group_by != null) {
                    _ = kqq_stream_exec.execGroupByNDJSON(
                        allocator,
                        &sub_reader,
                        q,
                        out_writer,
                        opts,
                    ) catch {
                        continue;
                    };
                } else if (q.global_agg) {
                    _ = kqq_stream_exec.execGlobalAggNDJSON(
                        allocator,
                        &sub_reader,
                        q,
                        out_writer,
                        opts,
                    ) catch {
                        continue;
                    };
                } else {
                    _ = kqq_stream_exec.execStreamNDJSON(
                        allocator,
                        &sub_reader,
                        q,
                        out_writer,
                        opts,
                    ) catch {
                        continue;
                    };
                }
            }
            try primary_out.flush();
            return;
        }
    }

    // ── SPLIT-BY PATH ─────────────────────────────────────────────────────
    // When --split-by <field> is set, run the normal query against stdin,
    // capture the NDJSON output, then fan out each output line to a file
    // named <value>.jsonl based on the split field in that line.
    if (split_by_field) |split_field| {
        const qs = query_src orelse "select *";
        var fp1: usize = 0;
        const q = kqq_query.parse(allocator, qs, &fp1) catch |err| {
            dieQuery(qs, fp1, err);
        };
        defer q.deinit(allocator);

        // Buffer all stdin
        var stdin_rbuf1: [65536]u8 = undefined;
        var stdin_r1 = std.Io.File.stdin().reader(io, &stdin_rbuf1);
        const input_data = stdin_r1.interface.allocRemaining(allocator, .limited(256 * 1024 * 1024)) catch die("failed to read stdin");
        defer allocator.free(input_data);

        // Run the query into a memory buffer
        var mem_writer = std.Io.Writer.Allocating.init(allocator);
        defer mem_writer.deinit();

        const opts = kqq_stream_exec.ExecOptions{
            .raw = false,
            .format = .json,
            .tee_writer = tee_iface,
            .reject_writer = reject_iface,
        };

        var sub_reader = std.Io.Reader.fixed(input_data);
        if (q.group_by != null) {
            _ = kqq_stream_exec.execGroupByNDJSON(allocator, &sub_reader, q, &mem_writer.writer, opts) catch {};
        } else if (q.global_agg) {
            _ = kqq_stream_exec.execGlobalAggNDJSON(allocator, &sub_reader, q, &mem_writer.writer, opts) catch {};
        } else {
            _ = kqq_stream_exec.execStreamNDJSON(allocator, &sub_reader, q, &mem_writer.writer, opts) catch {};
        }

        // Now split the output by the field value.
        // The executor output is a JSON array: [{...}, {...}, ...]
        // Parse it and extract the split field from each element.
        const output = mem_writer.toOwnedSlice() catch die("out of memory");
        defer allocator.free(output);

        const parsed = std.json.parseFromSlice(std.json.Value, allocator, output, .{}) catch {
            // If output is empty or not valid JSON, nothing to split
            try primary_out.writeAll("[]\n");
            try primary_out.flush();
            return;
        };
        defer parsed.deinit();

        if (parsed.value != .array) {
            try primary_out.writeAll("[]\n");
            try primary_out.flush();
            return;
        }

        // Map: field_value_string → list of JSON elements (as serialized strings)
        var split_map = std.StringHashMap(std.array_list.Managed([]const u8)).init(allocator);
        defer {
            var sit = split_map.iterator();
            while (sit.next()) |e| {
                for (e.value_ptr.items) |s| allocator.free(s);
                e.value_ptr.deinit();
                allocator.free(e.key_ptr.*);
            }
            split_map.deinit();
        }

        for (parsed.value.array.items) |elem| {
            if (elem != .object) continue;
            const obj = elem.object;

            // Extract the split field value as a string
            const val = obj.get(split_field) orelse continue;
            var key_str_buf: [256]u8 = undefined;
            const key_str: []const u8 = switch (val) {
                .string => |s| s,
                .integer => |n| std.fmt.bufPrint(&key_str_buf, "{d}", .{n}) catch continue,
                .float => |f| std.fmt.bufPrint(&key_str_buf, "{d}", .{f}) catch continue,
                .bool => |b| if (b) "true" else "false",
                .null => "null",
                else => continue,
            };

            // Serialize this element as a single JSON line
            var aw = std.Io.Writer.Allocating.init(allocator);
            if (std.json.Stringify.value(elem, .{}, &aw.writer)) {} else |_| {
                aw.deinit();
                continue;
            }
            const line_owned = aw.toOwnedSlice() catch {
                aw.deinit();
                continue;
            };

            const gop = split_map.getOrPut(key_str) catch {
                allocator.free(line_owned);
                continue;
            };
            if (!gop.found_existing) {
                gop.key_ptr.* = allocator.dupe(u8, key_str) catch {
                    allocator.free(line_owned);
                    continue;
                };
                gop.value_ptr.* = std.array_list.Managed([]const u8).init(allocator);
            }
            gop.value_ptr.append(line_owned) catch {
                allocator.free(line_owned);
                continue;
            };
        }

        // Write each bucket to <value>.jsonl
        var sit = split_map.iterator();
        while (sit.next()) |e| {
            var fname_buf: [512]u8 = undefined;
            const safe_key = sanitizeFilename(e.key_ptr.*, &fname_buf);
            const filename = std.fmt.allocPrint(allocator, "{s}.jsonl", .{safe_key}) catch continue;
            defer allocator.free(filename);
            const file = std.Io.Dir.cwd().createFile(io, filename, .{}) catch continue;
            var file_fbuf: [65536]u8 = undefined;
            var file_fw = file.writer(io, &file_fbuf);
            for (e.value_ptr.items) |json_line| {
                file_fw.interface.writeAll(json_line) catch continue;
                file_fw.interface.writeByte('\n') catch continue;
            }
            file_fw.interface.flush() catch {};
            file.close(io);
        }

        // Summary to stdout
        try primary_out.writeAll("[\n");
        var sit2 = split_map.iterator();
        var first_entry = true;
        while (sit2.next()) |e| {
            if (!first_entry) try primary_out.writeAll(",\n");
            first_entry = false;
            var fname_buf: [512]u8 = undefined;
            const safe_key = sanitizeFilename(e.key_ptr.*, &fname_buf);
            try primary_out.writeAll("  {\"");
            try primary_out.writeAll(split_field);
            try primary_out.writeAll("\": \"");
            try primary_out.writeAll(e.key_ptr.*);
            try primary_out.writeAll("\", \"file\": \"");
            try primary_out.writeAll(safe_key);
            try primary_out.writeAll(".jsonl\", \"count\": ");
            var cnt_buf: [32]u8 = undefined;
            const cnt_str = std.fmt.bufPrint(&cnt_buf, "{d}", .{e.value_ptr.items.len}) catch "0";
            try primary_out.writeAll(cnt_str);
            try primary_out.writeByte('}');
        }
        try primary_out.writeAll("\n]\n");
        try primary_out.flush();
        return;
    }

    // ── TEXT / DELIMITED PATH ─────────────────────────────────────────────
    // When --text or --delim is set, read stdin as plain text or delimited
    // lines and route through the format-agnostic generic executors.
    if (text_mode or delim_char != null) {
        const qs = query_src orelse "select *";
        var fp2: usize = 0;
        const q = kqq_query.parse(allocator, qs, &fp2) catch |err| {
            dieQuery(qs, fp2, err);
        };
        defer q.deinit(allocator);

        const opts = kqq_stream_exec.ExecOptions{
            .raw = raw_mode,
            .format = out_format,
            .tee_writer = tee_iface,
            .reject_writer = reject_iface,
        };

        var stdin_rbuf: [65536]u8 = undefined;
        var stdin_file_reader = std.Io.File.stdin().reader(io, &stdin_rbuf);

        if (text_mode) {
            // Plain text mode: each line → {line: "...", _n: N}
            var src = kqq_record_source.TextLineSource.init(
                allocator,
                &stdin_file_reader.interface,
            );
            defer src.deinit();

            if (q.group_by != null) {
                _ = kqq_stream_exec.execGenericGroupBy(allocator, &src, q, primary_out, opts) catch |err| dieErr(err);
            } else if (q.global_agg) {
                _ = kqq_stream_exec.execGenericGlobalAgg(allocator, &src, q, primary_out, opts) catch |err| dieErr(err);
            } else {
                _ = kqq_stream_exec.execGenericSource(allocator, &src, q, primary_out, opts) catch |err| dieErr(err);
            }
        } else {
            // Delimited mode: parse --cols into slice if provided
            var col_buf: [256][]const u8 = undefined;
            var n_cols: usize = 0;
            if (delim_cols) |cols_str| {
                var col_it = std.mem.splitScalar(u8, cols_str, ',');
                while (col_it.next()) |c| {
                    if (n_cols >= col_buf.len) break;
                    col_buf[n_cols] = std.mem.trim(u8, c, " ");
                    n_cols += 1;
                }
            }
            const explicit: ?[]const []const u8 = if (n_cols > 0) col_buf[0..n_cols] else null;

            var src = kqq_record_source.DelimitedSource.init(
                allocator,
                &stdin_file_reader.interface,
                delim_char.?,
                delim_header,
                explicit,
            );
            defer src.deinit();

            if (q.group_by != null) {
                _ = kqq_stream_exec.execGenericGroupBy(allocator, &src, q, primary_out, opts) catch |err| dieErr(err);
            } else if (q.global_agg) {
                _ = kqq_stream_exec.execGenericGlobalAgg(allocator, &src, q, primary_out, opts) catch |err| dieErr(err);
            } else {
                _ = kqq_stream_exec.execGenericSource(allocator, &src, q, primary_out, opts) catch |err| dieErr(err);
            }
        }
        try primary_out.flush();
        return;
    }

    // ── LLM TOKEN ACCUMULATION PATH ──────────────────────────────────────
    // When --llm <field> or --llm-path <path> or --api <preset> is set,
    // accumulate token fragments from the named field in each NDJSON/SSE
    // envelope, detect complete JSON objects via brace depth tracking,
    // and query each in real-time.
    if (llm_field != null or llm_path != null) {
        const qs = query_src orelse die("--llm/--llm-path requires a query argument");
        var fp3: usize = 0;
        const q = kqq_query.parse(allocator, qs, &fp3) catch |err| {
            dieQuery(qs, fp3, err);
        };
        defer q.deinit(allocator);

        // Parse --expect schema if provided
        const schema = if (expect_schema) |es|
            (kqq_stream_exec.parseExpectSchema(allocator, es) catch die("invalid --expect schema"))
        else
            null;
        defer if (schema) |s| allocator.free(s);

        const opts = kqq_stream_exec.ExecOptions{
            .raw = raw_mode,
            .format = out_format,
            .tee_writer = tee_iface,
            .reject_writer = reject_iface,
            .llm_field = llm_field,
            .llm_path = llm_path,
            .api_mode = api_mode,
            .rolling = rolling_mode,
            .expect_schema = schema,
        };
        var stdin_rbuf: [65536]u8 = undefined;
        var stdin_file_reader = std.Io.File.stdin().reader(io, &stdin_rbuf);

        if (q.group_by != null) {
            _ = kqq_stream_exec.execLlmGroupBy(
                allocator,
                &stdin_file_reader.interface,
                q,
                primary_out,
                opts,
            ) catch |err| dieErr(err);
        } else if (q.global_agg) {
            _ = kqq_stream_exec.execLlmGlobalAgg(
                allocator,
                &stdin_file_reader.interface,
                q,
                primary_out,
                opts,
            ) catch |err| dieErr(err);
        } else {
            _ = kqq_stream_exec.execLlmStream(
                allocator,
                &stdin_file_reader.interface,
                q,
                primary_out,
                opts,
            ) catch |err| dieErr(err);
        }
        try primary_out.flush();
        return;
    }

    // ── NDJSON PATH ────────────────────────────────────────────────────────
    // No scope in query (no [bracket]) implies NDJSON.
    if (!flat_mode and !buf_mode) {
        if (query_src) |qs| {
            var fp4: usize = 0;
            const q = kqq_query.parse(allocator, qs, &fp4) catch |err| {
                dieQuery(qs, fp4, err);
            };
            defer q.deinit(allocator);
            // Use NDJSON mode when: no scope pattern in query
            const is_ndjson = q.scope_pattern == null;
            if (is_ndjson) {
                var skip_count: usize = 0;
                const opts = kqq_stream_exec.ExecOptions{
                    .raw = raw_mode,
                    .format = out_format,
                    .tee_writer = tee_iface,
                    .reject_writer = reject_iface,
                    .skipped_records = &skip_count,
                };
                var stdin_rbuf: [65536]u8 = undefined;
                var stdin_file_reader = std.Io.File.stdin().reader(io, &stdin_rbuf);

                // INTO support: route output to file if query has INTO clause
                var into_file: std.Io.File = undefined;
                var into_fbuf: [65536]u8 = undefined;
                var into_fw: std.Io.File.Writer = undefined;
                var use_into = false;
                if (q.into) |into_path| {
                    into_file = std.Io.Dir.cwd().createFile(io, into_path, .{}) catch die("cannot open INTO file");
                    into_fw = into_file.writer(io, &into_fbuf);
                    use_into = true;
                }
                defer if (use_into) {
                    into_fw.interface.flush() catch {};
                    into_file.close(io);
                };
                const out_writer: *std.Io.Writer = if (use_into) &into_fw.interface else primary_out;

                // Route to appropriate executor based on query type
                if (q.group_by != null) {
                    _ = kqq_stream_exec.execGroupByNDJSON(
                        allocator,
                        &stdin_file_reader.interface,
                        q,
                        out_writer,
                        opts,
                    ) catch |err| dieErr(err);
                } else if (q.global_agg) {
                    _ = kqq_stream_exec.execGlobalAggNDJSON(
                        allocator,
                        &stdin_file_reader.interface,
                        q,
                        out_writer,
                        opts,
                    ) catch |err| dieErr(err);
                } else {
                    _ = kqq_stream_exec.execStreamNDJSON(
                        allocator,
                        &stdin_file_reader.interface,
                        q,
                        out_writer,
                        opts,
                    ) catch |err| dieErr(err);
                }
                try primary_out.flush();
                if (skip_count > 0) warn("{d} record(s) skipped (malformed JSON)", .{skip_count});
                return;
            }
        } else {
            // No query: passthrough all records as array
            var stdin_rbuf: [65536]u8 = undefined;
            var stdin_file_reader = std.Io.File.stdin().reader(io, &stdin_rbuf);
            const empty_q = kqq_query.Query{
                .scope_pattern = null,
                .fields = null,
                .where = null,
                .order_by = null,
                .limit = null,
                .group_by = null,
                .has_count = false,
                .global_agg = false,
                .distinct = false,
            };
            _ = kqq_stream_exec.execStreamNDJSON(
                allocator,
                &stdin_file_reader.interface,
                empty_q,
                primary_out,
                .{},
            ) catch |err| dieErr(err);
            try primary_out.flush();
            return;
        }
    }

    // ── STREAMING PATH (default for scoped queries) ────────────────────────
    // When we have a scoped query ([arr.*] ...) and NOT --flat / --buf,
    // run the streaming executor directly on stdin — never buffers the input.
    if (!flat_mode and !buf_mode) {
        if (query_src) |qs| {
            var fp5: usize = 0;
            const q = kqq_query.parse(allocator, qs, &fp5) catch |err| {
                dieQuery(qs, fp5, err);
            };
            defer q.deinit(allocator);

            if (q.scope_pattern != null and !std.mem.eql(u8, q.scope_pattern.?, "*")) {
                // Streaming scoped mode
                var stdin_rbuf: [65536]u8 = undefined;
                var stdin_file_reader = std.Io.File.stdin().reader(io, &stdin_rbuf);
                const opts = kqq_stream_exec.ExecOptions{ .raw = raw_mode, .format = out_format };
                // Route group-by / global-agg queries to appropriate executor
                _ = if (q.group_by != null)
                    kqq_stream_exec.execGroupByStream(
                        allocator,
                        &stdin_file_reader.interface,
                        q,
                        primary_out,
                        opts,
                    ) catch |err| dieErr(err)
                else if (q.global_agg)
                    kqq_stream_exec.execGlobalAggStream(
                        allocator,
                        &stdin_file_reader.interface,
                        q,
                        primary_out,
                        opts,
                    ) catch |err| dieErr(err)
                else
                    kqq_stream_exec.execStream(
                        allocator,
                        &stdin_file_reader.interface,
                        q,
                        primary_out,
                        opts,
                    ) catch |err| dieErr(err);
                try primary_out.flush();
                return;
            }
        }
    }

    // ── BUFFERED PATHS ─────────────────────────────────────────────────────
    // Read all of stdin (256 MiB limit)
    var stdin_rbuf2: [65536]u8 = undefined;
    var stdin_r2 = std.Io.File.stdin().reader(io, &stdin_rbuf2);
    const input = try stdin_r2.interface.allocRemaining(allocator, .limited(256 * 1024 * 1024));
    defer allocator.free(input);

    // Parse JSON
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, input, .{}) catch {
        die("failed to parse JSON");
    };
    defer parsed.deinit();

    if (flat_mode) {
        // ── FLAT MODE ──────────────────────────────────────────────────────
        var flat: std.json.ObjectMap = .{};
        defer {
            var it = flat.iterator();
            while (it.next()) |e| allocator.free(e.key_ptr.*);
            flat.deinit(allocator);
        }
        try kqq.flatten(allocator, parsed.value, "", &flat);

        var result: std.json.ObjectMap = undefined;
        var result_owned = false;
        if (query_src) |qs| {
            var fp6: usize = 0;
            const query = kqq_query.parse(allocator, qs, &fp6) catch |err| {
                dieQuery(qs, fp6, err);
            };
            defer query.deinit(allocator);
            result = try kqq_query.execQuery(allocator, flat, query);
            result_owned = true;
        } else {
            result = flat;
        }
        defer if (result_owned) {
            var it = result.iterator();
            while (it.next()) |e| allocator.free(e.key_ptr.*);
            result.deinit(allocator);
        };
        const out_value = std.json.Value{ .object = result };
        try std.json.Stringify.value(out_value, .{ .whitespace = .indent_2 }, primary_out);
    } else {
        // ── NATIVE BUFFERED MODE ───────────────────────────────────────────
        if (query_src) |qs| {
            var fp7: usize = 0;
            const query = kqq_query.parse(allocator, qs, &fp7) catch |err| {
                dieQuery(qs, fp7, err);
            };
            defer query.deinit(allocator);
            const result = try kqq_query.execQueryNative(allocator, parsed.value, query);
            const result_is_new = query.scope_pattern != null;
            defer if (result_is_new) kqq_query.freeValue(allocator, result);
            try std.json.Stringify.value(result, .{ .whitespace = .indent_2 }, primary_out);
        } else {
            try std.json.Stringify.value(parsed.value, .{ .whitespace = .indent_2 }, primary_out);
        }
    }

    try primary_out.writeByte('\n');
    try primary_out.flush();
}
