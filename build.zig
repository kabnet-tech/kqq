const std = @import("std");

// Although this function looks imperative, it does not perform the build
// directly and instead it mutates the build graph (`b`) that will be then
// executed by an external runner. The functions in `std.Build` implement a DSL
// for defining build steps and express dependencies between them, allowing the
// build runner to parallelize the build automatically (and the cache system to
// know when a step doesn't need to be re-run).
pub fn build(b: *std.Build) void {
    // Standard target options allow the person running `zig build` to choose
    // what target to build for. Here we do not override the defaults, which
    // means any target is allowed, and the default is native. Other options
    // for restricting supported target set are available.
    const target = b.standardTargetOptions(.{});
    // Standard optimization options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall. Here we do not
    // set a preferred release mode, allowing the user to decide how to optimize.
    const optimize = b.standardOptimizeOption(.{});
    // It's also possible to define more custom flags to toggle optional features
    // of this build script using `b.option()`. All defined flags (including
    // target and optimize options) will be listed when running `zig build --help`
    // in this directory.

    // This creates a module, which represents a collection of source files alongside
    // some compilation options, such as optimization mode and linked system libraries.
    // Zig modules are the preferred way of making Zig code available to consumers.
    // addModule defines a module that we intend to make available for importing
    // to our consumers. We must give it a name because a Zig package can expose
    // multiple modules and consumers will need to be able to specify which
    // module they want to access.
    const mod = b.addModule("zig_kq", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
    });

    const regex_mod = b.addModule("kq_regex", .{
        .root_source_file = b.path("src/core/regex.zig"),
        .target = target,
    });

    const ast_mod = b.addModule("kq_ast", .{
        .root_source_file = b.path("src/query/ast.zig"),
        .target = target,
    });

    const tokenizer_mod = b.addModule("kq_tokenizer", .{
        .root_source_file = b.path("src/query/tokenizer.zig"),
        .target = target,
    });

    const query_mod = b.addModule("kq_query", .{
        .root_source_file = b.path("src/query.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "kq_regex", .module = regex_mod },
            .{ .name = "kq_ast", .module = ast_mod },
            .{ .name = "kq_tokenizer", .module = tokenizer_mod },
        },
    });

    const record_mod = b.addModule("kq_record", .{
        .root_source_file = b.path("src/core/record.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "kq_query", .module = query_mod },
        },
    });

    // expr.zig and where.zig have a circular dependency (expr calls
    // recordPassesWhere for CASE WHEN, where calls evalExpr for computed LHS).
    // Create both modules first, then wire imports via addImport.
    const expr_mod = b.addModule("kq_expr", .{
        .root_source_file = b.path("src/core/expr.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "kq_query", .module = query_mod },
            .{ .name = "kq_record", .module = record_mod },
        },
    });

    const where_mod = b.addModule("kq_where", .{
        .root_source_file = b.path("src/core/where.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "kq_query", .module = query_mod },
            .{ .name = "kq_regex", .module = regex_mod },
            .{ .name = "kq_record", .module = record_mod },
            .{ .name = "kq_expr", .module = expr_mod },
        },
    });

    // Now add the circular import: expr needs where for CASE WHEN
    expr_mod.addImport("kq_where", where_mod);

    const output_mod = b.addModule("kq_output", .{
        .root_source_file = b.path("src/core/output.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "kq_record", .module = record_mod },
        },
    });

    const raw_where_mod = b.addModule("kq_raw_where", .{
        .root_source_file = b.path("src/core/raw_where.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "kq_query", .module = query_mod },
        },
    });

    const llm_mod = b.addModule("kq_llm", .{
        .root_source_file = b.path("src/exec/llm.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "kq_query", .module = query_mod },
            .{ .name = "kq_record", .module = record_mod },
            .{ .name = "kq_output", .module = output_mod },
        },
    });

    const stream_mod = b.addModule("kq_stream", .{
        .root_source_file = b.path("src/stream.zig"),
        .target = target,
    });

    const stream_exec_mod = b.addModule("kq_stream_exec", .{
        .root_source_file = b.path("src/stream_exec.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "kq_query", .module = query_mod },
            .{ .name = "kq_stream", .module = stream_mod },
            .{ .name = "kq_regex", .module = regex_mod },
            .{ .name = "kq_record", .module = record_mod },
            .{ .name = "kq_expr", .module = expr_mod },
            .{ .name = "kq_where", .module = where_mod },
            .{ .name = "kq_output", .module = output_mod },
            .{ .name = "kq_llm", .module = llm_mod },
            .{ .name = "kq_raw_where", .module = raw_where_mod },
        },
    });

    const record_source_mod = b.addModule("kq_record_source", .{
        .root_source_file = b.path("src/record_source.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "kq_stream_exec", .module = stream_exec_mod },
        },
    });

    const writers_mod = b.addModule("kq_writers", .{
        .root_source_file = b.path("src/cli/writers.zig"),
        .target = target,
    });

    // Here we define an executable. An executable needs to have a root module
    // which needs to expose a `main` function. While we could add a main function
    // to the module defined above, it's sometimes preferable to split business
    // logic and the CLI into two separate modules.
    //
    // If your goal is to create a Zig library for others to use, consider if
    // it might benefit from also exposing a CLI tool. A parser library for a
    // data serialization format could also bundle a CLI syntax checker, for example.
    //
    // If instead your goal is to create an executable, consider if users might
    // be interested in also being able to embed the core functionality of your
    // program in their own executable in order to avoid the overhead involved in
    // subprocessing your CLI tool.
    //
    // If neither case applies to you, feel free to delete the declaration you
    // don't need and to put everything under a single module.
    // Embed version from build.zig.zon as a comptime constant so --version
    // and the build file stay in sync without manual duplication.
    const build_options = b.addOptions();
    build_options.addOption([]const u8, "version", "0.7.0");

    const exe = b.addExecutable(.{
        .name = "kq",
        .root_module = b.createModule(.{
            // b.createModule defines a new module just like b.addModule but,
            // unlike b.addModule, it does not expose the module to consumers of
            // this package, which is why in this case we don't have to give it a name.
            .root_source_file = b.path("src/main.zig"),
            // Target and optimization levels must be explicitly wired in when
            // defining an executable or library (in the root module), and you
            // can also hardcode a specific target for an executable or library
            // definition if desireable (e.g. firmware for embedded devices).
            .target = target,
            .optimize = optimize,
            // List of modules available for import in source files part of the
            // root module.
            .imports = &.{
                .{ .name = "kq", .module = mod },
                .{ .name = "kq_query", .module = query_mod },
                .{ .name = "kq_stream", .module = stream_mod },
                .{ .name = "kq_stream_exec", .module = stream_exec_mod },
                .{ .name = "kq_record_source", .module = record_source_mod },
                .{ .name = "kq_writers", .module = writers_mod },
                .{ .name = "kq_build_options", .module = build_options.createModule() },
            },
        }),
    });

    // This declares intent for the executable to be installed into the
    // install prefix when running `zig build` (i.e. when executing the default
    // step). By default the install prefix is `zig-out/` but can be overridden
    // by passing `--prefix` or `-p`.
    b.installArtifact(exe);

    // Link libc on platforms that require it for clock_gettime.
    // Linux uses direct syscalls (std.os.linux.clock_gettime) and Windows
    // uses ntdll, so neither needs libc. macOS and other POSIX systems
    // use std.c.clock_gettime which requires linking libc.
    const target_result = target.result;
    if (target_result.os.tag != .linux and target_result.os.tag != .windows) {
        exe.root_module.link_libc = true;
    }

    // This creates a top level step. Top level steps have a name and can be
    // invoked by name when running `zig build` (e.g. `zig build run`).
    // This will evaluate the `run` step rather than the default step.
    // For a top level step to actually do something, it must depend on other
    // steps (e.g. a Run step, as we will see in a moment).
    const run_step = b.step("run", "Run the app");

    // This creates a RunArtifact step in the build graph. A RunArtifact step
    // invokes an executable compiled by Zig. Steps will only be executed by the
    // runner if invoked directly by the user (in the case of top level steps)
    // or if another step depends on it, so it's up to you to define when and
    // how this Run step will be executed. In our case we want to run it when
    // the user runs `zig build run`, so we create a dependency link.
    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);

    // By making the run step depend on the default step, it will be run from the
    // installation directory rather than directly from within the cache directory.
    run_cmd.step.dependOn(b.getInstallStep());

    // This allows the user to pass arguments to the application in the build
    // command itself, like this: `zig build run -- arg1 arg2 etc`
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    // Creates an executable that will run `test` blocks from the provided module.
    // Here `mod` needs to define a target, which is why earlier we made sure to
    // set the releative field.
    const mod_tests = b.addTest(.{
        .root_module = mod,
    });

    // A run step that will run the test executable.
    const run_mod_tests = b.addRunArtifact(mod_tests);

    // A top level step for running all tests.
    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);

    const stream_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/stream.zig"),
            .target = target,
        }),
    });
    test_step.dependOn(&b.addRunArtifact(stream_tests).step);

    const regex_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/core/regex.zig"),
            .target = target,
        }),
    });
    test_step.dependOn(&b.addRunArtifact(regex_tests).step);

    const record_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/core/record.zig"),
            .target = target,
            .imports = &.{
                .{ .name = "kq_query", .module = query_mod },
            },
        }),
    });
    test_step.dependOn(&b.addRunArtifact(record_tests).step);

    const llm_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/exec/llm.zig"),
            .target = target,
            .imports = &.{
                .{ .name = "kq_query", .module = query_mod },
                .{ .name = "kq_record", .module = record_mod },
                .{ .name = "kq_output", .module = output_mod },
            },
        }),
    });
    test_step.dependOn(&b.addRunArtifact(llm_tests).step);

    const stream_exec_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/stream_exec.zig"),
            .target = target,
            .imports = &.{
                .{ .name = "kq_query", .module = query_mod },
                .{ .name = "kq_stream", .module = stream_mod },
                .{ .name = "kq_regex", .module = regex_mod },
                .{ .name = "kq_record", .module = record_mod },
                .{ .name = "kq_expr", .module = expr_mod },
                .{ .name = "kq_where", .module = where_mod },
                .{ .name = "kq_output", .module = output_mod },
                .{ .name = "kq_llm", .module = llm_mod },
                .{ .name = "kq_raw_where", .module = raw_where_mod },
            },
        }),
    });
    test_step.dependOn(&b.addRunArtifact(stream_exec_tests).step);

    const record_source_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/record_source.zig"),
            .target = target,
            .imports = &.{
                .{ .name = "kq_stream_exec", .module = stream_exec_mod },
            },
        }),
    });
    test_step.dependOn(&b.addRunArtifact(record_source_tests).step);

    const integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/integration_tests.zig"),
            .target = target,
            .imports = &.{
                .{ .name = "kq_query", .module = query_mod },
                .{ .name = "kq_stream", .module = stream_mod },
                .{ .name = "kq_stream_exec", .module = stream_exec_mod },
                .{ .name = "kq_record_source", .module = record_source_mod },
            },
        }),
    });
    test_step.dependOn(&b.addRunArtifact(integration_tests).step);

    // Just like flags, top level steps are also listed in the `--help` menu.
    //
    // The Zig build system is entirely implemented in userland, which means
    // that it cannot hook into private compiler APIs. All compilation work
    // orchestrated by the build system will result in other Zig compiler
    // subcommands being invoked with the right flags defined. You can observe
    // these invocations when one fails (or you pass a flag to increase
    // verbosity) to validate assumptions and diagnose problems.
    //
    // Lastly, the Zig build system is relatively simple and self-contained,
    // and reading its source code will allow you to master it.
}
