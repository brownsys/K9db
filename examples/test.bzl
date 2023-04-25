# Runs an instance of K9db proxy/server in the background,
# Then runs the given binary program against it.
# Closes server when binaries program is finished.
# Also clean the database from the filesystem before and after
# each  start.
script_template = """\
#!/bin/bash
status=0
# First clean database if instructed to.
if {clean_start}; then
  rm -rf /tmp/k9db_{db_name}
fi

echo "" > /tmp/k9db_{db_name}.log

binaries=({binaries})
for binary in "${{binaries[@]}}"
do
  echo "Test ==================================================="
  echo "Running K9db in background"

  # Run k9db in background.
  {k9db} --hostname=127.0.0.1:{port} --db_name={db_name} --logtostderr=1 \
         >> /tmp/k9db_{db_name}.log 2>&1 &
  pid=$!
  sleep 10

  # K9db ran!  
  echo "Running ${{binary}}"
  if ${{binary}}; then
    echo "PASSED ${{binary}}"
  else
    status=1
    echo "FAILED ${{binary}}"
  fi

  kill -9 ${{pid}} > /dev/null 2>/dev/null
  wait ${{pid}} > /dev/null 2>/dev/null  
  echo "==================================================== End"
done

echo "K9db logs available at: /tmp/k9db_{db_name}.log"
exit ${{status}}
"""

def _k9db_test_impl(ctx):
    clean_start = "true" if ctx.attr.clean_start else "false"

    # Path of K9db server and the binaries to run against it.
    k9db_provider = ctx.attr._k9db[DefaultInfo]
    k9db = k9db_provider.files_to_run.executable.short_path
    runfiles = k9db_provider.default_runfiles

    binaries = []
    for binary in ctx.attr.binaries:
        provider = binary[DefaultInfo]
        if not provider.files_to_run:
            fail("Binary is not executable")

        executable = provider.files_to_run.executable
        binaries.append("\"" + executable.short_path + "\"")
        runfiles = runfiles.merge(provider.default_runfiles)

    # Create test script.
    ctx.actions.write(
        output = ctx.outputs.executable,
        is_executable = True,
        content = script_template.format(
            db_name = ctx.attr.db_name,
            clean_start = clean_start,
            port = ctx.attr.port,
            k9db = k9db,
            binaries = " ".join(binaries),
        ),
    )

    return DefaultInfo(runfiles = runfiles)

k9db_test = rule(
    doc = """
        Runs a test against K9db end-2-end including proxy.
        Runs an instance of k9db proxy in the background at the given port,
        then runs the given binaries against it.
        Kills the instance when the binary finishes, also cleans the DB before
        the initial start.
        """,
    implementation = _k9db_test_impl,
    test = True,
    attrs = {
        "_k9db": attr.label(
            doc = "The K9db server binary",
            mandatory = False,
            executable = True,
            default = "//:k9db",
            cfg = "target",
        ),
        "binaries": attr.label_list(
            doc = "The binaries to run against k9db server.",
            mandatory = True,
            allow_empty = False,
            allow_files = False,
        ),
        "port": attr.int(
            doc = "port to run k9db server at",
            mandatory = True,
        ),
        "db_name": attr.string(
            doc = "the name of the database to run k9db for",
            mandatory = True,
        ),
        "clean_start": attr.bool(
            doc = "Whether to clean database before start.",
            mandatory = False,
            default = True,
        ),
    },
)

def _sql_binary(ctx):
    port = ctx.attr.port
    file = ctx.file.src

    script = "mariadb -P{port} < {file}"
    script = script.format(
        port = port,
        file = file.path,
    )

    ctx.actions.write(
        output = ctx.outputs.executable,
        is_executable = True,
        content = script,
    )

    return DefaultInfo(runfiles = ctx.runfiles(files = [file]))

sql_binary = rule(
    doc = """
        Runs an SQL file against K9db server using mariadb client.
        mariadb is assumed to be installed globally on the system.
        """,
    implementation = _sql_binary,
    executable = True,
    attrs = {
        "src": attr.label(
            doc = "The binaries to run against k9db server.",
            mandatory = True,
            allow_single_file = [".sql"],
        ),
        "port": attr.int(
            doc = "Port for corresponding K9db server.",
            mandatory = True,
        ),
    },
)

def k9db_sql_test(**kwargs):
    name = kwargs["name"]
    port = kwargs["port"]
    srcs = kwargs["srcs"]
    clean_start = kwargs.get("clean_start", True)

    # Create one sql binary rule per source file.
    sql_names = []
    for src in srcs:
        sql_name = name + "_" + src[:-4] + "_sql"
        sql_names.append(":" + sql_name)
        sql_binary(
            name = sql_name,
            src = src,
            port = port,
        )

    # Create a K9db_test rule for all of them in the given order.
    k9db_test(
        name = name,
        binaries = sql_names,
        port = port,
        db_name = name,
        clean_start = clean_start,
    )
