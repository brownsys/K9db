def _format_java_impl(ctx):
    tool_script = None
    for f in ctx.files.tool:
        if not f.basename.endswith(".jar"):
            tool_script = f

    ctx.actions.write(
        output = ctx.outputs.executable,
        content = """
            cd ${{BUILD_WORKSPACE_DIRECTORY}}
            if [ "$1" = "CHECK" ]
            then
              FILES=$(find . | grep "k9db/planner/calcite/.*\\.java$")
              CHANGES=$(./bazel-bin/{script} -n $FILES)
              if [ -z "$CHANGES" ]
              then
                echo "java style OK!"
              else
                echo "Java style errors found in:"
                printf '%s\\n' "${{CHANGES[@]}}"
                exit 1
              fi
            else
              find . | grep "k9db/planner/calcite/.*\\.java$" | xargs ./bazel-bin/{script} -i
            fi
        """.format(
            script = tool_script.short_path,
        ),
    )

    DefaultInfo(
        runfiles = ctx.runfiles(files = ctx.files.tool),
    )

format_java = rule(
    doc = """
        Auto formats java source files according to Google's
        java style guide using google-java-format
        https://github.com/google/google-java-format
        """,
    implementation = _format_java_impl,
    executable = True,
    attrs = {
        "tool": attr.label(
            doc = """The google java format tool runnable jar.""",
            mandatory = True,
        ),
    },
)
