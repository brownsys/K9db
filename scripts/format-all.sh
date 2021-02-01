#!/usr/bin/env bash
# format all source files using clang-format

echo "Running clang-format over C++ files..."
find .. -iname *.h -o -iname *.cc | xargs clang-format -i
echo "done."