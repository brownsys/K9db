# Test Usage

1. Add filenames containing the schema, queries, and expected results to `/bin/data/BUILD.bazel` and to `/bin/BUILD.bazel`

2. Run `correctness.cc`, passing the files as cmd line arguments

- For example:  
   `bazel build ... && ./bin/drop.sh pelton pelton && bazel run //bin:correctness -- bin/data/medical_chat_schema.sql bin/data/medical_chat_queries.sql bin/data/medical_chat_expected.txt`
