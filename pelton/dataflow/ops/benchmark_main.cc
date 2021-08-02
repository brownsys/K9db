#include "benchmark/benchmark.h"

// We want a custom main instead of using BENCHMARK_MAIN macro.
// The BENCHMARK_MAIN macro throw an error if any command line flags passed
// are unrecognizable. This causes our CI script to throw an error, as we
// pass db_username and db_password flags to all tests, even ones that don't
// use it.
// Our main function is identical to BENCHMARK_MAIN, except it does not invoke
// benchmark::ReportUnrecognizedArguments()
// https://github.com/google/benchmark/issues/320
// https://github.com/google/benchmark/pull/332
int main(int argc, char **argv) {
  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
}
