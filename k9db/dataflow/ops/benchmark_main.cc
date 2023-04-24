#define K9DB_BENCHMARK

#include <string>
#include <vector>

#include "benchmark/benchmark.h"
#include "k9db/dataflow/operator.h"
#include "k9db/dataflow/ops/benchmark_utils.h"
#include "k9db/dataflow/types.h"
#include "k9db/sqlast/ast.h"

// Utility function used by all benchmarks.
namespace k9db {
namespace dataflow {

using CType = k9db::sqlast::ColumnDefinition::Type;

SchemaRef MakeSchema(bool use_strings, bool use_pk) {
  std::vector<std::string> names = {"Col1", "Col2"};
  CType t = use_strings ? CType::TEXT : CType::UINT;
  std::vector<CType> types = {CType::UINT, t};
  std::vector<ColumnID> keys;
  if (use_pk) {
    keys.push_back(0);
  }
  return SchemaFactory::Create(names, types, keys);
}

// This function is a friend of k9db::dataflow::Operator.
void ProcessBenchmark(Operator *op, NodeIndex src, RecordGenFunc gen) {
  op->Process(src, gen(), Promise::None);
}

}  // namespace dataflow
}  // namespace k9db

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
