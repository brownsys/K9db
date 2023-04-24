#ifndef K9DB_DATAFLOW_OPS_BENCHMARK_UTILS_H_
#define K9DB_DATAFLOW_OPS_BENCHMARK_UTILS_H_

#include <functional>
#include <vector>

#include "k9db/dataflow/record.h"
#include "k9db/dataflow/schema.h"
#include "k9db/dataflow/types.h"

namespace k9db {
namespace dataflow {

class Operator;
using RecordGenFunc = std::function<std::vector<Record>()>;

SchemaRef MakeSchema(bool use_strings, bool use_pk);
void ProcessBenchmark(Operator *op, NodeIndex src, RecordGenFunc gen);

}  // namespace dataflow
}  // namespace k9db

#endif  // K9DB_DATAFLOW_OPS_BENCHMARK_UTILS_H_
