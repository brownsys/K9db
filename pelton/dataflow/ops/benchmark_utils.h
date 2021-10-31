#ifndef PELTON_DATAFLOW_OPS_BENCHMARK_UTILS_H_
#define PELTON_DATAFLOW_OPS_BENCHMARK_UTILS_H_

#include <functional>
#include <vector>

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class Operator;
using RecordGenFunc = std::function<std::vector<Record>()>;

SchemaRef MakeSchema(bool use_strings);
void ProcessBenchmark(Operator *op, NodeIndex src, RecordGenFunc gen);

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_BENCHMARK_UTILS_H_
