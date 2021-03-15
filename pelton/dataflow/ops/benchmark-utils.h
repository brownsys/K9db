#ifndef PELTON_DATAFLOW_OPS_BENCHMARK_UTILS_H_
#define PELTON_DATAFLOW_OPS_BENCHMARK_UTILS_H_

#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace dataflow {

using CType = pelton::sqlast::ColumnDefinition::Type;

SchemaOwner MakeSchema(bool use_strings);

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_BENCHMARK_UTILS_H_
