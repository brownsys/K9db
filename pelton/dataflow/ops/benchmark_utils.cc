#include "pelton/dataflow/ops/benchmark_utils.h"

#include <string>
#include <vector>

#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace dataflow {

SchemaRef MakeSchema(bool use_strings) {
  std::vector<std::string> names = {"Col1", "Col2"};
  CType t;
  if (use_strings) {
    t = CType::TEXT;
  } else {
    t = CType::UINT;
  }
  std::vector<CType> types = {CType::UINT, t};
  std::vector<ColumnID> keys = {0};
  return SchemaFactory::Create(names, types, keys);
}

}  // namespace dataflow
}  // namespace pelton
