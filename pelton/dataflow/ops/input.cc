#include "pelton/dataflow/ops/input.h"

#include <utility>
#include <vector>

#include "pelton/dataflow/record.h"

namespace pelton {
namespace dataflow {

bool InputOperator::process(std::vector<Record>& rs,
                            std::vector<Record>& out_rs) {
  std::swap(rs, out_rs);
  return true;
}

}  // namespace dataflow
}  // namespace pelton
