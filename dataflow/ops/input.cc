#include "dataflow/ops/input.h"

#include <utility>
#include <vector>

#include "dataflow/record.h"

namespace dataflow {

bool InputOperator::process(std::vector<Record>& rs,
                            std::vector<Record>& out_rs) {
  std::swap(rs, out_rs);
  return true;
}

}  // namespace dataflow
