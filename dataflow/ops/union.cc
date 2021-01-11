#include "dataflow/ops/union.h"

#include <utility>
#include <vector>

#include "dataflow/record.h"

namespace dataflow {

bool UnionOperator::process(std::vector<Record>& rs,
                            std::vector<Record>& out_rs) {
  std::swap(rs, out_rs);
  return true;
}

}  // namespace dataflow
