#include "pelton/dataflow/ops/union.h"

#include <utility>
#include <vector>

#include "pelton/dataflow/record.h"

namespace pelton {
namespace dataflow {

bool UnionOperator::process(std::vector<Record>& rs,
                            std::vector<Record>& out_rs) {
  std::swap(rs, out_rs);
  return true;
}

}  // namespace dataflow
}  // namespace pelton
