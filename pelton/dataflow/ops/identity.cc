#include "pelton/dataflow/ops/identity.h"

#include <utility>
#include <vector>

#include "pelton/dataflow/record.h"

namespace pelton {
namespace dataflow {

bool IdentityOperator::process(std::vector<Record>& rs,
                               std::vector<Record>& out_rs) {
  std::swap(rs, out_rs);
  return true;
}

}  // namespace dataflow
}  // namespace pelton
