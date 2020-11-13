#include "dataflow/ops/union.h"

#include <utility>
#include <vector>

#include "dataflow/record.h"

namespace dataflow {

bool UnionOperator::process(std::vector<Record>& rs,
                            std::vector<Record>& out_rs) {
  for (auto r : rs) {
    out_rs.push_back(std::move(r));
  };

  return true;
}

}  // namespace dataflow
