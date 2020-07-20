#include "dataflow/ops/identity.h"

#include <vector>

#include "dataflow/record.h"

namespace dataflow {

bool IdentityOperator::process(std::vector<Record>& rs) { return true; }

}  // namespace dataflow
