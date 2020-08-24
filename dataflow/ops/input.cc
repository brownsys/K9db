#include "dataflow/ops/input.h"

#include <vector>

#include "dataflow/record.h"

namespace dataflow {

bool InputOperator::process(std::vector<Record>& rs) { return true; }

}  // namespace dataflow
