#include "pelton/dataflow/ops/input.h"

#include <memory>

#include "glog/logging.h"
#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/record.h"

namespace pelton {
namespace dataflow {

std::optional<std::vector<Record>> InputOperator::Process(
    NodeIndex source, const std::vector<Record> &records) {
  // Validate input records have correct schemas.
  for (const Record &record : records) {
    if (record.schema() != this->input_schemas_.at(0)) {
      LOG(FATAL) << "Input record has bad schema";
    }
  }
  return std::nullopt;
}

}  // namespace dataflow
}  // namespace pelton
