#include "pelton/dataflow/ops/purpose.h"

#include <tuple>
#include <utility>
#include <regex>

#include "glog/logging.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace dataflow {

void PurposeOperator::ComputeOutputSchema() {
  this->output_schema_ = this->input_schemas_.at(0);
}

std::vector<Record> PurposeOperator:: Process(NodeIndex source,
                                            std::vector<Record> &&records) {
  std::vector<Record> output;
  output.reserve(records.size());
  for (Record &record : records) {
    if (this->Accept(record)) {
      output.push_back(std::move(record));
    }
  }
  return output;
}

bool PurposeOperator::Accept(const Record &record) const {
  for (const auto &operation : this->ops_) {
    switch (operation.op()) {
      case Operation::LIKE: // amrit

        if (record.schema().TypeOf(operation.left()) != Record::TypeOfVariant(operation.right())) {    \
          LOG(FATAL) << "Type mistmatch in purpose value";                \
        }                                                                 \
        
        if (record.IsNull(operation.left())) {                                         \
          return false;                                                   \
        }                                                                 \
        
        // if(!(record.GetString(operation.left()) == std::get<std::string>(operation.right()))){
        //   std::cout << "failed\n";
        //   return false;
        // }

        if(!std::regex_search(record.GetString(operation.left()), std::regex("\\b" + std::get<std::string> (operation.right()) + "\\b") )){
          return false;
        }
        break;
      case Operation::IS_NULL:
        if (!record.IsNull(operation.left())) {
          return false;
        }
        break;

      case Operation::IS_NOT_NULL:
        if (record.IsNull(operation.left())) {
          return false;
        }
        break;

      default:
        LOG(FATAL) << "Unsupported operation in purpose";
    }
  }
  return true;
}

std::unique_ptr<Operator> PurposeOperator::Clone() const {
  auto clone = std::make_unique<PurposeOperator>();
  clone->ops_ = this->ops_;
  return clone;
}

}  // namespace dataflow
}  // namespace pelton
