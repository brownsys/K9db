#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/ops/purpose.h"

#include <utility>

#include "glog/logging.h"

namespace pelton {
namespace dataflow {

std::vector<Record> InputOperator::Process(NodeIndex source,
                                           std::vector<Record> &&records) {
  // Validate input records have correct schemas.
  for (const Record &record : records) {
    if (record.schema() != this->input_schemas_.at(0)) {
      LOG(FATAL) << "Input record has bad schema";
    }
  }

  // Enforcing gdpr purpose whenever data enters a pipeling from a table
  PurposeOperator purposeOp;
  
  // get the last column that will be gdpr
  ColumnID gdpr_col_id = this->input_schemas_.at(0).IndexOf("gdpr_purpose");
  
  purposeOp.AddOperation(this->view_name_ ,gdpr_col_id, LIKE); // amrit - add value of the gdpr purpose
  records = purposeOp.Process(source, std::move(records));

  return std::move(records);
}

std::unique_ptr<Operator> InputOperator::Clone() const {
  std::unique_ptr<InputOperator> iop = std::make_unique<InputOperator>(this->input_name_,
                                         this->input_schemas_.at(0));
  // return std::make_unique<InputOperator>(this->input_name_,
                                        //  this->input_schemas_.at(0));

  iop->view_name_ = this->view_name_;
  return iop;
}

}  // namespace dataflow
}  // namespace pelton
