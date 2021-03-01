#include "pelton/dataflow/ops/project.h"

#include <tuple>

#include "glog/logging.h"
#include "pelton/dataflow/record.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace dataflow {

void ProjectOperator::ComputeOutputSchema() {
  std::vector<std::string> output_column_names = {};
  std::vector<sqlast::ColumnDefinition::Type> output_column_types = {};
  std::vector<ColumnID> output_keys = {0};

  // obtain column names and types
  for(const auto &cid : cids_){
    output_column_names.push_back(this->input_schemas_.at(0).NameOf(cid));
    output_column_types.push_back(this->input_schemas_.at(0).TypeOf(cid));
  }
  owned_output_schema_ = SchemaOwner(output_column_names, output_column_types, output_keys);
  this->output_schema_ = SchemaRef(owned_output_schema_);
}

bool ProjectOperator::Process(NodeIndex source,
                             const std::vector<Record> &records,
                             std::vector<Record> *output) {
  for (const Record &record : records) {
    Record out_record{this->output_schema_};
    for(size_t i = 0; i < cids_.size(); i++){
      switch(this->output_schema_.TypeOf(i)){
        case sqlast::ColumnDefinition::Type::UINT:
          out_record.SetUInt(record.GetUInt(cids_.at(i)), i);
          break;
        case sqlast::ColumnDefinition::Type::INT:
          out_record.SetInt(record.GetInt(cids_.at(i)), i);
          break;
        case sqlast::ColumnDefinition::Type::TEXT:
          out_record.SetString(std::make_unique<std::string>(record.GetString(cids_.at(i))), i);
          break;
        default:
          LOG(FATAL) << "Unsupported type in project";
      }
  }
  output->push_back(std::move(out_record));
  }
  return true;
}

}  // namespace dataflow
}  // namespace pelton
