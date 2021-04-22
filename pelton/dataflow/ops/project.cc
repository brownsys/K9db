#include "pelton/dataflow/ops/project.h"

#include <algorithm>
#include <memory>
#include <tuple>
#include <utility>

#include "glog/logging.h"
#include "pelton/dataflow/record.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace dataflow {

bool ProjectOperator::EnclosedKeyCols(
    const std::vector<ColumnID> &input_keycols,
    const std::vector<ColumnID> &cids) const {
  for (const auto &keycol : input_keycols) {
    bool is_present = false;
    for (const auto &cid : cids)
      if (keycol == cid) is_present = true;

    // at least one key column of the composite key is not being projected
    if (!is_present) return false;
  }
  // all input key columns are present in the projected schema
  return true;
}

void ProjectOperator::ComputeOutputSchema() {
  std::vector<std::string> output_column_names = {};
  std::vector<sqlast::ColumnDefinition::Type> output_column_types = {};
  std::vector<ColumnID> input_keys = this->input_schemas_.at(0).keys();
  std::vector<ColumnID> output_keys = {};

  // If the input key set is a subset of the projected columns only then form an
  // output keyset. Else do not assign keys for the output schema. Because the
  // subset of input keycols can no longer uniqely identify records. This is
  // only for semantic purposes as, as of now, this does not have an effect on
  // the materialized view.
  if (EnclosedKeyCols(input_keys, cids_)) {
    for (auto ik : input_keys) {
      auto it = std::find(cids_.begin(), cids_.end(), ik);
      if (it != cids_.end()) {
        output_keys.push_back(std::distance(cids_.begin(), it));
      }
    }
  }

  // obtain column names and types
  for (const auto &cid : cids_) {
    output_column_names.push_back(this->input_schemas_.at(0).NameOf(cid));
    output_column_types.push_back(this->input_schemas_.at(0).TypeOf(cid));
  }
  owned_output_schema_ =
      SchemaOwner(output_column_names, output_column_types, output_keys);
  this->output_schema_ = SchemaRef(owned_output_schema_);
}

bool ProjectOperator::Process(NodeIndex source,
                              const std::vector<Record> &records,
                              std::vector<Record> *output) {
  for (const Record &record : records) {
    Record out_record{this->output_schema_};
    for (size_t i = 0; i < cids_.size(); i++) {
      switch (this->output_schema_.TypeOf(i)) {
        case sqlast::ColumnDefinition::Type::UINT:
          out_record.SetUInt(record.GetUInt(cids_.at(i)), i);
          break;
        case sqlast::ColumnDefinition::Type::INT:
          out_record.SetInt(record.GetInt(cids_.at(i)), i);
          break;
        case sqlast::ColumnDefinition::Type::TEXT:
          out_record.SetString(
              std::make_unique<std::string>(record.GetString(cids_.at(i))), i);
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
