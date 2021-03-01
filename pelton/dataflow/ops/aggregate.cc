#include "pelton/dataflow/ops/aggregate.h"

#include <tuple>

#include "glog/logging.h"
#include "pelton/dataflow/record.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace dataflow {

void AggregateOperator::ComputeOutputSchema() {
  std::vector<std::string> output_column_names = {};
  std::vector<sqlast::ColumnDefinition::Type> output_column_types = {};
  std::vector<ColumnID> key_columns = {0};

  std::vector<std::string> aggregate_column_name{
      this->input_schemas_.at(0).NameOf(aggregate_column_)};
  std::vector<sqlast::ColumnDefinition::Type> aggregate_column_type;
  // The calcite planner does not supply a column id for count function, hence
  // be independent of it
  if (aggregate_function_ == Function::COUNT)
    aggregate_column_type.push_back(sqlast::ColumnDefinition::Type::UINT);
  else
    aggregate_column_type.push_back(
        this->input_schemas_.at(0).TypeOf(aggregate_column_));

  // obtain column names and types
  for (const auto &cid : group_columns_) {
    output_column_names.push_back(this->input_schemas_.at(0).NameOf(cid));
    output_column_types.push_back(this->input_schemas_.at(0).TypeOf(cid));
  }
  // add another column for the aggregate
  output_column_names.push_back(aggregate_column_name.at(0));
  output_column_types.push_back(aggregate_column_type.at(0));

  owned_output_schema_ =
      SchemaOwner(output_column_names, output_column_types, key_columns);
  aggregate_schema_ =
      SchemaOwner(aggregate_column_name, aggregate_column_type, key_columns);
  aggregate_schema_ref_ = SchemaRef(aggregate_schema_);
  this->output_schema_ = SchemaRef(owned_output_schema_);
}

std::vector<Key> AggregateOperator::GenerateKey(const Record &record) const {
  std::vector<Key> key;
  for (auto i : group_columns_) key.push_back(record.GetValue(i));
  return key;
}

void AggregateOperator::EmitRecord(const std::vector<Key> &key,
                                   const Record &aggregate_record,
                                   bool positive,
                                   std::vector<Record> *output) const {
  // create record
  Record output_record(this->output_schema_, positive);
  for (size_t i = 0; i < key.size(); i++) {
    switch (this->output_schema_.TypeOf(i)) {
      case sqlast::ColumnDefinition::Type::UINT:
        output_record.SetUInt(key.at(i).GetUInt(), i);
        break;
      case sqlast::ColumnDefinition::Type::INT:
        output_record.SetInt(key.at(i).GetInt(), i);
        break;
      case sqlast::ColumnDefinition::Type::TEXT:
        // records have to own their pointed data
        output_record.SetString(
            std::make_unique<std::string>(key.at(i).GetString()), i);
        break;
      default:
        LOG(FATAL) << "Unsupported type when forming output record";
    }
  }

  // Add aggregate value to the last column
  size_t aggregate_index = this->output_schema_.size() - 1;
  switch (aggregate_schema_ref_.TypeOf(0)) {
    case sqlast::ColumnDefinition::Type::UINT:
      output_record.SetUInt(aggregate_record.GetUInt(0), aggregate_index);
      break;
    case sqlast::ColumnDefinition::Type::INT:
      output_record.SetInt(aggregate_record.GetInt(0), aggregate_index);
      break;
    default:
      // Aggregate value does not support Type::TEXT
      LOG(FATAL) << "Unsupported type when forming output record";
  }

  // emit record
  output->push_back(std::move(output_record));
}

// This class is used to track state changes when processing given batch of
// input records
class StateChange {
 public:
  StateChange(bool is_insert, const SchemaRef &aggregate_schema)
      : is_insert_(is_insert), old_value_(Record(aggregate_schema, true)) {}
  bool is_insert_;
  Record old_value_;
};

bool AggregateOperator::Process(NodeIndex source,
                                const std::vector<Record> &records,
                                std::vector<Record> *output) {
  absl::flat_hash_map<std::vector<Key>, StateChange> first_delta;

  for (const Record &record : records) {
    const std::vector<Key> key = GenerateKey(record);
    switch (aggregate_function_) {
      case Function::COUNT:
        /*
         * For negative records value in state is decremented by 1 and for
         * positive records value in state is incremented by 1
         */
        if (!record.IsPositive()) {
          if (!state_.contains(key)) {  // handle negative record
            LOG(FATAL)
                << "State does not exist for corresponding negative record";
          }
          if (!first_delta.contains(key)) {  // track first state change of key
            first_delta.emplace(key, StateChange(false, aggregate_schema_ref_));
            // need to explicitly set value since records cannot be copied
            first_delta.at(key).old_value_.SetUInt(state_.at(key).GetUInt(0),
                                                   0);
          }
          state_.at(key).SetUInt(state_.at(key).GetUInt(0) - 1, 0);
        } else {  // handle positive record
          if (!state_.contains(key)) {
            // track first state change of key. old_value_ does not exist.
            first_delta.emplace(key, StateChange(true, aggregate_schema_ref_));
            // new aggregate record
            Record aggregate_record(aggregate_schema_ref_);
            aggregate_record.SetUInt(1ULL, 0);
            state_.emplace(key, std::move(aggregate_record));
          } else {
            if (!first_delta.contains(
                    key)) {  // track first state change of key
              first_delta.emplace(key,
                                  StateChange(false, aggregate_schema_ref_));
              first_delta.emplace(key,
                                  StateChange(false, aggregate_schema_ref_));
              // need to explicitly set value since records cannot be copied
              first_delta.at(key).old_value_.SetUInt(state_.at(key).GetUInt(0),
                                                     0);
            }
            state_.at(key).SetUInt(state_.at(key).GetUInt(0) + 1, 0);
          }
        }
        break;
      case Function::SUM:
        /*
         * For negative records value in state is subtracted by value in @record
         * and for positive records value in state is incremented by value in
         * @record.
         */
        if (!record.IsPositive()) {  // handle negative record
          if (!state_.contains(key)) {
            LOG(FATAL)
                << "State does not exist for corresponding negative record";
          }
          if (!first_delta.contains(key)) {  // track first state change of key
            first_delta.emplace(key, StateChange(false, aggregate_schema_ref_));
            // need to explicitly set value since records cannot be copied
            switch (aggregate_schema_ref_.TypeOf(0)) {
              case sqlast::ColumnDefinition::Type::UINT:
                first_delta.at(key).old_value_.SetUInt(
                    state_.at(key).GetUInt(0), 0);
                break;
              case sqlast::ColumnDefinition::Type::INT:
                first_delta.at(key).old_value_.SetInt(state_.at(key).GetInt(0),
                                                      0);
                break;
              default:
                LOG(FATAL) << "Unsupported type when computing SUM aggregate";
            }
          }
          switch (aggregate_schema_ref_.TypeOf(0)) {
            case sqlast::ColumnDefinition::Type::UINT:
              state_.at(key).SetUInt(
                  state_.at(key).GetUInt(0) - record.GetUInt(aggregate_column_),
                  0);
              break;
            case sqlast::ColumnDefinition::Type::INT:
              state_.at(key).SetInt(
                  state_.at(key).GetInt(0) - record.GetInt(aggregate_column_),
                  0);
              break;
            default:
              LOG(FATAL) << "Unsupported type when computing SUM aggregate";
          }
        } else {  // handle positive record
          if (!state_.contains(key)) {
            // track first state change of key. old_value_ does not exist.
            first_delta.emplace(key, StateChange(true, aggregate_schema_ref_));
            // new aggregate record
            Record aggregate_record(aggregate_schema_ref_);
            switch (aggregate_schema_ref_.TypeOf(0)) {
              case sqlast::ColumnDefinition::Type::UINT:
                aggregate_record.SetUInt(record.GetUInt(aggregate_column_), 0);
                break;
              case sqlast::ColumnDefinition::Type::INT:
                aggregate_record.SetInt(record.GetInt(aggregate_column_), 0);
                break;
              default:
                LOG(FATAL) << "Unsupported type when computing SUM aggregate";
            }
            state_.emplace(key, std::move(aggregate_record));
          } else {
            if (!first_delta.contains(
                    key)) {  // track first state change of key
              first_delta.emplace(key,
                                  StateChange(false, aggregate_schema_ref_));
              // need to explicitly set value since records cannot be copied
              switch (aggregate_schema_ref_.TypeOf(0)) {
                case sqlast::ColumnDefinition::Type::UINT:
                  first_delta.at(key).old_value_.SetUInt(
                      state_.at(key).GetUInt(0), 0);
                  break;
                case sqlast::ColumnDefinition::Type::INT:
                  first_delta.at(key).old_value_.SetInt(
                      state_.at(key).GetInt(0), 0);
                  break;
                default:
                  LOG(FATAL) << "Unsupported type when computing SUM aggregate";
              }
            }
            switch (aggregate_schema_ref_.TypeOf(0)) {
              case sqlast::ColumnDefinition::Type::UINT:
                state_.at(key).SetUInt(state_.at(key).GetUInt(0) +
                                           record.GetUInt(aggregate_column_),
                                       0);
                break;
              case sqlast::ColumnDefinition::Type::INT:
                state_.at(key).SetInt(
                    state_.at(key).GetInt(0) + record.GetInt(aggregate_column_),
                    0);
                break;
              default:
                LOG(FATAL) << "Unsupported type when computing SUM aggregate";
            }
          }
        }
        break;
    }
  }

  // emit records only for tracked changes
  for (auto const &item : first_delta) {
    // avoid an edge case where a key is inserted via a positive record
    // and whose effect is negated by the subsequent negative record(s)
    // in the same batch
    if (item.second.is_insert_) {
      bool flag = false;
      // in the following switch block, comparison of latest state update
      // with 0 is semantically correct for both FuncSum and FuncCout
      switch (aggregate_schema_ref_.TypeOf(0)) {
        case sqlast::ColumnDefinition::Type::UINT:
          if (state_.at(item.first).GetUInt(0) == 0ULL) flag = true;
          break;
        case sqlast::ColumnDefinition::Type::INT:
          if (state_.at(item.first).GetInt(0) == 0L) flag = true;
          break;
        default:
          // The aggregate value does not support Type::TEXT
          LOG(FATAL) << "Unsupported type in aggregate";
      }

      if (flag) {
        // don't emit any records since the negative records have cancelled
        // out the effect of positive records within a single batch.
        continue;
      }
    }  // end of handling edge case

    if (item.second.is_insert_) {
      EmitRecord(item.first, state_.at(item.first), true, output);
    } else {
      // for a negative update push negative record followed by positvie record
      EmitRecord(item.first, item.second.old_value_, false, output);
      EmitRecord(item.first, state_.at(item.first), true, output);
    }
  }
  return true;
}

}  // namespace dataflow
}  // namespace pelton
