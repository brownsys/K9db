#include "pelton/dataflow/ops/aggregate.h"

#include <memory>
#include <tuple>
#include <utility>

#include "glog/logging.h"
#include "pelton/dataflow/record.h"
#include "pelton/sqlast/ast.h"

#define SUMFUNC_UPDATE_AGGREGATE_MACRO(OP)                                 \
  {                                                                        \
    switch (aggregate_schema_.TypeOf(0)) {                                 \
      case sqlast::ColumnDefinition::Type::UINT:                           \
        (*state_.Lookup(key).begin())                                      \
            .SetUInt((*state_.Lookup(key).begin())                         \
                         .GetUInt(0) OP record.GetUInt(aggregate_column_), \
                     0);                                                   \
        break;                                                             \
      case sqlast::ColumnDefinition::Type::INT:                            \
        (*state_.Lookup(key).begin())                                      \
            .SetInt((*state_.Lookup(key).begin())                          \
                        .GetInt(0) OP record.GetInt(aggregate_column_),    \
                    0);                                                    \
        break;                                                             \
      default:                                                             \
        LOG(FATAL) << "Unsupported type when computing SUM aggregate";     \
    }                                                                      \
  }
// SUMFUNC_UPDATE_AGGREGATE_MACRO

namespace pelton {
namespace dataflow {

namespace {

// This class is used to track state changes when processing given batch of
// input records
class StateChange {
 public:
  StateChange(bool is_insert, const SchemaRef &aggregate_schema)
      : is_insert_(is_insert), old_value_(Record(aggregate_schema, true)) {}
  bool is_insert_;
  Record old_value_;
};

}  // namespace

bool AggregateOperator::EnclosedKeyCols(
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

void AggregateOperator::ComputeOutputSchema() {
  std::vector<std::string> out_column_names = {};
  std::vector<sqlast::ColumnDefinition::Type> out_column_types = {};
  std::vector<ColumnID> out_keys = {};
  const std::vector<ColumnID> &input_keys = this->input_schemas_.at(0).keys();

  // Construct out_keys
  // If the input key set is a subset of the projected columns only then form an
  // output keyset. Else do not assign keys for the output schema. Because the
  // subset of input keycols can no longer uniqely identify records. This is
  // only for semantic purposes as, as of now, this does not have an effect on
  // the materialized view.
  if (EnclosedKeyCols(input_keys, group_columns_)) {
    for (auto ik : input_keys) {
      auto it = std::find(group_columns_.begin(), group_columns_.end(), ik);
      if (it != out_keys.end()) {
        out_keys.push_back(std::distance(group_columns_.begin(), it));
      }
    }
  }

  std::vector<std::string> agg_column_name;
  std::vector<sqlast::ColumnDefinition::Type> agg_column_type;
  // Calcite does not supply a column id for count function, hence
  // be independent of it
  if (aggregate_function_ == Function::COUNT) {
    agg_column_name.push_back("Count");
    agg_column_type.push_back(sqlast::ColumnDefinition::Type::UINT);
  } else {
    agg_column_name.push_back("Sum");
    agg_column_type.push_back(
        this->input_schemas_.at(0).TypeOf(aggregate_column_));
  }

  // Obtain column names and types
  for (const auto &cid : group_columns_) {
    out_column_names.push_back(this->input_schemas_.at(0).NameOf(cid));
    out_column_types.push_back(this->input_schemas_.at(0).TypeOf(cid));
  }
  // Add another column for the aggregate
  out_column_names.push_back(agg_column_name.at(0));
  out_column_types.push_back(agg_column_type.at(0));

  this->aggregate_schema_ =
      SchemaFactory::Create(agg_column_name, agg_column_type, {});
  this->output_schema_ =
      SchemaFactory::Create(out_column_names, out_column_types, out_keys);
}

void AggregateOperator::EmitRecord(const Key &key,
                                   const Record &aggregate_record,
                                   bool positive,
                                   std::vector<Record> *output) const {
  // create record
  Record output_record(this->output_schema_, positive);
  for (size_t i = 0; i < key.size(); i++) {
    switch (this->output_schema_.TypeOf(i)) {
      case sqlast::ColumnDefinition::Type::UINT:
        output_record.SetUInt(key.value(i).GetUInt(), i);
        break;
      case sqlast::ColumnDefinition::Type::INT:
        output_record.SetInt(key.value(i).GetInt(), i);
        break;
      case sqlast::ColumnDefinition::Type::TEXT:
        // records have to own their pointed data
        output_record.SetString(
            std::make_unique<std::string>(key.value(i).GetString()), i);
        break;
      default:
        LOG(FATAL) << "Unsupported type when forming output record";
    }
  }

  // Add aggregate value to the last column
  size_t aggregate_index = this->output_schema_.size() - 1;
  switch (aggregate_schema_.TypeOf(0)) {
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

inline void AggregateOperator::InitAggregateValue(Record *aggregate_record,
                                                  const Record &from_record,
                                                  ColumnID from_column) const {
  switch (aggregate_schema_.TypeOf(0)) {
    case sqlast::ColumnDefinition::Type::UINT:
      aggregate_record->SetUInt(from_record.GetUInt(from_column), 0);
      break;
    case sqlast::ColumnDefinition::Type::INT:
      aggregate_record->SetInt(from_record.GetInt(from_column), 0);
      break;
    default:
      LOG(FATAL) << "Unsupported type when computing SUM aggregate";
  }
}

bool AggregateOperator::Process(NodeIndex source,
                                const std::vector<Record> &records,
                                std::vector<Record> *output) {
  absl::flat_hash_map<Key, StateChange> first_delta;

  for (const Record &record : records) {
    const Key key = record.GetValues(group_columns_);
    switch (aggregate_function_) {
      case Function::COUNT:
        /*
         * For negative records value in state is decremented by 1 and for
         * positive records value in state is incremented by 1
         */
        if (!record.IsPositive()) {
          if (!state_.Contains(key)) {  // handle negative record
            LOG(FATAL)
                << "State does not exist for corresponding negative record";
          }
          if (!first_delta.contains(key)) {  // track first state change of key
            first_delta.emplace(key, StateChange(false, aggregate_schema_));
            // need to explicitly set value since records cannot be copied
            InitAggregateValue(&first_delta.at(key).old_value_,
                               *state_.Lookup(key).begin(), 0);
          }
          auto state_record = state_.Lookup(key).begin();
          (*state_.Lookup(key).begin())
              .SetUInt((*state_.Lookup(key).begin()).GetUInt(0) - 1, 0);
        } else {  // handle positive record
          if (!state_.Contains(key)) {
            // track first state change of key. old_value_ does not exist.
            first_delta.emplace(key, StateChange(true, aggregate_schema_));
            // new aggregate record
            Record aggregate_record(aggregate_schema_);
            aggregate_record.SetUInt(1ULL, 0);
            state_.Insert(key, std::move(aggregate_record));
          } else {
            if (!first_delta.contains(
                    key)) {  // track first state change of key
              first_delta.emplace(key, StateChange(false, aggregate_schema_));
              // need to explicitly set value since records cannot be copied
              InitAggregateValue(&first_delta.at(key).old_value_,
                                 *state_.Lookup(key).begin(), 0);
            }
            (*state_.Lookup(key).begin())
                .SetUInt((*state_.Lookup(key).begin()).GetUInt(0) + 1, 0);
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
          if (!state_.Contains(key)) {
            LOG(FATAL)
                << "State does not exist for corresponding negative record";
          }
          if (!first_delta.contains(key)) {  // track first state change of key
            first_delta.emplace(key, StateChange(false, aggregate_schema_));
            // need to explicitly set value since records cannot be copied
            InitAggregateValue(&first_delta.at(key).old_value_,
                               *state_.Lookup(key).begin(), 0);
          }
          SUMFUNC_UPDATE_AGGREGATE_MACRO(-)
        } else {  // handle positive record
          if (!state_.Contains(key)) {
            // track first state change of key. old_value_ does not exist.
            first_delta.emplace(key, StateChange(true, aggregate_schema_));
            // new aggregate record
            Record aggregate_record(aggregate_schema_);
            InitAggregateValue(&aggregate_record, record, aggregate_column_);
            state_.Insert(key, std::move(aggregate_record));
          } else {
            if (!first_delta.contains(
                    key)) {  // track first state change of key
              first_delta.emplace(key, StateChange(false, aggregate_schema_));
              // need to explicitly set value since records cannot be copied
              InitAggregateValue(&first_delta.at(key).old_value_,
                                 *state_.Lookup(key).begin(), 0);
            }
            SUMFUNC_UPDATE_AGGREGATE_MACRO(+)
          }
        }
        break;
    }
  }

  // emit records only for tracked changes
  for (auto const &item : first_delta) {
    // Logic for emitting records:
    // When emitting record(s), the key deciding factor is if the final
    // update of a record consists of 0. Following are two outcomes when final
    // state update = 0:
    // CASE 1: If the state change for item corresponds to an insert
    // then it implies that a record with key item.first was inserted
    // into state_ but it's effect got negated via subsequent negative
    // record(s) in the same batch. Hence, don't emit any output records
    // CASE 2: If the state change for item corresponds to an update then only
    // emit negative record. This will ensure that corresponding
    // state in the down stream operators will get cleared. Which would not
    // happen if a positive record if emitted for the corresponding key with
    // value 0.
    // NOTE: The comparision of final state update with 0 is semantically
    // correct for both SUM and COUNT.
    if (item.second.is_insert_) {
      switch (aggregate_schema_.TypeOf(0)) {
        case sqlast::ColumnDefinition::Type::UINT:
          if ((*state_.Lookup(item.first).begin()).GetUInt(0) == 0ULL) {
            state_.Erase(item.first);
            continue;
          }
          break;
        case sqlast::ColumnDefinition::Type::INT:
          if ((*state_.Lookup(item.first).begin()).GetInt(0) == 0L) {
            state_.Erase(item.first);
            continue;
          }
          break;
        default:
          // The aggregate value does not support Type::TEXT
          LOG(FATAL) << "Unsupported type in aggregate";
      }
      EmitRecord(item.first, *state_.Lookup(item.first).begin(), true, output);
    } else {
      switch (aggregate_schema_.TypeOf(0)) {
        case sqlast::ColumnDefinition::Type::UINT:
          if ((*state_.Lookup(item.first).begin()).GetUInt(0) == 0ULL) {
            state_.Erase(item.first);
            EmitRecord(item.first, item.second.old_value_, false, output);
            continue;
          }
          break;
        case sqlast::ColumnDefinition::Type::INT:
          if ((*state_.Lookup(item.first).begin()).GetInt(0) == 0L) {
            state_.Erase(item.first);
            EmitRecord(item.first, item.second.old_value_, false, output);
            continue;
          }
          break;
        default:
          // The aggregate value does not support Type::TEXT
          LOG(FATAL) << "Unsupported type in aggregate";
      }
      // for a negative update push negative record followed by positive record
      EmitRecord(item.first, item.second.old_value_, false, output);
      EmitRecord(item.first, *state_.Lookup(item.first).begin(), true, output);
    }
  }
  return true;
}

}  // namespace dataflow
}  // namespace pelton
