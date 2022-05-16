#include "pelton/dataflow/ops/aggregate.h"

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "algorithms/algorithm.h"
#include "algorithms/bounded-sum.h"
#include "algorithms/count.h"
#include "base/statusor.h"
#include "glog/logging.h"
#include "pelton/dp/dp_util.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace dataflow {

void AggregateOperator::ComputeOutputSchema() {
  this->state_.Initialize(false);

  // Figure out the aggregate column information.
  if (aggregate_function_ == Function::COUNT) {
    if (this->aggregate_column_name_ == "") {
      this->aggregate_column_name_ = "Count";
    }
    this->aggregate_column_type_ = sqlast::ColumnDefinition::Type::UINT;
  } else {
    if (this->aggregate_column_name_ == "") {
      this->aggregate_column_name_ = "Sum";
    }
    this->aggregate_column_type_ =
        this->input_schemas_.at(0).TypeOf(this->aggregate_column_index_);
    if (this->aggregate_column_type_ != sqlast::ColumnDefinition::Type::UINT &&
        this->aggregate_column_type_ != sqlast::ColumnDefinition::Type::INT) {
      LOG(FATAL) << "Unsupported column type for Sum aggregate";
    }
  }

  // Obtain column names and types.
  std::vector<std::string> out_column_names = {};
  std::vector<sqlast::ColumnDefinition::Type> out_column_types = {};
  for (const auto &cid : this->group_columns_) {
    out_column_names.push_back(this->input_schemas_.at(0).NameOf(cid));
    out_column_types.push_back(this->input_schemas_.at(0).TypeOf(cid));
  }
  // Add another column for the aggregate.
  out_column_names.push_back(this->aggregate_column_name_);
  out_column_types.push_back(this->aggregate_column_type_);

  // Construct out_keys.
  // The key for the produced aggregated records are the columns that were
  // grouped by on. The combined value of these columns are unique, as all rows
  // that had that value are aggregated into a single one. This is only for
  // semantic purposes as, as of now, this does not have an effect on
  std::vector<ColumnID> out_keys;
  for (size_t i = 0; i < this->group_columns_.size(); i++) {
    out_keys.push_back(i);
  }

  // Now we know the output schema.
  this->output_schema_ =
      SchemaFactory::Create(out_column_names, out_column_types, out_keys);
}

// Construct an output record with this->output_schema_.
// The first n-1 columns are assigned values from the given key corresponding
// to group by columns.
// The last column is assigned value from the aggregate value.
Record AggregateOperator::EmitRecord(const Key &key, const Value &aggregate,
                                     bool positive) const {
  // Create record and add the group by field values to it.
  Record result{this->output_schema_, positive};
  for (size_t i = 0; i < key.size(); i++) {
    result.SetValue(key.value(i), i);
  }
  result.SetValue(aggregate, this->output_schema_.size() - 1);
  return result;
}

std::vector<Record> AggregateOperator::Process(NodeIndex source,
                                               std::vector<Record> &&records,
                                               const Promise &promise) {
  // old_values[group_key] has the old value associated to that key prior to
  // processing records.
  // if old_values[group_key] is a null Value then there was no value associated
  // to the key prior to processing records.
  // This information is used to determine what records to emit to children
  // operators.
  absl::flat_hash_map<Key, Value> old_values;

  // Go over records, updating the aggregate state for each record, as well as
  // old_values. The records to emit will be computed afterwards.
  for (const Record &record : records) {
    Key group_key = record.GetValues(this->group_columns_);
    switch (this->aggregate_function_) {
      case Function::COUNT: {
        if (!record.IsPositive()) {
          // Negative record: decrement value in state by -1.
          if (!this->state_.Contains(group_key)) {
            LOG(FATAL) << "Negative record not seen before in aggregate";
          }
          // Save old value and update the state.
          Value &value = this->state_.Get(group_key).front();
          if (old_values.count(group_key) == 0) {
            old_values.emplace(group_key, value);
          }
          value = Value(value.GetUInt() - 1);
          if (value.GetUInt() == 0) {
            this->state_.Erase(group_key);
          }
        } else {
          // Positive record: increment value in state by +1.
          if (this->state_.Contains(group_key)) {
            // Increment state by +1 and track old value.
            Value &value = this->state_.Get(group_key).front();
            if (old_values.count(group_key) == 0) {
              old_values.emplace(group_key, value);
            }
            value = Value(value.GetUInt() + 1);
          } else {
            if (old_values.count(group_key) == 0) {
              // Put in null value.
              old_values.emplace(group_key,
                                 Value(sqlast::ColumnDefinition::Type::UINT));
            }
            this->state_.Insert(group_key, Value(static_cast<uint64_t>(1)));
          }
        }
        break;
      }
      case Function::SUM: {
        Value record_value = record.GetValue(this->aggregate_column_index_);
        if (!record.IsPositive()) {
          // Negative record: decrement value in state by the value in record.
          if (!this->state_.Contains(group_key)) {
            LOG(FATAL) << "Negative record not seen before in aggregate";
          }
          // Save old value and update the state.
          Value &value = this->state_.Get(group_key).front();
          if (old_values.count(group_key) == 0) {
            old_values.emplace(group_key, value);
          }
          switch (this->aggregate_column_type_) {
            case sqlast::ColumnDefinition::Type::UINT:
              value = Value(value.GetUInt() - record_value.GetUInt());
              break;
            case sqlast::ColumnDefinition::Type::INT:
              value = Value(value.GetInt() - record_value.GetInt());
              break;
            default:
              LOG(FATAL) << "Unsupported type";
          }
        } else {
          // Positive record: increment value in state by the value in record.
          if (this->state_.Contains(group_key)) {
            Value &value = this->state_.Get(group_key).front();
            if (old_values.count(group_key) == 0) {
              old_values.emplace(group_key, value);
            }
            switch (this->aggregate_column_type_) {
              case sqlast::ColumnDefinition::Type::UINT:
                value = Value(value.GetUInt() + record_value.GetUInt());
                break;
              case sqlast::ColumnDefinition::Type::INT:
                value = Value(value.GetInt() + record_value.GetInt());
                break;
              default:
                LOG(FATAL) << "Unsupported type";
            }
          } else {
            if (old_values.count(group_key) == 0) {
              // Put in null value.
              old_values.emplace(group_key,
                                 Value(this->aggregate_column_type_));
            }
            this->state_.Insert(group_key, std::move(record_value));
          }
        }
        break;
      }
    }
  }

  // Emit records only for groups whose value ended up changing.
  std::vector<Record> output;
  for (const auto &[group_key, old_value] : old_values) {
    if (!this->state_.Contains(group_key)) {
      if (!old_value.IsNull()) {
        output.push_back(this->EmitRecord(group_key, old_value, false));
      }
    } else {
      const Value &new_value = this->state_.Get(group_key).front();
      if (old_value != new_value) {
        if (!old_value.IsNull()) {
          output.push_back(this->EmitRecord(group_key, old_value, false));
        }
        output.push_back(this->EmitRecord(group_key, new_value, true));
      }
    }
  }
  return output;
}

// Largely adapted from the above Process implementation
std::vector<Record> AggregateOperator::ProcessDP(
    NodeIndex source, std::vector<Record> &&records, const Promise &promise,
    pelton::dp::DPOptions *dp_options) {
  // old_values[group_key] has the old value associated to that key prior to
  // processing records.
  // if old_values[group_key] is a null Value then there was no value associated
  // to the key prior to processing records.
  // This information is used to determine what records to emit to children
  // operators.

  // Get the epsilon value specified for the view, defaulting to 1.0
  auto eps = dp_options->epsilon.value_or(1.0);

  absl::flat_hash_map<Key, Value> old_values;

  std::unique_ptr<differential_privacy::Algorithm<int64_t> > dp_algo;

  switch (this->aggregate_function_) {
    case Function::COUNT: {
      // Use the DP Library Count algorithm
      differential_privacy::Count<int64_t>::Builder intermediate =
          differential_privacy::Count<int64_t>::Builder();

      // Set the epsilon value for this query
      intermediate.SetEpsilon(eps);

      differential_privacy::base::StatusOr<
          std::unique_ptr<differential_privacy::Count<int64_t> > >
          dp_count_build = intermediate.Build();

      dp_algo = std::move(dp_count_build).ValueOrDie();

      break;
    }
    case Function::SUM: {
      // Use the DP Library bounded sum algorithm
      differential_privacy::BoundedSum<int64_t>::Builder intermediate =
          differential_privacy::BoundedSum<int64_t>::Builder();

      // Set the epsilon value and delta value (defaulting to 0) for this query
      intermediate.SetEpsilon(eps).SetDelta(dp_options->delta.value_or(0.0));

      // If upper and lower bound values are specified, add those bounds to the algorithm
      if (dp_options->bound_l.has_value()) {
        intermediate.SetLower(dp_options->bound_l.value());
      }
      if (dp_options->bound_h.has_value()) {
        intermediate.SetUpper(dp_options->bound_h.value());
      }

      differential_privacy::base::StatusOr<
          std::unique_ptr<differential_privacy::BoundedSum<int64_t> > >
          dp_sum_build = intermediate.Build();

      dp_algo = std::move(dp_sum_build).ValueOrDie();

      break;
    }
  }

  // Go over records, updating the aggregate state for each record, as well as
  // old_values. The records to emit will be computed afterwards.
  for (const Record &record : records) {
    Key group_key = record.GetValues(this->group_columns_);
    switch (this->aggregate_function_) {
      case Function::COUNT: {
        
        // Increment the count
        dp_algo->AddEntry(1);
        
        if (!record.IsPositive()) {
          // Negative record: decrement value in state by -1.
          if (!this->state_.Contains(group_key)) {
            LOG(FATAL) << "Negative record not seen before in aggregate";
          }
          // Save old value and update the state.
          Value &value = this->state_.Get(group_key).front();
          if (old_values.count(group_key) == 0) {
            old_values.emplace(group_key, value);
          }
          value = Value(value.GetUInt() - 1);
          if (value.GetUInt() == 0) {
            this->state_.Erase(group_key);
          }
        } else {
          // Positive record: increment value in state by +1.
          if (this->state_.Contains(group_key)) {
            // Increment state by +1 and track old value.
            Value &value = this->state_.Get(group_key).front();
            if (old_values.count(group_key) == 0) {
              old_values.emplace(group_key, value);
            }
            value = Value(value.GetUInt() + 1);
          } else {
            if (old_values.count(group_key) == 0) {
              // Put in null value.
              old_values.emplace(group_key,
                                 Value(sqlast::ColumnDefinition::Type::UINT));
            }
            this->state_.Insert(group_key, Value(static_cast<uint64_t>(1)));
          }
        }
        break;
      }
      case Function::SUM: {
        Value record_value = record.GetValue(this->aggregate_column_index_);

        if (!record.IsPositive()) {
          LOG(FATAL) << "Got an unexpected non-positive record in differentially private SUM, in ProcessDP.";
        }

        // Positive record: increment value in state by the value in record.
        switch (this->aggregate_column_type_) {
          case sqlast::ColumnDefinition::Type::UINT:
            dp_algo->AddEntry((int64_t)record_value.GetUInt());
            break;
          case sqlast::ColumnDefinition::Type::INT:
            dp_algo->AddEntry(record_value.GetInt());
            break;
          default:
            LOG(FATAL) << "Unsupported type";
        }
        if (this->state_.Contains(group_key)) {
          Value &value = this->state_.Get(group_key).front();
          if (old_values.count(group_key) == 0) {
            old_values.emplace(group_key, value);
          }
          // Increment the value using the aproipriate type
          switch (this->aggregate_column_type_) {
            case sqlast::ColumnDefinition::Type::UINT:
              value = Value(value.GetUInt() + record_value.GetUInt());
              break;
            case sqlast::ColumnDefinition::Type::INT:
              value = Value(value.GetInt() + record_value.GetInt());
              break;
            default:
              LOG(FATAL) << "Unsupported type";
          }
        } else {
          if (old_values.count(group_key) == 0) {
            // Put in null value.
            old_values.emplace(group_key, Value(this->aggregate_column_type_));
          }
          this->state_.Insert(group_key, std::move(record_value));
        }

        break;
      }
    }
  }

  // Emit records only for groups whose value ended up changing.
  std::vector<Record> output;
  for (const auto &[group_key, old_value] : old_values) {
    if (!this->state_.Contains(group_key)) {
      if (!old_value.IsNull()) {
        output.push_back(this->EmitRecord(group_key, old_value, false));
      }
    } else {
      const Value &new_value =
          (this->aggregate_function_ == Function::SUM)
              ? Value(differential_privacy::GetValue<int64_t>(
                    (dp_algo->PartialResult()).ValueOrDie()))
              : Value(differential_privacy::GetValue<uint64_t>(
                    (dp_algo->PartialResult()).ValueOrDie()));
      if (old_value != new_value) {
        if (!old_value.IsNull()) {
          output.push_back(this->EmitRecord(group_key, old_value, false));
        }
        output.push_back(this->EmitRecord(group_key, new_value, true));
      }
    }
  }
  return output;
}

// Clone this operator.
std::unique_ptr<Operator> AggregateOperator::Clone() const {
  return std::make_unique<AggregateOperator>(
      this->group_columns_, this->aggregate_function_,
      this->aggregate_column_index_, this->aggregate_column_name_);
}

}  // namespace dataflow
}  // namespace pelton
