#include "k9db/dataflow/ops/aggregate.h"

#include <utility>

#include "glog/logging.h"

namespace k9db {
namespace dataflow {

void AggregateOperator::ComputeOutputSchema() {
  this->state_.Initialize(false);

  // Figure out the aggregate column information.
  if (this->aggregate_function_ == Function::COUNT) {
    if (this->aggregate_column_name_ == "") {
      this->aggregate_column_name_ = "Count";
    }
    this->aggregate_column_type_ = sqlast::ColumnDefinition::Type::UINT;
  } else if (this->aggregate_function_ == Function::SUM) {
    if (this->aggregate_column_name_ == "") {
      this->aggregate_column_name_ = "Sum";
    }
    this->aggregate_column_type_ =
        this->input_schemas_.at(0).TypeOf(this->aggregate_column_index_);
    if (this->aggregate_column_type_ != sqlast::ColumnDefinition::Type::UINT &&
        this->aggregate_column_type_ != sqlast::ColumnDefinition::Type::INT) {
      LOG(FATAL) << "Unsupported column type for Sum aggregate";
    }
  } else if (this->aggregate_function_ == Function::AVG) {
    if (this->aggregate_column_name_ == "") {
      this->aggregate_column_name_ = "Avg";
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
  if (this->aggregate_function_ != Function::NO_AGGREGATE) {
    out_column_names.push_back(this->aggregate_column_name_);
    out_column_types.push_back(this->aggregate_column_type_);
  }

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
Record AggregateOperator::EmitRecord(
    const Key &key, const sqlast::Value &aggregate,
    std::unique_ptr<policy::AbstractPolicy> &&policy, bool positive) const {
  // Create record and add the group by field values to it.
  Record result{this->output_schema_, positive};
  for (size_t i = 0; i < key.size(); i++) {
    result.SetValue(key.value(i), i);
  }
  if (this->aggregate_function_ != Function::NO_AGGREGATE) {
    result.SetValue(aggregate, this->output_schema_.size() - 1);
    result.SetPolicy(this->output_schema_.size() - 1, std::move(policy));
  }
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
  using PolicyPtr = std::unique_ptr<policy::AbstractPolicy>;
  absl::flat_hash_map<Key, sqlast::Value> old_values;
  absl::flat_hash_map<Key, PolicyPtr> old_policies;

  // Make sure it is allowed to aggregate.
  for (const Record &record : records) {
    for (const auto &col : this->group_columns_) {
      const PolicyPtr &policy = record.GetPolicy(col);
      if (policy != nullptr) {
        CHECK(policy->Allows(this->type())) << "Policy does not allow this op";
      }
    }
  }

  // Go over records, updating the aggregate state for each record, as well as
  // old_values. The records to emit will be computed afterwards.
  for (const Record &record : records) {
    Key group_key = record.GetValues(this->group_columns_);
    switch (this->aggregate_function_) {
      case Function::NO_AGGREGATE:
      case Function::COUNT: {
        if (!record.IsPositive()) {
          // Negative record: decrement value in state by -1.
          if (!this->state_.Contains(group_key)) {
            LOG(FATAL) << "Negative record not seen before in aggregate";
          }
          // Save old value and update the state.
          sqlast::Value &value = this->state_.Get(group_key).front().value;
          if (old_values.count(group_key) == 0) {
            old_values.emplace(group_key, value);
            old_policies.emplace(group_key, nullptr);
          }
          value = sqlast::Value(value.GetUInt() - 1);
          if (value.GetUInt() == 0) {
            this->state_.Erase(group_key);
          }
        } else {
          // Positive record: increment value in state by +1.
          if (this->state_.Contains(group_key)) {
            // Increment state by +1 and track old value.
            sqlast::Value &value = this->state_.Get(group_key).front().value;
            if (old_values.count(group_key) == 0) {
              old_values.emplace(group_key, value);
              old_policies.emplace(group_key, nullptr);
            }
            value = sqlast::Value(value.GetUInt() + 1);
          } else {
            if (old_values.count(group_key) == 0) {
              // Put in null value.
              old_values.emplace(group_key, sqlast::Value());
              old_policies.emplace(group_key, nullptr);
            }
            PolicyPtr &policy = this->policies_.emplace_back(nullptr);
            AggregateData data = {
                sqlast::Value(static_cast<uint64_t>(1)), {}, {}, policy};
            this->state_.Insert(group_key, std::move(data));
          }
        }
        break;
      }
      case Function::SUM: {
        auto record_value = record.GetValue(this->aggregate_column_index_);
        PolicyPtr policy = record.CopyPolicy(this->aggregate_column_index_);
        if (!record.IsPositive()) {
          // Negative record: decrement value in state by the value in record.
          if (!this->state_.Contains(group_key)) {
            LOG(FATAL) << "Negative record not seen before in aggregate";
          }
          // Save old value and update the state.
          AggregateData &data = this->state_.Get(group_key).front();
          sqlast::Value &value = data.value;
          if (old_values.count(group_key) == 0) {
            old_values.emplace(group_key, value);
            old_policies.emplace(group_key, data.CopyPolicy());
          }
          data.SubtractPolicy(policy);
          switch (this->aggregate_column_type_) {
            case sqlast::ColumnDefinition::Type::UINT:
              value = sqlast::Value(value.GetUInt() - record_value.GetUInt());
              break;
            case sqlast::ColumnDefinition::Type::INT:
              value = sqlast::Value(value.GetInt() - record_value.GetInt());
              break;
            default:
              LOG(FATAL) << "Unsupported type";
          }
        } else {
          // Positive record: increment value in state by the value in record.
          if (this->state_.Contains(group_key)) {
            AggregateData &data = this->state_.Get(group_key).front();
            sqlast::Value &value = data.value;
            if (old_values.count(group_key) == 0) {
              old_values.emplace(group_key, value);
              old_policies.emplace(group_key, data.CopyPolicy());
            }
            data.CombinePolicy(policy);
            switch (this->aggregate_column_type_) {
              case sqlast::ColumnDefinition::Type::UINT:
                value = sqlast::Value(value.GetUInt() + record_value.GetUInt());
                break;
              case sqlast::ColumnDefinition::Type::INT:
                value = sqlast::Value(value.GetInt() + record_value.GetInt());
                break;
              default:
                LOG(FATAL) << "Unsupported type";
            }
          } else {
            if (old_values.count(group_key) == 0) {
              // Put in null value.
              old_values.emplace(group_key, sqlast::Value());
              old_policies.emplace(group_key, nullptr);
            }
            PolicyPtr &pref = this->policies_.emplace_back(std::move(policy));
            AggregateData data = {std::move(record_value), {}, {}, pref};
            this->state_.Insert(group_key, std::move(data));
          }
        }
        break;
      }
      case Function::AVG: {
        auto record_value = record.GetValue(this->aggregate_column_index_);
        PolicyPtr policy = record.CopyPolicy(this->aggregate_column_index_);
        if (!record.IsPositive()) {
          // Negative record: decrement value in state by the value in record.
          if (!this->state_.Contains(group_key)) {
            LOG(FATAL) << "Negative record not seen before in aggregate";
          }
          // Save old value and update the state.
          AggregateData &data = this->state_.Get(group_key).front();
          sqlast::Value &value = data.value;
          if (old_values.count(group_key) == 0) {
            old_values.emplace(group_key, value);
            old_policies.emplace(group_key, data.CopyPolicy());
          }

          data.SubtractPolicy(policy);
          sqlast::Value &v1 = data.v1;
          sqlast::Value &v2 = data.v2;
          v2 = sqlast::Value(v2.GetUInt() - 1);
          if (v2.GetUInt() == 0) {
            this->state_.Erase(group_key);
            continue;
          }
          switch (this->aggregate_column_type_) {
            case sqlast::ColumnDefinition::Type::UINT:
              v1 = sqlast::Value(v1.GetUInt() - record_value.GetUInt());
              value = sqlast::Value(v1.GetUInt() / v2.GetUInt());
              break;
            case sqlast::ColumnDefinition::Type::INT: {
              int64_t count = static_cast<int64_t>(v2.GetUInt());
              v1 = sqlast::Value(v1.GetInt() - record_value.GetInt());
              value = sqlast::Value(v1.GetInt() / count);
              break;
            }
            default:
              LOG(FATAL) << "Unsupported type";
          }
        } else {
          // Positive record: increment value in state by the value in record.
          if (this->state_.Contains(group_key)) {
            AggregateData &data = this->state_.Get(group_key).front();
            sqlast::Value &value = data.value;
            if (old_values.count(group_key) == 0) {
              old_values.emplace(group_key, value);
              old_policies.emplace(group_key, data.CopyPolicy());
            }
            data.CombinePolicy(policy);
            sqlast::Value &v1 = data.v1;
            sqlast::Value &v2 = data.v2;
            v2 = sqlast::Value(v2.GetUInt() + 1);
            switch (this->aggregate_column_type_) {
              case sqlast::ColumnDefinition::Type::UINT:
                v1 = sqlast::Value(v1.GetUInt() + record_value.GetUInt());
                value = sqlast::Value(v1.GetUInt() / v2.GetUInt());
                break;
              case sqlast::ColumnDefinition::Type::INT: {
                int64_t count = static_cast<int64_t>(v2.GetUInt());
                v1 = sqlast::Value(v1.GetInt() + record_value.GetInt());
                value = sqlast::Value(v1.GetInt() / count);
                break;
              }
              default:
                LOG(FATAL) << "Unsupported type";
            }
          } else {
            if (old_values.count(group_key) == 0) {
              // Put in null value.
              old_values.emplace(group_key, sqlast::Value());
              old_policies.emplace(group_key, nullptr);
            }
            PolicyPtr &pref = this->policies_.emplace_back(std::move(policy));
            AggregateData data = {record_value, record_value,
                                  sqlast::Value(static_cast<uint64_t>(1)),
                                  pref};
            this->state_.Insert(group_key, std::move(data));
          }
        }
        break;
      }
    }
  }

  // Emit records only for groups whose value ended up changing.
  std::vector<Record> output;
  for (const auto &[group_key, old_value] : old_values) {
    PolicyPtr old_policy = std::move(old_policies.at(group_key));
    if (!this->state_.Contains(group_key)) {
      if (!old_value.IsNull()) {
        output.push_back(this->EmitRecord(group_key, old_value,
                                          std::move(old_policy), false));
      }
    } else {
      const AggregateData &data = this->state_.Get(group_key).front();
      const sqlast::Value &new_value = data.value;
      PolicyPtr new_policy = data.CopyPolicy();
      if (old_value != new_value) {
        if (!old_value.IsNull()) {
          output.push_back(this->EmitRecord(group_key, old_value,
                                            std::move(old_policy), false));
        }
        output.push_back(this->EmitRecord(group_key, new_value,
                                          std::move(new_policy), true));
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
}  // namespace k9db
