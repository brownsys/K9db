#include "k9db/dataflow/ops/aggregate.h"

#include <utility>

#include "glog/logging.h"

namespace k9db {
namespace dataflow {

// AbstractAggregateState.
AbstractAggregateState::PolicyPtr AbstractAggregateState::CopyPolicy() const {
  if (this->policy_ == nullptr) {
    return nullptr;
  } else {
    return this->policy_->Copy();
  }
}
void AbstractAggregateState::CombinePolicy(const PolicyPtr &o) {
  if (this->policy_ != nullptr && o != nullptr) {
    this->policy_ = this->policy_->Combine(o);
  } else if (o != nullptr) {
    this->policy_ = o->Copy();
  }
}
void AbstractAggregateState::SubtractPolicy(const PolicyPtr &o) {
  if (this->policy_ != nullptr && o != nullptr) {
    this->policy_ = this->policy_->Subtract(o);
  } else if (o != nullptr) {
    this->policy_ = o->Copy();
  }
}

// AggregateState per aggregation function.
namespace {

// Simple count
class SimpleCountAggregateState : public AbstractAggregateState {
 private:
  uint64_t count_;

 public:
  // NOLINTNEXTLINE
  explicit SimpleCountAggregateState(PolicyPtr &policy)
      : AbstractAggregateState(policy), count_(0) {}

  bool Depleted() const override { return this->count_ == 0; }
  void Add(const sqlast::Value &nvalue) override { this->count_++; }
  void Remove(const sqlast::Value &ovalue) override { this->count_--; }
  sqlast::Value Value() const override { return sqlast::Value(this->count_); }
  uint64_t SizeInMemory() const override { return sizeof(uint64_t); }
};

// Distinct count
class DistinctCountAggregateState : public AbstractAggregateState {
 private:
  uint64_t count_;
  absl::flat_hash_map<sqlast::Value, uint64_t> histogram_;

 public:
  // NOLINTNEXTLINE
  explicit DistinctCountAggregateState(PolicyPtr &policy)
      : AbstractAggregateState(policy), count_(0), histogram_() {}

  bool Depleted() const override { return this->count_ == 0; }
  void Add(const sqlast::Value &nvalue) override {
    if (this->histogram_.contains(nvalue)) {
      this->histogram_[nvalue]++;
    } else {
      this->histogram_[nvalue]++;
      this->count_++;
    }
  }
  void Remove(const sqlast::Value &ovalue) override {
    if (--this->histogram_.at(ovalue) == 0) {
      this->histogram_.erase(ovalue);
      this->count_--;
    }
  }
  sqlast::Value Value() const override { return sqlast::Value(this->count_); }
  uint64_t SizeInMemory() const override {
    uint64_t size = sizeof(uint64_t);
    for (const auto &[k, v] : this->histogram_) {
      size += k.SizeInMemory() + sizeof(v);
    }
    return size;
  }
};

// Sum
class SumAggregateState : public AbstractAggregateState {
 private:
  sqlast::Value::Type type_;
  std::variant<int64_t, uint64_t> sum_;

 public:
  // NOLINTNEXTLINE
  SumAggregateState(PolicyPtr &policy, sqlast::Value::Type type)
      : AbstractAggregateState(policy), type_(type), sum_() {
    if (this->type_ == sqlast::Value::Type::INT) {
      this->sum_ = static_cast<int64_t>(0);
    } else {
      this->sum_ = static_cast<uint64_t>(0);
    }
  }

  bool Depleted() const override { return false; }
  void Add(const sqlast::Value &nvalue) override {
    if (this->type_ == sqlast::Value::Type::INT) {
      this->sum_ = std::get<int64_t>(this->sum_) + nvalue.GetInt();
    } else {
      this->sum_ = std::get<uint64_t>(this->sum_) + nvalue.GetUInt();
    }
  }
  void Remove(const sqlast::Value &ovalue) override {
    if (this->type_ == sqlast::Value::Type::INT) {
      this->sum_ = std::get<int64_t>(this->sum_) - ovalue.GetInt();
    } else {
      this->sum_ = std::get<uint64_t>(this->sum_) - ovalue.GetUInt();
    }
  }
  sqlast::Value Value() const override {
    if (this->type_ == sqlast::Value::Type::INT) {
      return sqlast::Value(std::get<int64_t>(this->sum_));
    } else {
      return sqlast::Value(std::get<uint64_t>(this->sum_));
    }
  }
  uint64_t SizeInMemory() const override {
    return sizeof(this->type_) + sizeof(this->sum_);
  }
};

// Avg
class AvgAggregateState : public AbstractAggregateState {
 private:
  SumAggregateState sum_;
  SimpleCountAggregateState count_;

 public:
  // NOLINTNEXTLINE
  AvgAggregateState(PolicyPtr &policy, sqlast::Value::Type type)
      : AbstractAggregateState(policy), sum_(policy, type), count_(policy) {}

  bool Depleted() const override { return this->count_.Depleted(); }
  void Add(const sqlast::Value &nvalue) override {
    this->sum_.Add(nvalue);
    this->count_.Add(nvalue);
  }
  void Remove(const sqlast::Value &ovalue) override {
    this->sum_.Remove(ovalue);
    this->count_.Remove(ovalue);
  }
  sqlast::Value Value() const override {
    sqlast::Value sum = this->sum_.Value();
    if (sum.type() == sqlast::Value::Type::UINT) {
      uint64_t count = this->count_.Value().GetUInt();
      return sqlast::Value(sum.GetUInt() / count);
    } else {
      int64_t count = static_cast<int64_t>(this->count_.Value().GetUInt());
      return sqlast::Value(sum.GetInt() / count);
    }
  }
  uint64_t SizeInMemory() const override {
    return this->sum_.SizeInMemory() + this->count_.SizeInMemory();
  }
};

}  // namespace

// AggregateOperator.
uint64_t AggregateOperator::SizeInMemory() const {
  uint64_t size = 0;
  for (const auto &[_, v] : this->state_) {
    size += v->SizeInMemory();
  }
  return size;
}

void AggregateOperator::ComputeOutputSchema() {
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
  using PolicyPtr = std::unique_ptr<policy::AbstractPolicy>;
  bool has_value = this->aggregate_column_index_ < 4294967295;

  // old_groups[group_key] indicates whether we have seen this group_key
  // before in this batch of updates.
  absl::flat_hash_map<Key, bool> old_groups;

  // Go over records, updating the aggregate state for each record, as well as
  // old_groups.
  // The negative records to emit are emitted immediately,
  // the positive records are computed after all records are processed
  // to get their most recent version.
  std::vector<Record> output;
  for (const Record &record : records) {
    // Read group by key.
    Key group_key = record.GetValues(this->group_columns_);

    // Make sure it is allowed to aggregate.
    for (const auto &col : this->group_columns_) {
      const PolicyPtr &policy = record.GetPolicy(col);
      if (policy != nullptr) {
        CHECK(policy->Allows(this->type())) << "Policy does not allow this op";
      }
    }

    // First time we encounter this group.
    bool found_old_value = this->state_.contains(group_key);
    if (!found_old_value) {
      if (!record.IsPositive()) {
        LOG(FATAL) << "Negative record not seen before in aggregate";
      }

      // Insert data into state.
      this->state_.insert({group_key, this->InitialData()});
    }

    // Look up earlier data in state.
    std::unique_ptr<AbstractAggregateState> &data = this->state_.at(group_key);

    // Track the 1st old value and policy.
    if (old_groups.count(group_key) == 0) {
      old_groups.emplace(group_key, true);
      if (found_old_value) {
        // Emit removal of the original record prior to any updates.
        output.push_back(this->EmitRecord(group_key, data->Value(),
                                          data->CopyPolicy(), false));
      }
    }

    // Read value being aggregated.
    sqlast::Value record_value = sqlast::Value();
    if (has_value) {
      record_value = record.GetValue(this->aggregate_column_index_);
    }

    // Update stored aggregate data.
    if (record.IsPositive()) {
      data->Add(record_value);
      if (has_value) {
        data->CombinePolicy(record.GetPolicy(this->aggregate_column_index_));
      }
    } else {
      data->Remove(record_value);
      if (has_value) {
        data->SubtractPolicy(record.GetPolicy(this->aggregate_column_index_));
      }
    }

    // If aggregate is depleted, we erase it.
    if (data->Depleted()) {
      // TODO(babman): policy in policies_ is not deleted
      // policies keep growing indefinitely.
      this->state_.erase(group_key);
    }
  }

  // Emit records only for groups whose value ended up changing.
  for (const auto &[group_key, _] : old_groups) {
    if (this->state_.contains(group_key)) {
      const std::unique_ptr<AbstractAggregateState> &data =
          this->state_.at(group_key);
      output.push_back(
          this->EmitRecord(group_key, data->Value(), data->CopyPolicy(), true));
    }
  }
  return output;
}

// Initial empty data for a new group.
std::unique_ptr<AbstractAggregateState> AggregateOperator::InitialData() {
  using PolicyPtr = std::unique_ptr<policy::AbstractPolicy>;

  sqlast::Value::Type type = sqlast::Value::Type::UINT;
  if (this->aggregate_column_type_ == sqlast::ColumnDefinition::Type::INT) {
    type = sqlast::Value::Type::INT;
  }

  PolicyPtr &ref = this->policies_.emplace_back(nullptr);
  switch (this->aggregate_function_) {
    case Function::NO_AGGREGATE:
    case Function::COUNT: {
      if (this->aggregate_column_index_ < 4294967295) {
        return std::make_unique<DistinctCountAggregateState>(ref);
      } else {
        return std::make_unique<SimpleCountAggregateState>(ref);
      }
    }
    case Function::SUM: {
      return std::make_unique<SumAggregateState>(ref, type);
    }
    case Function::AVG: {
      return std::make_unique<AvgAggregateState>(ref, type);
    }
  }
}

// Clone this operator.
std::unique_ptr<Operator> AggregateOperator::Clone() const {
  return std::make_unique<AggregateOperator>(
      this->group_columns_, this->aggregate_function_,
      this->aggregate_column_index_, this->aggregate_column_name_);
}

}  // namespace dataflow
}  // namespace k9db
