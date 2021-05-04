#include "pelton/mysql/result.h"

#include "glog/logging.h"
#include "pelton/mysql/util.h"

namespace pelton {
namespace mysql {

namespace {

std::string StringifyRecord(const dataflow::Record &record) {
  std::string delim("\0", 1);
  std::string str = "";
  for (size_t i = 0; i < record.schema().size(); i++) {
    str += record.GetValueString(i) + delim;
  }
  return str;
}

}  // namespace

// SqlResult.
SqlResult::SqlResult()
    : impl_(std::make_unique<StatementResult>(true)), schema_() {}

void SqlResult::Append(SqlResult &&other) {
  if (this->IsStatement()) {
    this->impl_ = std::move(other.impl_);
    this->schema_ = other.schema_;
  } else if (this->IsUpdate()) {
    if (!other.IsUpdate()) {
      LOG(FATAL) << "Appending non update to update result";
    }
    UpdateResult *ptr = reinterpret_cast<UpdateResult *>(this->impl_.get());
    ptr->IncrementCount(other.UpdateCount());
  } else if (this->IsQuery()) {
    if (!other.IsQuery()) {
      LOG(FATAL) << "Appending non query to query result";
    }
    if (!this->impl_->IsInlined()) {
      LOG(FATAL) << "Appending to non-inlined query result";
    }

    InlinedSqlResult *ptr =
        reinterpret_cast<InlinedSqlResult *>(this->impl_.get());
    while (other.HasNext()) {
      ptr->AddRecord(other.FetchOne());
    }
  }
}

void SqlResult::AppendDeduplicate(SqlResult &&other) {
  if (this->IsStatement()) {
    this->impl_ = std::move(other.impl_);
    this->schema_ = other.schema_;
  } else if (this->IsUpdate()) {
    if (!other.IsUpdate()) {
      LOG(FATAL) << "Appending non update to update result";
    }
    UpdateResult *ptr = reinterpret_cast<UpdateResult *>(this->impl_.get());
    ptr->IncrementCount(other.UpdateCount());
  } else if (this->IsQuery()) {
    if (!other.IsQuery()) {
      LOG(FATAL) << "Appending non query to query result";
    }
    if (!this->impl_->IsInlined()) {
      LOG(FATAL) << "Appending to non-inlined query result";
    }

    InlinedSqlResult *ptr =
        reinterpret_cast<InlinedSqlResult *>(this->impl_.get());
    // Only add unduplicated rows.
    std::unordered_set<std::string> row_set = ptr->RowSet();
    while (other.HasNext()) {
      dataflow::Record record = other.FetchOne();
      if (row_set.count(StringifyRecord(record)) == 0) {
        ptr->AddRecord(std::move(record));
      }
    }
  }
}

void SqlResult::MakeInline() {
  if (this->impl_->IsQuery() && !this->impl_->IsInlined()) {
    std::vector<dataflow::Record> records;
    while (this->impl_->HasNext()) {
      records.push_back(std::move(this->FetchOne()));
    }
    this->impl_ = std::make_unique<InlinedSqlResult>(std::move(records));
  }
}

// MySqlResult.
bool MySqlResult::IsQuery() const { return true; }

bool MySqlResult::HasNext() { return this->result_->next(); }

dataflow::Record MySqlResult::FetchOne(const dataflow::SchemaRef &schema) {
  return MySqlRowToRecord(this->result_.get(), schema, false);
}

// AugmentedSqlResult.
bool AugmentedSqlResult::IsQuery() const { return true; }

bool AugmentedSqlResult::HasNext() { return this->result_->next(); }

dataflow::Record AugmentedSqlResult::FetchOne(
    const dataflow::SchemaRef &schema) {
  dataflow::Record record = MySqlRowToRecordSkipping(
      this->result_.get(), schema, false, this->aug_index_);
  record.SetValue(this->aug_value_, this->aug_index_);
  return record;
}

// InlinedSqlResult.
bool InlinedSqlResult::IsQuery() const { return true; }

bool InlinedSqlResult::HasNext() {
  return this->index_ < this->records_.size();
}

dataflow::Record InlinedSqlResult::FetchOne(const dataflow::SchemaRef &schema) {
  return std::move(this->records_.at(this->index_++));
}
bool InlinedSqlResult::IsInlined() const { return true; }

std::unordered_set<std::string> InlinedSqlResult::RowSet() const {
  std::unordered_set<std::string> result;
  for (const auto &record : this->records_) {
    result.insert(StringifyRecord(record));
  }
  return result;
}
void InlinedSqlResult::AddRecord(dataflow::Record &&record) {
  this->records_.push_back(std::move(record));
}

}  // namespace mysql
}  // namespace pelton
