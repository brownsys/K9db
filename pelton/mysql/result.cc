#include "pelton/mysql/result.h"

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
void SqlResult::Append(SqlResult &&other) {
  if (other.hasData()) {
    if (!this->hasData()) {
      this->impl_ = std::move(other.impl_);
      this->schema_ = other.schema_;
    } else {
      InlinedSqlResult *ptr =
          reinterpret_cast<InlinedSqlResult *>(this->impl_.get());
      while (other.hasData()) {
        ptr->AddRecord(other.fetchOne());
      }
    }
  }
}

void SqlResult::AppendDeduplicate(SqlResult &&other) {
  if (other.hasData()) {
    if (!this->hasData()) {
      this->impl_ = std::move(other.impl_);
      this->schema_ = other.schema_;
    } else {
      InlinedSqlResult *ptr =
          reinterpret_cast<InlinedSqlResult *>(this->impl_.get());
      // Only add unduplicated rows.
      std::unordered_set<std::string> row_set = ptr->RowSet();
      while (other.hasData()) {
        dataflow::Record record = other.fetchOne();
        if (row_set.count(StringifyRecord(record)) == 0) {
          ptr->AddRecord(std::move(record));
        }
      }
    }
  }
}

void SqlResult::MakeInline() {
  if (this->impl_ && !this->impl_->isInlined()) {
    std::vector<dataflow::Record> records;
    while (this->impl_->hasData()) {
      records.push_back(std::move(this->fetchOne()));
    }
    this->impl_ = std::make_unique<InlinedSqlResult>(std::move(records));
  }
}

// MySqlResult.
bool MySqlResult::hasData() {
  return this->result_.hasData() && this->result_.count() > 0;
}

dataflow::Record MySqlResult::fetchOne(const dataflow::SchemaRef &schema) {
  mysqlx::Row row = this->result_.fetchOne();
  return MySqlRowToRecord(std::move(row), schema, false);
}
bool MySqlResult::isInlined() const { return false; }

// AugmentedSqlResult.
bool AugmentedSqlResult::hasData() {
  return this->result_.hasData() && this->result_.count() > 0;
}
dataflow::Record AugmentedSqlResult::fetchOne(
    const dataflow::SchemaRef &schema) {
  mysqlx::Row row = this->result_.fetchOne();
  dataflow::Record record =
      MySqlRowToRecordSkipping(std::move(row), schema, false, this->aug_index_);
  MySqlValueIntoRecord(*this->aug_value_, this->aug_index_, &record);
  return record;
}
bool AugmentedSqlResult::isInlined() const { return false; }

// InlinedSqlResult.
bool InlinedSqlResult::hasData() {
  return this->index_ < this->records_.size();
}
dataflow::Record InlinedSqlResult::fetchOne(const dataflow::SchemaRef &schema) {
  return std::move(this->records_.at(this->index_++));
}
bool InlinedSqlResult::isInlined() const { return true; }

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
