#include "pelton/sql/result/resultset.h"

#include <utility>

#include "glog/logging.h"

namespace pelton {
namespace sql {
namespace _result {

namespace {

// Turn record into a concatenated string.
std::string StringifyRecord(const dataflow::Record &record) {
  std::string delim("\0", 1);
  std::string str = "";
  for (size_t i = 0; i < record.schema().size(); i++) {
    str += record.GetValueString(i) + delim;
  }
  return str;
}

// Checks the type of a value in a result set.
bool IsStringType(::sql::ResultSetMetaData *meta, size_t col_index) {
  int type = meta->getColumnType(col_index);
  return type == ::sql::DataType::VARCHAR || type == ::sql::DataType::CHAR ||
         type == ::sql::DataType::LONGVARCHAR ||
         type == ::sql::DataType::NCHAR || type == ::sql::DataType::NVARCHAR ||
         type == ::sql::DataType::LONGNVARCHAR;
}

// Transform a mysql value to one compatible with our record format.
// Mariadb result index starts at 1.
void SqlValueIntoRecord(::sql::ResultSet *result, size_t index_in_result,
                        size_t index_in_record, dataflow::Record *record) {
  std::unique_ptr<::sql::ResultSetMetaData> meta{result->getMetaData()};

  switch (record->schema().TypeOf(index_in_record)) {
    case sqlast::ColumnDefinition::Type::TEXT: {
      ::sql::SQLString sql_val = result->getString(index_in_result);
      std::string val{sql_val.begin(), sql_val.end()};
      record->SetString(std::make_unique<std::string>(std::move(val)),
                        index_in_record);
      break;
    }
    case sqlast::ColumnDefinition::Type::INT: {
      if (IsStringType(meta.get(), index_in_result)) {
        ::sql::SQLString sql_val = result->getString(index_in_result);
        std::string val{sql_val.begin(), sql_val.end()};
        record->SetValue(val, index_in_record);
      } else {
        record->SetInt(result->getInt64(index_in_result), index_in_record);
      }
      break;
    }
    case sqlast::ColumnDefinition::Type::UINT: {
      if (IsStringType(meta.get(), index_in_result)) {
        ::sql::SQLString sql_val = result->getString(index_in_result);
        std::string val{sql_val.begin(), sql_val.end()};
        record->SetValue(val, index_in_record);
      } else {
        record->SetUInt(result->getUInt64(index_in_result), index_in_record);
      }
      break;
    }
    case sqlast::ColumnDefinition::Type::DATETIME: {
      switch (meta->getColumnType(index_in_result)) {
        case ::sql::DataType::TIMESTAMP: {
          ::sql::SQLString sql_val = result->getString(index_in_result);
          std::string val{sql_val.begin(), sql_val.end()};
          record->SetDateTime(std::make_unique<std::string>(std::move(val)),
                              index_in_record);
          break;
        }
        default:
          LOG(FATAL) << "Incompatible SQLType "
                     << meta->getColumnTypeName(index_in_result) << " #"
                     << meta->getColumnType(index_in_result)
                     << " with Pelton's DATETIME";
      }
      break;
    }
    default:
      LOG(FATAL) << "Unsupported type in SqlValueIntoRecord() "
                 << record->schema().TypeOf(index_in_record);
  }
}

// Transform a mysql row into a record with some value at a given index skipped,
// this is used to create a record for mysql row that are augmented with an
// external value from outside of the MySQL DB.
dataflow::Record SqlRowToRecord(
    ::sql::ResultSet *result,
    const std::vector<AugmentingInformation> &augment_info,
    const dataflow::SchemaRef &schema, bool positive = false) {
  dataflow::Record record(schema, positive);
  // Fill in augmented values first.
  std::vector<bool> is_augmented(schema.size(), false);
  for (const AugmentingInformation &aug : augment_info) {
    record.SetValue(aug.value, aug.index);
    is_augmented.at(aug.index) = true;
  }
  // Fill in values from underlying result. Value index needs translation
  // since underlying result has no augmentation.
  size_t augmented_count = 0;
  for (size_t k = 0; k < schema.size(); k++) {
    if (is_augmented.at(k)) {
      augmented_count++;
      continue;
    }
    size_t i = k - augmented_count;
    SqlValueIntoRecord(result, i + 1, k, &record);
  }
  return record;
}

}  // namespace

// Constructor.
SqlResultSet::SqlResultSet(const dataflow::SchemaRef &schema,
                           SqlEagerExecutor *eager_executor)
    : lazy_data_(),
      current_augment_info_(),
      current_result_(),
      current_record_(schema),
      current_record_ready_(false),
      schema_(schema),
      deduplicate_(false),
      duplicates_(),
      eager_executor_(eager_executor) {}

// Adding additional results to this set.
void SqlResultSet::AddShardResult(LazyResultSet &&lazy_result_set) {
  this->lazy_data_.push_back(std::move(lazy_result_set));
}
void SqlResultSet::Append(SqlResultSet &&other, bool deduplicate) {
  // TODO(babman): mark that results should be deduplicated and perform
  // deduplication online.
  CHECK(this->schema_ == other.schema_) << "Appending unequal schemas";
  // Perform the appending by putting the lazy shard results from other at the
  // end of the current data.
  this->lazy_data_.splice(this->lazy_data_.end(), other.lazy_data_);
  this->deduplicate_ = this->deduplicate_ || deduplicate;
}

// Query API.
bool SqlResultSet::HasNext() {
  if (current_record_ready_) {
    return true;
  }
  return this->GetNext();
}
dataflow::Record SqlResultSet::FetchOne() {
  if (!this->current_record_ready_) {
    if (!this->GetNext()) {
      LOG(FATAL) << ".FetchOne() called on empty SqlResultSet";
    }
  }
  this->current_record_ready_ = false;
  return std::move(this->current_record_);
}

// Get the next record, from current_result_ or from the next result(s)
// found by executing the next LazyResultSet(s). Store record in
// current_record_.
bool SqlResultSet::GetNext() {
  while (!this->lazy_data_.empty() || this->current_result_ != nullptr) {
    while (this->current_result_ != nullptr && this->current_result_->next()) {
      this->current_record_ =
          SqlRowToRecord(this->current_result_.get(),
                         this->current_augment_info_, this->schema_);
      // Deduplicate if configured.
      std::string record_str = StringifyRecord(this->current_record_);
      if (this->duplicates_.count(record_str) == 0) {
        if (this->deduplicate_) {
          this->duplicates_.insert(std::move(record_str));
        }
        this->current_record_ready_ = true;
        return true;
      }
    }
    this->Execute();
  }
  return false;
}

// Actually execute SQL statement in LazyResultSet against the shards.
// TODO(babman): actually call the executor from here etc.
void SqlResultSet::Execute() {
  this->current_result_ = std::unique_ptr<::sql::ResultSet>(nullptr);
  this->current_record_ready_ = false;
  this->current_augment_info_.clear();
  if (!this->lazy_data_.empty()) {
    LazyResultSet &lazy_info = this->lazy_data_.front();
    this->current_augment_info_ = std::move(lazy_info.augment_info);
    this->current_result_ = this->eager_executor_->ExecuteQuery(lazy_info.sql);
    this->lazy_data_.pop_front();
  }
}

}  // namespace _result
}  // namespace sql
}  // namespace pelton
