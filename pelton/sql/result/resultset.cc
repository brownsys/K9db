#include "pelton/sql/result/resultset.h"

#include <utility>

#include "glog/logging.h"

namespace pelton {
namespace sql {
namespace _result {

namespace {

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
SqlResultSet::SqlResultSet(const dataflow::SchemaRef &schema)
    : index_(0), lazy_data_(), current_result_(), schema_(schema) {}

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
  for (LazyResultSet &lazy : other.lazy_data_) {
    this->lazy_data_.push_back(std::move(lazy));
  }
}

// Query API.
bool SqlResultSet::HasNext() {
  if (this->current_result_ != nullptr && this->current_result_->next()) {
    return true;
  }
  while (this->index_ < this->lazy_data_.size()) {
    this->ExecuteLazy();
    if (this->current_result_->next()) {
      return true;
    }
  }
  return false;
}

dataflow::Record SqlResultSet::FetchOne() {
  return SqlRowToRecord(this->current_result_.get(),
                        this->lazy_data_.at(this->index_ - 1).augment_info,
                        this->schema_);
}

// Actually execute the lazy sql command against the shards.
// TODO(babman): actually call the executor from here etc.
void SqlResultSet::ExecuteLazy() {
  LazyResultSet lazy_info = this->lazy_data_.at(this->index_);
  this->index_++;
  // this->current_result_ = execute(lazy_info.sql);
}

}  // namespace _result
}  // namespace sql
}  // namespace pelton
