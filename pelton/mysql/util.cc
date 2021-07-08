#include "pelton/mysql/util.h"

#include <memory>
#include <string>
#include <utility>

#include "glog/logging.h"

namespace pelton {
namespace mysql {

// Checks the type of a value in a result set.
bool IsStringType(sql::ResultSetMetaData *meta, size_t col_index) {
  int type = meta->getColumnType(col_index);
  return type == sql::DataType::VARCHAR || type == sql::DataType::CHAR ||
         type == sql::DataType::LONGVARCHAR || type == sql::DataType::NCHAR ||
         type == sql::DataType::NVARCHAR || type == sql::DataType::LONGNVARCHAR;
}

// Transform a mysql value to one compatible with our record format.
void MySqlValueIntoRecord(sql::ResultSet *result, size_t col_index,
                          size_t target_index, dataflow::Record *record) {
  std::unique_ptr<sql::ResultSetMetaData> meta{result->getMetaData()};
  switch (record->schema().TypeOf(target_index)) {
    case sqlast::ColumnDefinition::Type::TEXT: {
      sql::SQLString sql_val = result->getString(col_index);
      std::string val{sql_val.begin(), sql_val.end()};
      record->SetString(std::make_unique<std::string>(std::move(val)),
                        target_index);
      break;
    }
    case sqlast::ColumnDefinition::Type::INT: {
      if (IsStringType(meta.get(), col_index)) {
        sql::SQLString sql_val = result->getString(col_index);
        std::string val{sql_val.begin(), sql_val.end()};
        record->SetValue(val, target_index);
      } else {
        record->SetInt(result->getInt64(col_index), target_index);
      }
      break;
    }
    case sqlast::ColumnDefinition::Type::UINT: {
      if (IsStringType(meta.get(), col_index)) {
        sql::SQLString sql_val = result->getString(col_index);
        std::string val{sql_val.begin(), sql_val.end()};
        record->SetValue(val, target_index);
      } else {
        record->SetUInt(result->getUInt64(col_index), target_index);
      }
      break;
    }
    case sqlast::ColumnDefinition::Type::DATETIME: {
      switch (meta->getColumnType(col_index)) {
        case sql::DataType::TIMESTAMP: {
          sql::SQLString sql_val = result->getString(col_index);
          std::string val{sql_val.begin(), sql_val.end()};
          record->SetDateTime(std::make_unique<std::string>(std::move(val)),
                              target_index);
          break;
        }
        default:
          LOG(FATAL) << "Incompatible SQLType "
                     << meta->getColumnTypeName(col_index) << " #"
                     << meta->getColumnType(col_index)
                     << " with Pelton's DATETIME";
      }
      break;
    }
    default:
      LOG(FATAL) << "Unsupported type in MySqlValueIntoRecord "
                 << record->schema().TypeOf(target_index);
  }
}

// Transform a mysql row into a complete record.
dataflow::Record MySqlRowToRecord(sql::ResultSet *result,
                                  const dataflow::SchemaRef schema,
                                  bool positive) {
  dataflow::Record record(schema, positive);
  for (size_t i = 0; i < schema.size(); i++) {
    MySqlValueIntoRecord(result, i + 1, i, &record);
  }
  return record;
}

// Transform a mysql row into a record with some value at a given index skipped,
// this is used to create a record for mysql row that are augmented with an
// external value from outside of the MySQL DB.
dataflow::Record MySqlRowToRecordSkipping(sql::ResultSet *result,
                                          const dataflow::SchemaRef schema,
                                          bool positive, size_t skip_index) {
  dataflow::Record record(schema, positive);
  for (size_t k = 0; k < schema.size(); k++) {
    if (k != skip_index) {
      size_t i = k < skip_index ? k : (k - 1);
      MySqlValueIntoRecord(result, i + 1, k, &record);
    }
  }
  return record;
}

}  // namespace mysql
}  // namespace pelton