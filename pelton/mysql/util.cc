#include "pelton/mysql/util.h"

#include <memory>
#include <string>

#include "glog/logging.h"
#include "mysql-cppconn-8/jdbc/cppconn/datatype.h"

namespace pelton {
namespace mysql {

// Checks the type of a value in a result set.
bool IsStringType(const sql::ResultSet &result, size_t col_index) {
  int type = result.getMetaData()->getColumnType(col_index);
  return type == sql::DataType::VARCHAR || type == sql::DataType::CHAR ||
         type == sql::DataType::LONGVARCHAR;
}

// Transform a mysql value to one compatible with our record format.
void MySqlValueIntoRecord(const sql::ResultSet &result, size_t col_index,
                          size_t target_index, dataflow::Record *record) {
  switch (record->schema().TypeOf(target_index)) {
    case sqlast::ColumnDefinition::Type::TEXT: {
      std::string val = result.getString(col_index);
      record->SetString(std::make_unique<std::string>(val), target_index);
      break;
    }
    case sqlast::ColumnDefinition::Type::INT: {
      if (IsStringType(result, col_index)) {
        const std::string &val = result.getString(col_index).asStdString();
        record->SetValue(val, target_index);
      } else {
        record->SetInt(result.getInt64(col_index), target_index);
      }
      break;
    }
    case sqlast::ColumnDefinition::Type::UINT: {
      if (IsStringType(result, col_index)) {
        const std::string &val = result.getString(col_index).asStdString();
        record->SetValue(val, target_index);
      } else {
        record->SetInt(result.getUInt64(col_index), target_index);
      }
      break;
    }
    default:
      LOG(FATAL) << "Unsupported type in MySqlValueIntoRecord "
                 << record->schema().TypeOf(target_index);
  }
}

// Transform a mysql row into a complete record.
dataflow::Record MySqlRowToRecord(const sql::ResultSet &result,
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
dataflow::Record MySqlRowToRecordSkipping(const sql::ResultSet &result,
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
