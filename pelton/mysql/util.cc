#include "pelton/mysql/util.h"

#include <memory>
#include <string>

#include "glog/logging.h"

namespace pelton {
namespace mysql {

// Transform a mysql value to one compatible with our record format.
void MySqlValueIntoRecord(const mysqlx::Value &value, size_t i,
                          dataflow::Record *record) {
  switch (record->schema().TypeOf(i)) {
    case sqlast::ColumnDefinition::Type::TEXT:
      record->SetString(std::make_unique<std::string>(value), i);
      break;
    case sqlast::ColumnDefinition::Type::INT:
      if (value.getType() == mysqlx::Value::STRING) {
        record->SetValue(value.get<std::string>(), i);
      } else {
        record->SetInt(value, i);
      }
      break;
    case sqlast::ColumnDefinition::Type::UINT:
      if (value.getType() == mysqlx::Value::STRING) {
        record->SetValue(value.get<std::string>(), i);
      } else {
        record->SetUInt(value, i);
      }
      break;
    default:
      LOG(FATAL) << "Unsupported type in MySqlValueIntoRecord "
                 << record->schema().TypeOf(i);
  }
}

// Transform a mysql row into a complete record.
dataflow::Record MySqlRowToRecord(mysqlx::Row &&row,
                                  const dataflow::SchemaRef schema,
                                  bool positive) {
  dataflow::Record record(schema, positive);
  for (size_t i = 0; i < schema.size(); i++) {
    MySqlValueIntoRecord(row.get(i), i, &record);
  }
  return record;
}

// Transform a mysql row into a record with some value at a given index skipped,
// this is used to create a record for mysql row that are augmented with an
// external value from outside of the MySQL DB.
dataflow::Record MySqlRowToRecordSkipping(mysqlx::Row &&row,
                                          const dataflow::SchemaRef schema,
                                          bool positive, size_t skip_index) {
  dataflow::Record record(schema, positive);
  for (size_t k = 0; k < schema.size(); k++) {
    if (k != skip_index) {
      size_t i = k < skip_index ? k : (k - 1);
      MySqlValueIntoRecord(row.get(i), k, &record);
    }
  }
  return record;
}

}  // namespace mysql
}  // namespace pelton
