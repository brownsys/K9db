#ifndef PELTON_MYSQL_UTIL_H_
#define PELTON_MYSQL_UTIL_H_

#include "mariadb/conncpp.hpp"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"

namespace pelton {
namespace mysql {

// Checks the type of a value in a result set.
bool IsStringType(sql::ResultSetMetaData *meta, size_t col_index);

// Transform a mysql value to one compatible with our record format.
void MySqlValueIntoRecord(sql::ResultSet *result, size_t col_index,
                          size_t target_index, dataflow::Record *record);

// Transform a mysql row into a complete record.
dataflow::Record MySqlRowToRecord(sql::ResultSet *result,
                                  const dataflow::SchemaRef schema,
                                  bool positive);

// Transform a mysql row into a record with some value at a given index skipped,
// this is used to create a record for mysql row that are augmented with an
// external value from outside of the MySQL DB.
dataflow::Record MySqlRowToRecordSkipping(sql::ResultSet *result,
                                          const dataflow::SchemaRef schema,
                                          bool positive, size_t skip_index);

}  // namespace mysql
}  // namespace pelton

#endif  // PELTON_MYSQL_UTIL_H_
