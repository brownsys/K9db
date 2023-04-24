#ifndef K9DB_SQL_ROCKSDB_PROJECT_H__
#define K9DB_SQL_ROCKSDB_PROJECT_H__

#include <string>
// NOLINTNEXTLINE
#include <variant>
#include <vector>

#include "k9db/dataflow/record.h"
#include "k9db/dataflow/schema.h"

namespace k9db {
namespace sql {
namespace rocks {

struct Projection {
  dataflow::SchemaRef schema;
  std::vector<std::variant<uint32_t, sqlast::Value>> projections;
};

Projection ProjectionSchema(
    const dataflow::SchemaRef &table_schema,
    const std::vector<sqlast::Select::ResultColumn> &columns);

dataflow::Record Project(const Projection &project,
                         const dataflow::Record &record);

}  // namespace rocks
}  // namespace sql
}  // namespace k9db

#endif  // K9DB_SQL_ROCKSDB_PROJECT_H__
