#ifndef PELTON_SQL_ROCKSDB_PROJECT_H__
#define PELTON_SQL_ROCKSDB_PROJECT_H__

#include <string>
// NOLINTNEXTLINE
#include <variant>
#include <vector>

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"

namespace pelton {
namespace sql {

struct Projection {
  dataflow::SchemaRef schema;
  std::vector<std::variant<uint32_t, sqlast::Value>> projections;
};

Projection ProjectionSchema(
    const dataflow::SchemaRef &table_schema,
    const std::vector<sqlast::Select::ResultColumn> &columns);

dataflow::Record Project(const Projection &project,
                         const dataflow::Record &record);

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_ROCKSDB_PROJECT_H__
