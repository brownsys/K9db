#ifndef PELTON_SQL_ROCKSDB_FILTER_H__
#define PELTON_SQL_ROCKSDB_FILTER_H__

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sqlast/value_mapper.h"

namespace pelton {
namespace sql {
namespace rocks {

bool InMemoryFilter(const sqlast::ValueMapper &value_mapper,
                    const dataflow::Record &record);

}  // namespace rocks
}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_ROCKSDB_FILTER_H__
