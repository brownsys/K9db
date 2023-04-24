#ifndef K9DB_SQL_ROCKSDB_FILTER_H__
#define K9DB_SQL_ROCKSDB_FILTER_H__

#include "k9db/dataflow/record.h"
#include "k9db/dataflow/schema.h"
#include "k9db/sqlast/value_mapper.h"

namespace k9db {
namespace sql {
namespace rocks {

bool InMemoryFilter(const sqlast::ValueMapper &value_mapper,
                    const dataflow::Record &record);

}  // namespace rocks
}  // namespace sql
}  // namespace k9db

#endif  // K9DB_SQL_ROCKSDB_FILTER_H__
