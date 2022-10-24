// This file contains utility functions, mostly for standarizing
// name (suffixes etc) of things.
//
// TODO(babman): All these functions should be gone and we should make them
// part of the AST by improving annotations and making AST typed.

#ifndef PELTON_SHARDS_SQLENGINE_UTIL_H_
#define PELTON_SHARDS_SQLENGINE_UTIL_H_

#include <string>
#include <vector>

#include "pelton/connection.h"
#include "pelton/dataflow/record.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/shard_name.h"
#include "pelton/util/upgradable_lock.h"

namespace pelton {
namespace shards {
namespace sqlengine {

bool IsADataSubject(const sqlast::CreateTable &stmt);

bool IsOwner(const sqlast::ColumnDefinition &col);
bool IsAccessor(const sqlast::ColumnDefinition &col);
bool IsOwns(const sqlast::ColumnDefinition &col);
bool IsAccesses(const sqlast::ColumnDefinition &col);

// Determine which shards the given record reside in.
enum class ShardLocation { NO_SHARD, NOT_IN_GIVEN_SHARD, IN_GIVEN_SHARD };

std::vector<ShardLocation> Locate(const std::string &table_name,
                                  const util::ShardName &shard_name,
                                  const std::vector<dataflow::Record> &records,
                                  Connection *conn, util::SharedLock *lock);

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_UTIL_H_
