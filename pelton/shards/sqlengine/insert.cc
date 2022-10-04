// INSERT statements sharding and rewriting.
#include "pelton/shards/sqlengine/insert.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

// #include "pelton/shards/sqlengine/select.h"
// #include "pelton/shards/sqlengine/util.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/state.h"
#include "pelton/dataflow/value.h"
#include "pelton/shards/sqlengine/index.h"
#include "pelton/shards/state.h"
#include "pelton/shards/types.h"
#include "pelton/sql/abstract_connection.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace insert {

#define ACC_OR_RET(status, acc)                  \
  if (status < 0) return sql::SqlResult(status); \
  acc += status

absl::StatusOr<sql::SqlResult> Shard(const sqlast::Insert &stmt,
                                     Connection *connection,
                                     util::SharedLock *lock) {
  SharderState &sstate = connection->state->SharderState();
  dataflow::DataFlowState &dstate = connection->state->DataflowState();
  sql::AbstractConnection *db = connection->state->Database();

  // Make sure table exists in the schema first.
  const std::string &table_name = stmt.table_name();
  ASSERT_RET(sstate.TableExists(table_name), InvalidArgument,
             "Table does not exist");

  // Get table schema.
  dataflow::SchemaRef schema = dstate.GetTableSchema(table_name);

  // If replace, we need to delete the old record first (if exists), then
  // insert the new one.
  std::vector<dataflow::Record> records;
  if (stmt.replace()) {
    return absl::InvalidArgumentError("REPLACE unsupported");
  }

  // Now, we need to insert.
  // We insert a copy for each OWNER.
  int result = 0;
  const Table &table = sstate.GetTable(table_name);
  for (size_t copy_index = 0; copy_index < table.owners.size(); copy_index++) {
    const std::unique_ptr<ShardDescriptor> &desc = table.owners.at(copy_index);

    // Identify value of the column along which we are sharding.
    size_t column_index = EXTRACT_VARIANT(column_index, desc->info);
    dataflow::Value val(schema.TypeOf(column_index),
                        stmt.GetByColumnOrIndex(
                            EXTRACT_VARIANT(column, desc->info), column_index));
    ASSERT_RET(!val.IsNull(), Internal, "OWNER cannot be NULL");

    // Handle according to sharding type.
    switch (desc->type) {
      case InfoType::DIRECT: {
        int r = db->ExecuteInsert(stmt, val.AsUnquotedString(), copy_index);
        ACC_OR_RET(r, result);
        break;
      }
      case InfoType::TRANSITIVE: {
        // Insert into shards of users as determined via transitive index.
        const TransitiveInfo &info = std::get<TransitiveInfo>(desc->info);
        std::vector<dataflow::Record> indexed =
            index::LookupIndex(*info.index, std::move(val), connection, lock);

        // We know we dont have duplicates because index does group by.
        for (dataflow::Record &record : indexed) {
          ASSERT_RET(record.GetUInt(2) > 0, Internal, "Index count 0");
          ASSERT_RET(!record.IsNull(1), Internal, "T Index gives NULL owner");
          int r = db->ExecuteInsert(stmt, record.GetValueString(1), copy_index);
          ACC_OR_RET(r, result);
        }

        // Inserting this row before inserting the transitive row pointing to
        // its owner is an integrity error.
        ASSERT_RET(indexed.size() > 0, InvalidArgument,
                   "Integrity error: dangling ownerFK");
        break;
      }
      case InfoType::VARIABLE: {
        const VariableInfo &info = std::get<VariableInfo>(desc->info);
        std::vector<dataflow::Record> indexed =
            index::LookupIndex(*info.index, std::move(val), connection, lock);

        // We know we dont have duplicates because index does group by.
        for (dataflow::Record &record : indexed) {
          ASSERT_RET(record.GetUInt(2) > 0, Internal, "Index count 0");
          ASSERT_RET(!record.IsNull(1), Internal, "T Index gives NULL owner");
          int r = db->ExecuteInsert(stmt, record.GetValueString(1), copy_index);
          ACC_OR_RET(r, result);
        }

        // This row may be inserted before the corresponding many-to-many
        // rows are inserted into the variable ownership assocation table.
        if (indexed.size() == 0) {
          int r = db->ExecuteInsert(stmt, DEFAULT_SHARD, copy_index);
          ACC_OR_RET(r, result);
        }
        break;
      }
      default:
        return absl::InternalError("Unreachable sharding case");
    }

    // We just inserted a new user!
    if (desc->shard_kind == table_name) {
      sstate.IncrementUsers(desc->shard_kind);
    }
  }

  // If no OWNERs detected, we insert into global/default shard.
  if (table.owners.size() == 0) {
    int status = db->ExecuteInsert(stmt, DEFAULT_SHARD, 0);
    if (status < 0) {
      return sql::SqlResult(status);
    }
    result += status;
  }

  // Everything has been inserted; feed to dataflows.
  records.push_back(dstate.CreateRecord(stmt));
  dstate.ProcessRecords(table_name, std::move(records));

  // Return number of copies inserted.
  return sql::SqlResult(result);
}

}  // namespace insert
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
