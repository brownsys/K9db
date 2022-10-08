// CREATE TABLE statements sharding and rewriting.

#ifndef PELTON_SHARDS_SQLENGINE_CREATE_H_
#define PELTON_SHARDS_SQLENGINE_CREATE_H_

#include <memory>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "pelton/connection.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/state.h"
#include "pelton/shards/state.h"
#include "pelton/shards/types.h"
#include "pelton/sql/abstract_connection.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/upgradable_lock.h"

namespace pelton {
namespace shards {
namespace sqlengine {

class CreateContext {
 public:
  CreateContext(const sqlast::CreateTable &stmt, Connection *conn,
                util::UniqueLock *lock)
      : stmt_(stmt),
        table_name_(stmt_.table_name()),
        conn_(conn),
        sstate_(conn->state->SharderState()),
        dstate_(conn->state->DataflowState()),
        db_(conn->state->Database()),
        lock_(lock),
        table_(table_name_, {}, stmt) {}

  /* Handling create: Resolves sharding and creates the physical table. */
  absl::StatusOr<sql::SqlResult> Exec();

 private:
  /* Helpers for finding annotations in the create table statement. */
  struct Annotations {
    std::vector<size_t> accessors;
    std::vector<size_t> explicit_owners;
    std::vector<size_t> implicit_owners;
    std::vector<size_t> owns;
    std::vector<size_t> accesses;
  };
  absl::StatusOr<Annotations> DiscoverValidate();

  /* Helpers for creating indices for transitive and variable ownership. */
  absl::StatusOr<IndexDescriptor> MakeIndex(const std::string &indexed_table,
                                            const std::string &indexed_column,
                                            const ShardDescriptor &next);
  absl::StatusOr<IndexDescriptor> MakeVIndex(const std::string &indexed_table,
                                             const std::string &indexed_column,
                                             const ShardDescriptor &origin);

  /* Helpers for creating ShardingDescriptor for ownership or accessorships. */
  // Direct / transitive ownership (along a forward-looking foreign key).
  std::vector<std::unique_ptr<ShardDescriptor>> MakeFDescriptors(
      bool owners, bool create_indices, const sqlast::ColumnDefinition &fk_col,
      size_t fk_column_index, sqlast::ColumnDefinition::Type fk_column_type);

  // OWNS/ACCESSES (backwards-looking foreign key).
  std::vector<std::unique_ptr<ShardDescriptor>> MakeBDescriptors(
      bool owners, bool create_indices, const Table &origin,
      const sqlast::ColumnDefinition &origin_col, size_t origin_index);

  /* Members. */
  // Statement being inserted.
  const sqlast::CreateTable &stmt_;
  const std::string &table_name_;

  // Pelton connection.
  Connection *conn_;

  // Connection components.
  SharderState &sstate_;
  dataflow::DataFlowState &dstate_;
  sql::AbstractConnection *db_;

  // Shared Lock so we can read from the states safetly.
  util::UniqueLock *lock_;

  // The table being created.
  Table table_;
  dataflow::SchemaRef schema_;
};

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_CREATE_H_
