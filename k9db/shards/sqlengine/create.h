// CREATE TABLE statements sharding and rewriting.

#ifndef K9DB_SHARDS_SQLENGINE_CREATE_H_
#define K9DB_SHARDS_SQLENGINE_CREATE_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "k9db/connection.h"
#include "k9db/dataflow/schema.h"
#include "k9db/dataflow/state.h"
#include "k9db/shards/state.h"
#include "k9db/shards/types.h"
#include "k9db/sql/connection.h"
#include "k9db/sql/result.h"
#include "k9db/sqlast/ast.h"
#include "k9db/util/upgradable_lock.h"

namespace k9db {
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

  /* Helpers for creating ShardingDescriptor for ownership or accessorships. */
  // Direct / transitive ownership (along a forward-looking foreign key).
  std::vector<std::unique_ptr<ShardDescriptor>> MakeFDescriptors(
      bool owners, bool create_indices, const sqlast::ColumnDefinition &fk_col,
      size_t fk_column_index, sqlast::ColumnDefinition::Type fk_column_type);

  // OWNS/ACCESSES (backwards-looking foreign key).
  std::vector<std::unique_ptr<ShardDescriptor>> MakeBDescriptors(
      bool owners, bool create_indices, Table *origin,
      const sqlast::ColumnDefinition &origin_col, size_t origin_index);

  // Validate that anonymization rules are valid.
  absl::Status ValidateAnonymizationRules(const Annotations &annotations) const;

  /* Members. */
  // Statement being inserted.
  const sqlast::CreateTable &stmt_;
  const std::string &table_name_;

  // K9db connection.
  Connection *conn_;

  // Connection components.
  SharderState &sstate_;
  dataflow::DataFlowState &dstate_;
  sql::Connection *db_;

  // Shared Lock so we can read from the states safetly.
  util::UniqueLock *lock_;

  // The table being created.
  Table table_;
  dataflow::SchemaRef schema_;
};

}  // namespace sqlengine
}  // namespace shards
}  // namespace k9db

#endif  // K9DB_SHARDS_SQLENGINE_CREATE_H_
