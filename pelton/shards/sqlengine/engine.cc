// Entry point for our sql rewriting engine.

#include "pelton/shards/sqlengine/engine.h"

#include <memory>
#include <utility>

#include "pelton/shards/sqlengine/create.h"
#include "pelton/shards/sqlengine/delete.h"
#include "pelton/shards/sqlengine/gdpr.h"
#include "pelton/shards/sqlengine/index.h"
#include "pelton/shards/sqlengine/insert.h"
#include "pelton/shards/sqlengine/select.h"
#include "pelton/shards/sqlengine/update.h"
#include "pelton/shards/sqlengine/view.h"
#include "pelton/shards/state.h"
#include "pelton/sqlast/ast.h"
#include "pelton/sqlast/parser.h"
#include "pelton/util/perf.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {

absl::StatusOr<sql::SqlResult> Shard(const std::string &sql,
                                     Connection *connection,
                                     std::string *shard_kind,
                                     std::string *user_id) {
  dataflow::DataFlowState *dataflow_state =
      connection->pelton_state->GetDataFlowState();

  // Parse with ANTLR into our AST.
  perf::Start("parsing");
  sqlast::SQLParser parser;
  MOVE_OR_RETURN(std::unique_ptr<sqlast::AbstractStatement> statement,
                 parser.Parse(sql));
  perf::End("parsing");

  // Compute result.
  switch (statement->type()) {
    // Case 1: CREATE TABLE statement.
    case sqlast::AbstractStatement::Type::CREATE_TABLE: {
      auto *stmt = static_cast<sqlast::CreateTable *>(statement.get());
      return create::Shard(*stmt, connection);
    }

    // Case 2: Insert statement.
    case sqlast::AbstractStatement::Type::INSERT: {
      auto *stmt = static_cast<sqlast::Insert *>(statement.get());
      SharderStateLock state_lock = connection->pelton_state->GetSharderState()->LockShared();
      return insert::Shard(*stmt, connection, &state_lock);
    }

    // Case 3: Update statement.
    case sqlast::AbstractStatement::Type::UPDATE: {
      auto *stmt = static_cast<sqlast::Update *>(statement.get());
      return update::Shard(*stmt, connection);
    }

    // Case 4: Select statement.
    // Might be a select from a matview or a table.
    case sqlast::AbstractStatement::Type::SELECT: {
      auto *stmt = static_cast<sqlast::Select *>(statement.get());
      if (dataflow_state->HasFlow(stmt->table_name())) {
        return view::SelectView(*stmt, connection);
      } else {
        return select::Shard(*stmt, connection);
      }
    }

    // Case 5: Delete statement.
    case sqlast::AbstractStatement::Type::DELETE: {
      auto *stmt = static_cast<sqlast::Delete *>(statement.get());
      return delete_::Shard(*stmt, connection, true);
    }

    // Case 6: CREATE VIEW statement (e.g. dataflow).
    case sqlast::AbstractStatement::Type::CREATE_VIEW: {
      auto *stmt = static_cast<sqlast::CreateView *>(statement.get());
      CHECK_STATUS(view::CreateView(*stmt, connection));
      return sql::SqlResult(true);
    }

    // Case 7: CREATE INEDX statement.
    case sqlast::AbstractStatement::Type::CREATE_INDEX: {
      auto *stmt = static_cast<sqlast::CreateIndex *>(statement.get());
      return index::CreateIndex(*stmt, connection);
    }

    // Case 8: GDPR (GET | FORGET) statements.
    case sqlast::AbstractStatement::Type::GDPR: {
      auto *stmt = static_cast<sqlast::GDPRStatement *>(statement.get());
      return gdpr::Shard(*stmt, connection);
    }

    // Unsupported (this should not be reachable).
    default:
      return absl::InvalidArgumentError("Unsupported statement type");
  }
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
