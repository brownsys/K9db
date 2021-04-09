// Entry point for our sql rewriting engine.

#include "pelton/shards/sqlengine/engine.h"

#include <memory>
#include <utility>

#include "pelton/shards/sqlengine/create.h"
#include "pelton/shards/sqlengine/delete.h"
#include "pelton/shards/sqlengine/insert.h"
#include "pelton/shards/sqlengine/select.h"
#include "pelton/shards/sqlengine/update.h"
#include "pelton/shards/sqlengine/view.h"
#include "pelton/sqlast/ast.h"
#include "pelton/sqlast/parser.h"
#include "pelton/util/perf.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {

absl::StatusOr<SqlResult> Shard(const std::string &sql, SharderState *state,
                                dataflow::DataFlowState *dataflow_state) {
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
      CHECK_STATUS(create::Shard(*stmt, state, dataflow_state));
      return SqlResult();
    }

    // Case 2: Insert statement.
    case sqlast::AbstractStatement::Type::INSERT: {
      auto *stmt = static_cast<sqlast::Insert *>(statement.get());
      CHECK_STATUS(insert::Shard(*stmt, state, dataflow_state));
      return SqlResult();
    }

    // Case 3: Update statement.
    case sqlast::AbstractStatement::Type::UPDATE: {
      auto *stmt = static_cast<sqlast::Update *>(statement.get());
      CHECK_STATUS(update::Shard(*stmt, state, dataflow_state));
      return SqlResult();
    }

    // Case 4: Select statement.
    // Might be a select from a matview or a table.
    case sqlast::AbstractStatement::Type::SELECT: {
      auto *stmt = static_cast<sqlast::Select *>(statement.get());
      if (dataflow_state->HasFlow(stmt->table_name())) {
        return view::SelectView(*stmt, state, dataflow_state);
      } else {
        return select::Shard(*stmt, state, dataflow_state);
      }
    }

    // Case 5: Delete statement.
    case sqlast::AbstractStatement::Type::DELETE: {
      auto *stmt = static_cast<sqlast::Delete *>(statement.get());
      CHECK_STATUS(delete_::Shard(*stmt, state, dataflow_state));
      return SqlResult();
    }

    // Case 6: CREATE VIEW statement (e.g. dataflow).
    case sqlast::AbstractStatement::Type::CREATE_VIEW: {
      auto *stmt = static_cast<sqlast::CreateView *>(statement.get());
      CHECK_STATUS(view::CreateView(*stmt, state, dataflow_state));
      return SqlResult();
    }

    // Unsupported (this should not be reachable).
    default:
      return absl::InvalidArgumentError("Unsupported statement type");
  }
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
