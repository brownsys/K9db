// Entry point for our sql rewriting engine.
#include "pelton/shards/sqlengine/engine.h"

#include <memory>
#include <utility>

#include "pelton/shards/sqlengine/create.h"
#include "pelton/shards/sqlengine/delete.h"
#include "pelton/shards/sqlengine/explain.h"
#include "pelton/shards/sqlengine/gdpr.h"
#include "pelton/shards/sqlengine/index.h"
#include "pelton/shards/sqlengine/insert.h"
#include "pelton/shards/sqlengine/replace.h"
#include "pelton/shards/sqlengine/select.h"
#include "pelton/shards/sqlengine/update.h"
#include "pelton/shards/sqlengine/view.h"
#include "pelton/shards/types.h"
#include "pelton/sqlast/ast.h"
#include "pelton/sqlast/parser.h"
#include "pelton/util/status.h"
#include "pelton/util/upgradable_lock.h"

namespace pelton {
namespace shards {
namespace sqlengine {

absl::StatusOr<sql::SqlResult> Shard(const std::string &sql,
                                     Connection *connection) {
  dataflow::DataFlowState &dstate = connection->state->DataflowState();

  // Parse with ANTLR into our AST.
  sqlast::SQLParser parser;
  // parse the statement, move result of parsing sql string to newly created
  // <sqlast::AbstractStatement> statement.
  MOVE_OR_RETURN(std::unique_ptr<sqlast::AbstractStatement> statement,
                 parser.Parse(sql));

  // Get type of sql statement, initialize stmt (statement) as the associated
  // type custom defined in pelton via the sqlast class. Then call Shard in the
  // appropriate file. Eg if CreateTable we call create::Shard() from
  // sqlengine/create.cc
  switch (statement->type()) {
    // Case 1: CREATE TABLE statement.
    case sqlast::AbstractStatement::Type::CREATE_TABLE: {
      auto *stmt = static_cast<sqlast::CreateTable *>(statement.get());
      util::UniqueLock lock = connection->state->WriterLock();
      CreateContext context(*stmt, connection, &lock);
      return context.Exec();
    }

    // Case 2: INSERT or REPLACE statement.
    case sqlast::AbstractStatement::Type::INSERT: {
      auto *stmt = static_cast<sqlast::Insert *>(statement.get());
      util::SharedLock lock = connection->state->ReaderLock();
      InsertContext context(*stmt, connection, &lock);
      return context.Exec();
    }
    case sqlast::AbstractStatement::Type::REPLACE: {
      auto *stmt = static_cast<sqlast::Replace *>(statement.get());
      util::SharedLock lock = connection->state->ReaderLock();
      ReplaceContext context(*stmt, connection, &lock);
      return context.Exec();
    }

    // Case 3: UPDATE statement.
    case sqlast::AbstractStatement::Type::UPDATE: {
      auto *stmt = static_cast<sqlast::Update *>(statement.get());
      util::SharedLock lock = connection->state->ReaderLock();
      UpdateContext context(*stmt, connection, &lock);
      return context.Exec();
    }

    // Case 4: SELECT statement.
    // Might be a select from a matview or a table.
    case sqlast::AbstractStatement::Type::SELECT: {
      auto *stmt = static_cast<sqlast::Select *>(statement.get());
      util::SharedLock lock = connection->state->ReaderLock();
      if (dstate.HasFlow(stmt->table_name())) {
        return view::SelectView(*stmt, connection, &lock);
      } else {
        util::SharedLock lock = connection->state->ReaderLock();
        SelectContext context(*stmt, connection, &lock);
        return context.Exec();
      }
    }

    // Case 5: DELETE statement.
    case sqlast::AbstractStatement::Type::DELETE: {
      auto *stmt = static_cast<sqlast::Delete *>(statement.get());
      util::SharedLock lock = connection->state->ReaderLock();
      DeleteContext context(*stmt, connection, &lock);
      return context.Exec();
    }

    // Case 6: CREATE VIEW statement (e.g. dataflow).
    case sqlast::AbstractStatement::Type::CREATE_VIEW: {
      auto *stmt = static_cast<sqlast::CreateView *>(statement.get());
      util::UniqueLock lock = connection->state->WriterLock();
      CHECK_STATUS(view::CreateView(*stmt, connection, &lock));
      return sql::SqlResult(true);
    }

    // Case 7: CREATE INDEX statement.
    case sqlast::AbstractStatement::Type::CREATE_INDEX: {
      auto *stmt = static_cast<sqlast::CreateIndex *>(statement.get());
      util::SharedLock lock = connection->state->ReaderLock();
      auto db = connection->state->Database();
      return sql::SqlResult(db->ExecuteCreateIndex(*stmt));
    }

    // Case 8: GDPR (GET | FORGET) statements.
    case sqlast::AbstractStatement::Type::GDPR: {
      auto *stmt = static_cast<sqlast::GDPRStatement *>(statement.get());
      util::SharedLock lock = connection->state->ReaderLock();
      GDPRContext context(*stmt, connection, &lock);
      return context.Exec();
    }

    // Case 9: EXPLAIN <query> statements.
    case sqlast::AbstractStatement::Type::EXPLAIN_QUERY: {
      auto *stmt = static_cast<sqlast::ExplainQuery *>(statement.get());
      util::SharedLock lock = connection->state->ReaderLock();
      ExplainContext context(*stmt, connection, &lock);
      return context.Exec();
    }

    // Unsupported (this should not be reachable).
    default:
      return absl::InvalidArgumentError("Unsupported statement type");
  }
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
