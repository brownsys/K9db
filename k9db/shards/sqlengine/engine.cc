// Entry point for our sql rewriting engine.
#include "k9db/shards/sqlengine/engine.h"

#include <memory>
#include <utility>
#include <vector>

#include "k9db/dataflow/record.h"
#include "k9db/policy/policy_engine.h"
#include "k9db/shards/sqlengine/create.h"
#include "k9db/shards/sqlengine/delete.h"
#include "k9db/shards/sqlengine/explain.h"
#include "k9db/shards/sqlengine/gdpr.h"
#include "k9db/shards/sqlengine/index.h"
#include "k9db/shards/sqlengine/insert.h"
#include "k9db/shards/sqlengine/replace.h"
#include "k9db/shards/sqlengine/select.h"
#include "k9db/shards/sqlengine/update.h"
#include "k9db/shards/sqlengine/view.h"
#include "k9db/shards/types.h"
#include "k9db/sqlast/ast.h"
#include "k9db/sqlast/parser.h"
#include "k9db/util/status.h"
#include "k9db/util/upgradable_lock.h"

namespace k9db {
namespace shards {
namespace sqlengine {

// Helper for serializing policies.
namespace {

absl::StatusOr<sql::SqlResult> serialize_policies(
    const absl::StatusOr<sql::SqlResult> &status) {
  // Serialize policies of each resultset.
  if (status.ok()) {
    std::vector<sql::SqlResultSet> output;
    output.reserve(status->ResultSets().size());
    for (const sql::SqlResultSet &set : status->ResultSets()) {
      if (set.empty()) {
        output.emplace_back(set.schema());
      } else {
        auto rows = policy::SerializePolicies(set.rows());
        auto schema = rows.at(0).schema();
        output.emplace_back(schema, std::move(rows));
      }
    }
    return sql::SqlResult(std::move(output));
  }
  return status.status();
}

}  // namespace

absl::StatusOr<sql::SqlResult> Shard(const sqlast::SQLCommand &sql,
                                     Connection *connection) {
  dataflow::DataFlowState &dstate = connection->state->DataflowState();

  // Parse with ANTLR into our AST.
  sqlast::SQLParser parser;
  // parse the statement, move result of parsing sql string to newly created
  // <sqlast::AbstractStatement> statement.
  MOVE_OR_RETURN(std::unique_ptr<sqlast::AbstractStatement> statement,
                 parser.Parse(sql));

  // Get type of sql statement, initialize stmt (statement) as the associated
  // type custom defined in k9db via the sqlast class. Then call Shard in the
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
        return serialize_policies(view::SelectView(*stmt, connection, &lock));
      } else {
        util::SharedLock lock = connection->state->ReaderLock();
        SelectContext context(*stmt, connection, &lock);
        return serialize_policies(context.Exec());
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
      // Persist explicit views, not prepared statements.
      bool ok = true;
      if (stmt->view_name()[0] != '_') {
        ok = connection->state->Database()->PersistCreateView(*stmt);
      }
      return sql::SqlResult(ok);
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
      switch (stmt->operation()) {
        case sqlast::GDPRStatement::Operation::GET: {
          GDPRGetContext context(*stmt, connection, &lock);
          return serialize_policies(context.Exec());
        }
        case sqlast::GDPRStatement::Operation::FORGET: {
          GDPRForgetContext context(*stmt, connection, &lock);
          return context.Exec();
        }
      }
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
}  // namespace k9db
