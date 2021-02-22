// Entry point for our sql rewriting engine.

#include "pelton/shards/sqlengine/engine.h"

#include <memory>
#include <utility>

#include "pelton/shards/sqlengine/create.h"
#include "pelton/shards/sqlengine/delete.h"
#include "pelton/shards/sqlengine/insert.h"
#include "pelton/shards/sqlengine/select.h"
#include "pelton/shards/sqlengine/update.h"
#include "pelton/sqlast/ast.h"
#include "pelton/sqlast/parser.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {

absl::StatusOr<std::list<std::unique_ptr<sqlexecutor::ExecutableStatement>>>
Rewrite(const std::string &sql, SharderState *state) {
  // Parse with ANTLR into our AST.
  sqlast::SQLParser parser;
  MOVE_OR_RETURN(std::unique_ptr<sqlast::AbstractStatement> statement,
                 parser.Parse(sql));

  // Compute result.
  absl::StatusOr<std::list<std::unique_ptr<sqlexecutor::ExecutableStatement>>>
      result;
  switch (statement->type()) {
    // Case 1: CREATE TABLE statement.
    case sqlast::AbstractStatement::Type::CREATE_TABLE:
      result = create::Rewrite(
          *static_cast<sqlast::CreateTable *>(statement.get()), state);
      break;

    // Case 2: Insert statement.
    case sqlast::AbstractStatement::Type::INSERT:
      result = insert::Rewrite(*static_cast<sqlast::Insert *>(statement.get()),
                               state);
      break;

    // Case 3: Update statement.
    case sqlast::AbstractStatement::Type::UPDATE:
      result = update::Rewrite(*static_cast<sqlast::Update *>(statement.get()),
                               state);
      break;

    // Case 4: Select statement.
    case sqlast::AbstractStatement::Type::SELECT:
      result = select::Rewrite(*static_cast<sqlast::Select *>(statement.get()),
                               state);
      break;

    // Case 5: Delete statement.
    case sqlast::AbstractStatement::Type::DELETE:
      result = delete_::Rewrite(*static_cast<sqlast::Delete *>(statement.get()),
                                state);
      break;
  }

  state->SetLastStatement(std::move(statement));
  return result;
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
