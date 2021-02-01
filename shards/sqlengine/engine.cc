// Entry point for our sql rewriting engine.

#include "shards/sqlengine/engine.h"

#include <memory>

#include "shards/sqlast/ast.h"
#include "shards/sqlast/parser.h"
#include "shards/sqlengine/create.h"
#include "shards/sqlengine/delete.h"
#include "shards/sqlengine/insert.h"
#include "shards/sqlengine/select.h"
#include "shards/sqlengine/update.h"
#include "shards/util/status.h"

namespace shards {
namespace sqlengine {

absl::StatusOr<std::list<std::unique_ptr<sqlexecutor::ExecutableStatement>>>
Rewrite(const std::string &sql, SharderState *state) {
  // Parse with ANTLR into our AST.
  sqlast::SQLParser parser;
  MOVE_OR_RETURN(std::unique_ptr<sqlast::AbstractStatement> statement,
                 parser.Parse(sql));

  sqlast::Stringifier stringifier;
  switch (statement->type()) {
    // Case 1: CREATE TABLE statement.
    case sqlast::AbstractStatement::Type::CREATE_TABLE:
      return create::Rewrite(
          *static_cast<sqlast::CreateTable *>(statement.get()), state);

    // Case 2: Insert statement.
    case sqlast::AbstractStatement::Type::INSERT:
      return insert::Rewrite(*static_cast<sqlast::Insert *>(statement.get()),
                             state);

    // Case 3: Update statement.
    case sqlast::AbstractStatement::Type::UPDATE:
      return update::Rewrite(*static_cast<sqlast::Update *>(statement.get()),
                             state);

    // Case 4: Select statement.
    case sqlast::AbstractStatement::Type::SELECT:
      return select::Rewrite(*static_cast<sqlast::Select *>(statement.get()),
                             state);

    // Case 5: Delete statement.
    case sqlast::AbstractStatement::Type::DELETE:
      return delete_::Rewrite(*static_cast<sqlast::Delete *>(statement.get()),
                              state);
  }

  return std::list<std::unique_ptr<sqlexecutor::ExecutableStatement>>();
}

}  // namespace sqlengine
}  // namespace shards
