#include "k9db/sqlast/ast_abstract.h"

#include "glog/logging.h"

namespace k9db {
namespace sqlast {

std::ostream &operator<<(std::ostream &os, const AbstractStatement::Type &t) {
  switch (t) {
    case AbstractStatement::Type::CREATE_TABLE:
      return os << "CREATE TABLE";
    case AbstractStatement::Type::CREATE_INDEX:
      return os << "CREATE INDEX";
    case AbstractStatement::Type::INSERT:
      return os << "INSERT";
    case AbstractStatement::Type::UPDATE:
      return os << "UPDATE";
    case AbstractStatement::Type::SELECT:
      return os << "SELECT";
    case AbstractStatement::Type::DELETE:
      return os << "DELETE";
    case AbstractStatement::Type::CREATE_VIEW:
      return os << "CREATE VIEW";
    case AbstractStatement::Type::GDPR:
      return os << "GDPR";
    case AbstractStatement::Type::EXPLAIN_QUERY:
      return os << "EXPLAIN QUERY";
    default:
      LOG(FATAL) << "No string representation defined for an enum variant for "
                 << typeid(t).name();
  }
}

}  // namespace sqlast
}  // namespace k9db
