// Abstract statement and forward declarations.
#ifndef K9DB_SQLAST_AST_ABSTRACT_H_
#define K9DB_SQLAST_AST_ABSTRACT_H_

#include <ostream>

namespace k9db {
namespace sqlast {

// Forward declaration of visitor pattern classes.
template <class T>
class AbstractVisitor;
template <class T>
class MutableVisitor;

// Top-level statements derive this class.
class AbstractStatement {
 public:
  enum class Type {
    CREATE_TABLE,
    CREATE_INDEX,
    INSERT,
    REPLACE,
    UPDATE,
    SELECT,
    DELETE,
    CREATE_VIEW,
    GDPR,
    EXPLAIN_QUERY
  };

  // Constructor.
  explicit AbstractStatement(Type type) : type_(type) {}
  virtual ~AbstractStatement() {}

  // Accessors.
  const Type &type() const { return this->type_; }

  template <class T>
  T Visit(AbstractVisitor<T> *visitor) const;

  template <class T>
  T Visit(MutableVisitor<T> *visitor);

  // Printing to screen.
  friend std::ostream &operator<<(std::ostream &os, const AbstractStatement &r);

 protected:
  Type type_;
};

std::ostream &operator<<(std::ostream &os, const AbstractStatement::Type &t);

}  // namespace sqlast
}  // namespace k9db

#endif  // K9DB_SQLAST_AST_ABSTRACT_H_
