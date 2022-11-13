// Abstract statement and forward declarations.
#ifndef PELTON_SQLAST_AST_ABSTRACT_H_
#define PELTON_SQLAST_AST_ABSTRACT_H_

#include <ostream>

namespace pelton {
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
    GDPR
  };

  // Constructor.
  explicit AbstractStatement(Type type) : type_(type) {}
  virtual ~AbstractStatement() {}

  // Accessors.
  const Type &type() const { return this->type_; }

  // Printing to screen.
  friend std::ostream &operator<<(std::ostream &os, const AbstractStatement &r);

 protected:
  Type type_;
};

std::ostream &operator<<(std::ostream &os, const AbstractStatement::Type &t);

}  // namespace sqlast
}  // namespace pelton

#endif  // PELTON_SQLAST_AST_ABSTRACT_H_
