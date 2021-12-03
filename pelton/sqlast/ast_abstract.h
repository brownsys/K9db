// Abstract statement and forward declarations.
#ifndef PELTON_SQLAST_AST_ABSTRACT_H_
#define PELTON_SQLAST_AST_ABSTRACT_H_

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
    UPDATE,
    SELECT,
    DELETE,
    CREATE_VIEW,
    GDPR,
    POLICY
  };

  // Constructor.
  explicit AbstractStatement(Type type) : type_(type) {}

  virtual ~AbstractStatement() {}

  // Accessors.
  const Type &type() const { return this->type_; }

 private:
  Type type_;
};

}  // namespace sqlast
}  // namespace pelton

#endif  // PELTON_SQLAST_AST_ABSTRACT_H_
