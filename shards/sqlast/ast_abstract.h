// Abstract statement and forward declarations.
#ifndef SHARDS_SQLAST_AST_ABSTRACT_H_
#define SHARDS_SQLAST_AST_ABSTRACT_H_

namespace shards {
namespace sqlast {

// Forward declaration of visitor pattern classes.
template <class T>
class AbstractVisitor;
template <class T>
class MutableVisitor;

// Top-level statements derive this class.
class AbstractStatement {
 public:
  enum Type { CREATE_TABLE, INSERT, SELECT, DELETE };

  // Constructor.
  explicit AbstractStatement(Type type) : type_(type) {}

  // Accessors.
  const Type &type() const { return this->type_; }

 private:
  Type type_;
};

}  // namespace sqlast
}  // namespace shards

#endif  // SHARDS_SQLAST_AST_ABSTRACT_H_
