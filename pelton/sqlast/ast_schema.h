// Create table statement and sub expressions.
#ifndef PELTON_SQLAST_AST_SCHEMA_H_
#define PELTON_SQLAST_AST_SCHEMA_H_

#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "pelton/sqlast/ast_abstract.h"

namespace pelton {
namespace sqlast {

// Create table statement and sub expressions.
class ColumnConstraint {
 public:
  // Supported constraint types.
  enum class Type { PRIMARY_KEY, UNIQUE, NOT_NULL, FOREIGN_KEY };

  static std::string TypeToString(Type type);

  // Constructor.
  explicit ColumnConstraint(Type type) : type_(type) {}

  // Accessors.
  const Type &type() const { return this->type_; }

  // Manipulations (only for foreign key).
  const std::string &foreign_table() const;
  std::string &foreign_table();
  const std::string &foreign_column() const;
  std::string &foreign_column();

  // Visitor pattern.
  template <class T>
  T Visit(AbstractVisitor<T> *visitor) const {
    return visitor->VisitColumnConstraint(*this);
  }
  template <class T>
  T Visit(MutableVisitor<T> *visitor) {
    return visitor->VisitColumnConstraint(this);
  }
  template <class T>
  std::vector<T> VisitChildren(AbstractVisitor<T> *visitor) const {
    return {};
  }
  template <class T>
  std::vector<T> VisitChildren(MutableVisitor<T> *visitor) {
    return {};
  }

 private:
  Type type_;
  std::string foreign_table_;
  std::string foreign_column_;
};

class ColumnDefinition {
 public:
  // Supported column types.
  enum class Type { UINT, INT, TEXT, DATETIME };

  // Transform type name to type enum.
  static Type StringToType(const std::string &column_type);
  static std::string TypeToString(Type type);

  // Constructors.
  ColumnDefinition(const std::string &column_name,
                   const std::string &column_type)
      : ColumnDefinition(column_name, StringToType(column_type)) {}

  ColumnDefinition(const std::string &column_name, Type column_type)
      : column_name_(column_name), column_type_(column_type) {}

  // Accessors.
  const std::string &column_name() const;
  std::string &column_name();
  Type column_type() const;
  Type &column_type();

  // Constraint manipulations.
  void AddConstraint(const ColumnConstraint &constraint);

  const std::vector<ColumnConstraint> &GetConstraints() const;
  std::vector<ColumnConstraint> &MutableConstraints();

  const ColumnConstraint &GetConstraint(size_t index) const;
  ColumnConstraint &MutableConstraint(size_t index);

  void RemoveConstraint(size_t index);
  void RemoveConstraint(ColumnConstraint::Type type);

  bool HasConstraint(ColumnConstraint::Type type) const;

  // Visitor pattern.
  template <class T>
  T Visit(AbstractVisitor<T> *visitor) const {
    return visitor->VisitColumnDefinition(*this);
  }
  template <class T>
  T Visit(MutableVisitor<T> *visitor) {
    return visitor->VisitColumnDefinition(this);
  }

  template <class T>
  std::vector<T> VisitChildren(AbstractVisitor<T> *visitor) const {
    std::vector<T> result;
    for (const auto &constraint : this->constraints_) {
      result.push_back(std::move(visitor->VisitColumnConstraint(constraint)));
    }
    return result;
  }
  template <class T>
  std::vector<T> VisitChildren(MutableVisitor<T> *visitor) {
    std::vector<T> result;
    for (auto &constraint : this->constraints_) {
      result.push_back(std::move(visitor->VisitColumnConstraint(&constraint)));
    }
    return result;
  }

 private:
  std::string column_name_;
  Type column_type_;
  std::vector<ColumnConstraint> constraints_;
};

class CreateTable : public AbstractStatement {
 public:
  explicit CreateTable(const std::string &table_name)
      : AbstractStatement(AbstractStatement::Type::CREATE_TABLE),
        table_name_(table_name) {}

  // Accessors.
  const std::string &table_name() const;
  std::string &table_name();

  // Column manipulations.
  void AddColumn(const std::string &column_name, const ColumnDefinition &def);

  const std::vector<ColumnDefinition> &GetColumns() const;

  const ColumnDefinition &GetColumn(const std::string &column_name) const;
  ColumnDefinition &MutableColumn(const std::string &column_name);

  bool HasColumn(const std::string &column_name);
  size_t ColumnIndex(const std::string &column_name);
  void RemoveColumn(const std::string &column_name);

  // Visitor pattern.
  template <class T>
  T Visit(AbstractVisitor<T> *visitor) const {
    return visitor->VisitCreateTable(*this);
  }
  template <class T>
  T Visit(MutableVisitor<T> *visitor) {
    return visitor->VisitCreateTable(this);
  }

  template <class T>
  std::vector<T> VisitChildren(AbstractVisitor<T> *visitor) const {
    std::vector<T> result;
    for (const auto &def : this->columns_) {
      result.push_back(std::move(visitor->VisitColumnDefinition(def)));
    }
    return result;
  }
  template <class T>
  std::vector<T> VisitChildren(MutableVisitor<T> *visitor) {
    std::vector<T> result;
    for (auto &def : this->columns_) {
      result.push_back(std::move(visitor->VisitColumnDefinition(&def)));
    }
    return result;
  }

 private:
  std::string table_name_;
  std::vector<ColumnDefinition> columns_;
  std::unordered_map<std::string, size_t> columns_map_;
};

std::ostream &operator<<(std::ostream &os, const ColumnConstraint::Type &r);
std::ostream &operator<<(std::ostream &os, const ColumnDefinition::Type &r);

}  // namespace sqlast
}  // namespace pelton

#endif  // PELTON_SQLAST_AST_SCHEMA_H_
