// Create table statement and sub expressions.
#ifndef PELTON_SQLAST_AST_SCHEMA_H_
#define PELTON_SQLAST_AST_SCHEMA_H_

#include <ostream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "pelton/sqlast/ast_abstract.h"
#include "pelton/sqlast/ast_schema_enums.h"
#include "pelton/sqlast/ast_value.h"

namespace pelton {
namespace sqlast {

// Create table statement and sub expressions.
class ColumnConstraint {
 public:
  // Supported constraint types.
  using Type = ColumnConstraintTypeEnum;
  using FKType = ForeignKeyTypeEnum;
  static std::string TypeToString(Type type);

  // Additional data specific to different types.
  using ForeignKeyData = std::tuple<std::string, std::string, FKType>;
  using DefaultData = Value;  // Default value.

  // Constructor is private: use these functions to create.
  static ColumnConstraint Make(Type type);
  static ColumnConstraint MakeForeignKey(const std::string &foreign_table,
                                         const std::string &foreign_column,
                                         FKType type);

  // Accessors.
  const Type &type() const { return this->type_; }
  const ForeignKeyData &ForeignKey() const { return std::get<0>(this->data_); }
  const DefaultData &Default() const { return std::get<1>(this->data_); }

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
  // Type along with any data specific to that type.
  Type type_;
  std::variant<ForeignKeyData, DefaultData> data_;

  // Constructor.
  explicit ColumnConstraint(Type type) : type_(type) {}
};

class ColumnDefinition {
 public:
  // Supported column types.
  using Type = ColumnDefinitionTypeEnum;

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
  const std::string &column_name() const { return this->column_name_; }
  Type column_type() const { return this->column_type_; }

  // Constraint manipulations.
  void AddConstraint(const ColumnConstraint &constraint);
  const std::vector<ColumnConstraint> &GetConstraints() const;
  bool HasConstraint(ColumnConstraint::Type type) const;
  bool HasFKType(ColumnConstraint::FKType type) const;

  // Get the first constraint matching a specific type on this column. Errors if
  // no such constraint exists. Use `HasConstraint()` to check that such a
  // constraint exists first.
  const ColumnConstraint &GetConstraintOfType(
      ColumnConstraint::Type type) const;

  // Get a foreign key constraint on this column.
  // Errors if no such constraint exists.
  const ColumnConstraint &GetForeignKeyConstraint() const;

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

class AnonymizationRule {
 public:
  using Type = AnonymizationOpTypeEnum;

  AnonymizationRule(Type op_type, const std::string &data_subject)
      : op_type_(op_type), data_subject_(data_subject) {}

  Type GetType() const { return this->op_type_; }
  const std::string &GetDataSubject() const { return this->data_subject_; }
  const std::vector<std::string> &GetAnonymizeColumns() const {
    return this->anon_columns_;
  }

  void AddAnonymizeColumn(const std::string &anon_column) {
    this->anon_columns_.push_back(anon_column);
  }

  void SetAnonymizeColumns(std::vector<std::string> anon_columns) {
    this->anon_columns_ = anon_columns;
  }

  // Visitor pattern.
  template <class T>
  T Visit(AbstractVisitor<T> *visitor) const {
    return visitor->VisitAnonymizationRule(*this);
  }
  template <class T>
  T Visit(MutableVisitor<T> *visitor) {
    return visitor->VisitAnonymizationRule(this);
  }

  template <class T>
  std::vector<T> VisitChildren(AbstractVisitor<T> *visitor) const {
    return {};
  }
  template <class T>
  std::vector<T> VisitChildren(MutableVisitor<T> *visitor) {
    return {};
  }

  // For testing.
  bool operator==(const AnonymizationRule &other) const = default;

 private:
  // operation type -> enum
  Type op_type_;
  // column_name (data subject doing operation)
  std::string data_subject_;
  // vector of column_name (columns to anonymize)
  std::vector<std::string> anon_columns_;
};

class CreateTable : public AbstractStatement {
 public:
  explicit CreateTable(const std::string &table_name)
      : AbstractStatement(AbstractStatement::Type::CREATE_TABLE),
        table_name_(table_name),
        data_subject_(false) {}

  // Accessors.
  const std::string &table_name() const { return this->table_name_; }
  bool IsDataSubject() const { return this->data_subject_; }
  void SetDataSubject() { this->data_subject_ = true; }

  // Column manipulations.
  void AddColumn(const std::string &column_name, const ColumnDefinition &def);
  const std::vector<ColumnDefinition> &GetColumns() const;
  const ColumnDefinition &GetColumn(const std::string &column_name) const;
  ColumnDefinition &MutableColumn(const std::string &column_name);
  bool HasColumn(const std::string &column_name) const;

  // Anonymization rules.
  void AddAnonymizeRule(AnonymizationRule &&anon_rule);
  const std::vector<AnonymizationRule> &GetAnonymizationRules() const;

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
    for (const auto &anon_rule : this->anon_rules_) {
      result.push_back(std::move(visitor->VisitAnonymizationRule(anon_rule)));
    }
    return result;
  }
  template <class T>
  std::vector<T> VisitChildren(MutableVisitor<T> *visitor) {
    std::vector<T> result;
    for (auto &def : this->columns_) {
      result.push_back(std::move(visitor->VisitColumnDefinition(&def)));
    }
    for (auto &anon_rule : this->anon_rules_) {
      result.push_back(std::move(visitor->VisitAnonymizationRule(&anon_rule)));
    }
    return result;
  }

 private:
  std::string table_name_;
  bool data_subject_;
  std::vector<ColumnDefinition> columns_;
  std::unordered_map<std::string, size_t> columns_map_;
  std::vector<AnonymizationRule> anon_rules_;
};

class CreateIndex : public AbstractStatement {
 public:
  CreateIndex(const std::string &index_name, const std::string &table_name)
      : AbstractStatement(AbstractStatement::Type::CREATE_INDEX),
        index_name_(index_name),
        table_name_(table_name),
        column_names_() {}

  // Add columns.
  void AddColumn(const std::string &column_name) {
    this->column_names_.push_back(column_name);
  }

  // Accessors.
  const std::string &table_name() const { return this->table_name_; }
  const std::string &index_name() const { return this->index_name_; }
  const std::vector<std::string> &column_names() const {
    return this->column_names_;
  }

  // Visitor pattern.
  template <class T>
  T Visit(AbstractVisitor<T> *visitor) const {
    return visitor->VisitCreateIndex(*this);
  }
  template <class T>
  T Visit(MutableVisitor<T> *visitor) {
    return visitor->VisitCreateIndex(this);
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
  bool ondisk_;
  std::string index_name_;
  std::string table_name_;
  std::vector<std::string> column_names_;
};

class CreateView : public AbstractStatement {
 public:
  CreateView(const std::string &view_name, const std::string &query)
      : AbstractStatement(AbstractStatement::Type::CREATE_VIEW),
        view_name_(view_name),
        query_(query) {}

  // Accessors.
  const std::string &view_name() const { return this->view_name_; }
  const std::string &query() const { return this->query_; }

  template <class T>
  T Visit(AbstractVisitor<T> *visitor) const {
    return visitor->VisitCreateView(*this);
  }
  template <class T>
  T Visit(MutableVisitor<T> *visitor) {
    return visitor->VisitCreateView(this);
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
  std::string view_name_;
  std::string query_;
};

std::ostream &operator<<(std::ostream &os, const ColumnConstraint::Type &r);
std::ostream &operator<<(std::ostream &os, const ColumnDefinition::Type &r);
std::ostream &operator<<(std::ostream &os, const ColumnConstraint::FKType &t);

}  // namespace sqlast
}  // namespace pelton

#endif  // PELTON_SQLAST_AST_SCHEMA_H_
