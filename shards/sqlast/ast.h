// Defines the API for our SQLite-adapter.
#ifndef SHARDS_SQLAST_AST_H_
#define SHARDS_SQLAST_AST_H_

#include <algorithm>
#include <cassert>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace shards {
namespace sqlast {

// Forward declaration of visitor pattern classes.
template <class T>
class AbstractVisitor;
template <class T>
class MutableVisitor;

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

// Create table statement and sub expressions.
class ColumnConstraint {
 public:
  // Supported constraint types.
  enum Type { PRIMARY_KEY, UNIQUE, NOT_NULL, FOREIGN_KEY };

  // Constructor.
  explicit ColumnConstraint(Type type) : type_(type) {}

  // Accessors.
  const Type &type() const { return this->type_; }

  // Manipulations (only for foreign key).
  const std::string &foreign_table() const {
    assert(this->type_ == Type::FOREIGN_KEY);
    return this->foreign_table_;
  }
  std::string &foreign_table() {
    assert(this->type_ == Type::FOREIGN_KEY);
    return this->foreign_table_;
  }
  const std::string &foreign_column() const {
    assert(this->type_ == Type::FOREIGN_KEY);
    return this->foreign_column_;
  }
  std::string &foreign_column() {
    assert(this->type_ == Type::FOREIGN_KEY);
    return this->foreign_column_;
  }

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
  ColumnDefinition(const std::string &column_name,
                   const std::string &column_type)
      : column_name_(column_name), column_type_(column_type) {}

  // Accessors.
  const std::string &column_name() const { return this->column_name_; }
  std::string &column_name() { return this->column_name_; }
  const std::string &column_type() const { return this->column_type_; }
  std::string &column_type() { return this->column_type_; }

  // Constraint manipulations.
  void AddConstraint(const ColumnConstraint &constraint) {
    this->constraints_.push_back(constraint);
  }
  const std::vector<ColumnConstraint> &GetConstraints() const {
    return this->constraints_;
  }
  std::vector<ColumnConstraint> &MutableConstraints() {
    return this->constraints_;
  }
  const ColumnConstraint &GetConstraint(size_t index) const {
    return this->constraints_.at(index);
  }
  ColumnConstraint &MutableConstraint(size_t index) {
    return this->constraints_.at(index);
  }
  void RemoveConstraint(size_t index) {
    this->constraints_.erase(this->constraints_.begin() + index);
  }

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
      result.push_back(visitor->VisitColumnConstraint(constraint));
    }
    return result;
  }
  template <class T>
  std::vector<T> VisitChildren(MutableVisitor<T> *visitor) {
    std::vector<T> result;
    for (auto &constraint : this->constraints_) {
      result.push_back(visitor->VisitColumnConstraint(&constraint));
    }
    return result;
  }

 private:
  std::string column_name_;
  std::string column_type_;
  std::vector<ColumnConstraint> constraints_;
};

class CreateTable : public AbstractStatement {
 public:
  explicit CreateTable(const std::string &table_name)
      : AbstractStatement(AbstractStatement::Type::CREATE_TABLE),
        table_name_(table_name) {}

  // Accessors.
  const std::string &table_name() const { return this->table_name_; }
  std::string &table_name() { return this->table_name_; }

  // Column manipulations.
  void AddColumn(const std::string &column_name, const ColumnDefinition &def) {
    this->columns_map_.insert({column_name, this->columns_.size()});
    this->columns_.push_back(def);
  }
  const std::vector<ColumnDefinition> &GetColumns() const {
    return this->columns_;
  }
  const ColumnDefinition &GetColumn(const std::string &column_name) const {
    size_t column_index = this->columns_map_.at(column_name);
    return this->columns_.at(column_index);
  }
  ColumnDefinition &MutableColumn(const std::string &column_name) {
    size_t column_index = this->columns_map_.at(column_name);
    return this->columns_.at(column_index);
  }
  bool HasColumn(const std::string &column_name) {
    return this->columns_map_.count(column_name) == 1;
  }
  size_t ColumnIndex(const std::string &column_name) {
    return this->columns_map_.at(column_name);
  }
  void RemoveColumn(const std::string &column_name) {
    size_t column_index = this->columns_map_.at(column_name);
    this->columns_.erase(this->columns_.begin() + column_index);
    this->columns_map_.erase(column_name);
    for (size_t i = column_index; i < this->columns_.size(); i++) {
      this->columns_map_.at(this->columns_.at(i).column_name()) = i;
    }
  }

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
      result.push_back(visitor->VisitColumnDefinition(def));
    }
    return result;
  }
  template <class T>
  std::vector<T> VisitChildren(MutableVisitor<T> *visitor) {
    std::vector<T> result;
    for (auto &def : this->columns_) {
      result.push_back(visitor->VisitColumnDefinition(&def));
    }
    return result;
  }

 private:
  std::string table_name_;
  std::vector<ColumnDefinition> columns_;
  std::unordered_map<std::string, size_t> columns_map_;
};

// Insert statement.
class Insert : public AbstractStatement {
 public:
  explicit Insert(const std::string &table_name)
      : AbstractStatement(AbstractStatement::Type::INSERT),
        table_name_(table_name) {}

  // Accessors.
  const std::string &table_name() const { return this->table_name_; }
  std::string &table_name() { return this->table_name_; }

  // Columns and Values.
  void AddColumn(const std::string &colname) {
    this->columns_.push_back(colname);
  }
  const std::vector<std::string> &GetColumns() const { return this->columns_; }
  void SetValues(std::vector<std::string> &&values) { this->values_ = values; }
  const std::vector<std::string> &GetValues() const { return this->values_; }
  void RemoveValue(const std::string &colname) {
    auto it = std::find(this->columns_.begin(), this->columns_.end(), colname);
    this->columns_.erase(it);
    size_t index = std::distance(this->columns_.begin(), it);
    this->values_.erase(this->values_.begin() + index);
  }

  // Visitor pattern.
  template <class T>
  T Visit(AbstractVisitor<T> *visitor) const {
    return visitor->VisitInsert(*this);
  }
  template <class T>
  T Visit(MutableVisitor<T> *visitor) {
    return visitor->VisitInsert(this);
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
  std::string table_name_;
  std::vector<std::string> columns_;
  std::vector<std::string> values_;
};

// Select and delete statements.
class Expression {
 public:
  enum Type { LITERAL, COLUMN, EQ, AND };
  explicit Expression(Type type) : type_(type) {}
  const Type &type() const { return this->type_; }

 private:
  Type type_;
};

class ColumnExpression : public Expression {
 public:
  explicit ColumnExpression(const std::string &column)
      : Expression(Expression::Type::COLUMN), column_(column) {}

  const std::string &column() const { return this->column_; }
  std::string &column() { return this->column_; }

  template <class T>
  T Visit(AbstractVisitor<T> *visitor) const {
    return visitor->VisitColumnExpression(*this);
  }

  template <class T>
  T Visit(MutableVisitor<T> *visitor) {
    return visitor->VisitColumnExpression(this);
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
  std::string column_;
};

class LiteralExpression : public Expression {
 public:
  explicit LiteralExpression(const std::string &value)
      : Expression(Expression::Type::LITERAL), value_(value) {}

  const std::string &value() const { return this->value_; }
  std::string &value() { return this->value_; }

  template <class T>
  T Visit(AbstractVisitor<T> *visitor) const {
    return visitor->VisitLiteralExpression(*this);
  }

  template <class T>
  T Visit(MutableVisitor<T> *visitor) {
    return visitor->VisitLiteralExpression(this);
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
  std::string value_;
};

class BinaryExpression : public Expression {
 public:
  explicit BinaryExpression(Expression::Type type) : Expression(type) {}

  const Expression *const GetLeft() const { return this->left_.get(); }
  Expression *const GetLeft() { return this->left_.get(); }
  const Expression *const GetRight() const { return this->right_.get(); }
  Expression *const GetRight() { return this->right_.get(); }

  void SetLeft(std::unique_ptr<Expression> &&left) {
    this->left_ = std::move(left);
  }
  void SetRight(std::unique_ptr<Expression> &&right) {
    this->right_ = std::move(right);
  }

  template <class T>
  T Visit(AbstractVisitor<T> *visitor) const {
    return visitor->VisitBinaryExpression(*this);
  }

  template <class T>
  T Visit(MutableVisitor<T> *visitor) {
    return visitor->VisitBinaryExpression(this);
  }

  template <class T>
  std::vector<T> VisitChildren(AbstractVisitor<T> *visitor) const {
    std::vector<T> result;
    switch (this->left_->type()) {
      case Expression::Type::COLUMN:
        result.push_back(
            static_cast<ColumnExpression *>(this->left_.get())->Visit(visitor));
        break;
      case Expression::Type::LITERAL:
        result.push_back(static_cast<LiteralExpression *>(this->left_.get())
                             ->Visit(visitor));
        break;
      default:
        result.push_back(
            static_cast<BinaryExpression *>(this->left_.get())->Visit(visitor));
    }
    switch (this->right_->type()) {
      case Expression::Type::COLUMN:
        result.push_back(static_cast<ColumnExpression *>(this->right_.get())
                             ->Visit(visitor));
        break;
      case Expression::Type::LITERAL:
        result.push_back(static_cast<LiteralExpression *>(this->right_.get())
                             ->Visit(visitor));
        break;
      default:
        result.push_back(static_cast<BinaryExpression *>(this->right_.get())
                             ->Visit(visitor));
    }
    return result;
  }

  template <class T>
  std::vector<T> VisitChildren(MutableVisitor<T> *visitor) {
    std::vector<T> result;
    switch (this->left_->type()) {
      case Expression::Type::COLUMN:
        result.push_back(
            static_cast<ColumnExpression *>(this->left_.get())->Visit(visitor));
        break;
      case Expression::Type::LITERAL:
        result.push_back(static_cast<LiteralExpression *>(this->left_.get())
                             ->Visit(visitor));
        break;
      default:
        result.push_back(
            static_cast<BinaryExpression *>(this->left_.get())->Visit(visitor));
    }
    switch (this->right_->type()) {
      case Expression::Type::COLUMN:
        result.push_back(static_cast<ColumnExpression *>(this->right_.get())
                             ->Visit(visitor));
        break;
      case Expression::Type::LITERAL:
        result.push_back(static_cast<LiteralExpression *>(this->right_.get())
                             ->Visit(visitor));
        break;
      default:
        result.push_back(static_cast<BinaryExpression *>(this->right_.get())
                             ->Visit(visitor));
    }
    return result;
  }

 private:
  std::unique_ptr<Expression> left_;
  std::unique_ptr<Expression> right_;
};

class Select : public AbstractStatement {
 public:
  explicit Select(const std::string &table_name)
      : AbstractStatement(AbstractStatement::Type::SELECT),
        table_name_(table_name) {}

  const std::string &table_name() const { return this->table_name_; }
  std::string &table_name() { return this->table_name_; }

  void AddColumn(const std::string &column) {
    this->columns_.push_back(column);
  }
  const std::vector<std::string> &GetColumns() const { return this->columns_; }
  std::vector<std::string> &GetColumns() { return this->columns_; }
  void RemoveColumn(const std::string &column) {
    this->columns_.erase(
        std::find(this->columns_.begin(), this->columns_.end(), column));
  }

  bool HasWhereClause() const { return this->where_clause_.has_value(); }
  const BinaryExpression *const GetWhereClause() const {
    return this->where_clause_->get();
  }
  BinaryExpression *const GetWhereClause() {
    return this->where_clause_->get();
  }
  void SetWhereClause(std::unique_ptr<BinaryExpression> &&where) {
    this->where_clause_ = std::optional(std::move(where));
  }

  template <class T>
  T Visit(AbstractVisitor<T> *visitor) const {
    return visitor->VisitSelect(*this);
  }

  template <class T>
  T Visit(MutableVisitor<T> *visitor) {
    return visitor->VisitSelect(this);
  }

  template <class T>
  std::vector<T> VisitChildren(AbstractVisitor<T> *visitor) const {
    if (!this->where_clause_.has_value()) {
      return std::vector<T>{};
    }
    return std::vector<T>{this->where_clause_->get()->Visit(visitor)};
  }

  template <class T>
  std::vector<T> VisitChildren(MutableVisitor<T> *visitor) {
    if (!this->where_clause_.has_value()) {
      return std::vector<T>{};
    }
    return std::vector<T>{this->where_clause_->get()->Visit(visitor)};
  }

 private:
  std::string table_name_;
  std::vector<std::string> columns_;
  std::optional<std::unique_ptr<BinaryExpression>> where_clause_;
};

class Delete : public AbstractStatement {
 public:
  explicit Delete(const std::string &table_name)
      : AbstractStatement(AbstractStatement::Type::DELETE),
        table_name_(table_name) {}

  const std::string &table_name() const { return this->table_name_; }
  std::string &table_name() { return this->table_name_; }

  bool HasWhereClause() const { return this->where_clause_.has_value(); }
  const BinaryExpression *const GetWhereClause() const {
    return this->where_clause_->get();
  }
  BinaryExpression *const GetWhereClause() {
    return this->where_clause_->get();
  }
  void SetWhereClause(std::unique_ptr<BinaryExpression> &&where) {
    this->where_clause_ = std::optional(std::move(where));
  }

  template <class T>
  T Visit(AbstractVisitor<T> *visitor) const {
    return visitor->VisitDelete(*this);
  }

  template <class T>
  T Visit(MutableVisitor<T> *visitor) {
    return visitor->VisitDelete(this);
  }

  template <class T>
  std::vector<T> VisitChildren(AbstractVisitor<T> *visitor) const {
    if (!this->where_clause_.has_value()) {
      return std::vector<T>{};
    }
    return std::vector<T>{this->where_clause_->get()->Visit(visitor)};
  }

  template <class T>
  std::vector<T> VisitChildren(MutableVisitor<T> *visitor) {
    if (!this->where_clause_.has_value()) {
      return std::vector<T>{};
    }
    return std::vector<T>{this->where_clause_->get()->Visit(visitor)};
  }

 private:
  std::string table_name_;
  std::optional<std::unique_ptr<BinaryExpression>> where_clause_;
};

// Actual visitor pattern classes.
template <class T>
class AbstractVisitor {
 public:
  AbstractVisitor() {}
  virtual T VisitCreateTable(const CreateTable &ast) = 0;
  virtual T VisitColumnDefinition(const ColumnDefinition &ast) = 0;
  virtual T VisitColumnConstraint(const ColumnConstraint &ast) = 0;
  virtual T VisitInsert(const Insert &ast) = 0;
  virtual T VisitSelect(const Select &ast) = 0;
  virtual T VisitColumnExpression(const ColumnExpression &ast) = 0;
  virtual T VisitLiteralExpression(const LiteralExpression &ast) = 0;
  virtual T VisitBinaryExpression(const BinaryExpression &ast) = 0;
  virtual T VisitDelete(const Delete &ast) = 0;
};

template <class T>
class MutableVisitor {
 public:
  MutableVisitor() {}
  virtual T VisitCreateTable(CreateTable *ast) = 0;
  virtual T VisitColumnDefinition(ColumnDefinition *ast) = 0;
  virtual T VisitColumnConstraint(ColumnConstraint *ast) = 0;
  virtual T VisitInsert(Insert *ast) = 0;
  virtual T VisitSelect(Select *ast) = 0;
  virtual T VisitColumnExpression(ColumnExpression *ast) = 0;
  virtual T VisitLiteralExpression(LiteralExpression *ast) = 0;
  virtual T VisitBinaryExpression(BinaryExpression *ast) = 0;
  virtual T VisitDelete(Delete *ast) = 0;
};

class Stringifier : public AbstractVisitor<std::string> {
  std::string VisitCreateTable(const CreateTable &ast) override {
    std::string result = "CREATE TABLE " + ast.table_name() + " (";
    bool first = true;
    for (const std::string &col : ast.VisitChildren(this)) {
      if (!first) {
        result += ", ";
      }
      first = false;
      result += col;
    }
    result += ");";
    return result;
  }
  std::string VisitColumnDefinition(const ColumnDefinition &ast) override {
    std::string result = ast.column_name() + " " + ast.column_type();
    for (const std::string &col : ast.VisitChildren(this)) {
      result += " " + col;
    }
    return result;
  }
  std::string VisitColumnConstraint(const ColumnConstraint &ast) override {
    switch (ast.type()) {
      case ColumnConstraint::Type::PRIMARY_KEY:
        return "PRIMARY KEY";
      case ColumnConstraint::Type::UNIQUE:
        return "UNIQUE";
      case ColumnConstraint::Type::NOT_NULL:
        return "NOT NULL";
      default:
        return "REFERENCES " + ast.foreign_table() + "(" +
               ast.foreign_column() + ")";
    }
  }

  std::string VisitInsert(const Insert &ast) override {
    std::string result = "INSERT INTO " + ast.table_name();
    // Columns if explicitly specified.
    if (ast.GetColumns().size() > 0) {
      result += "(";
      bool first = true;
      for (const std::string &col : ast.GetColumns()) {
        if (!first) {
          result += ", ";
        }
        first = false;
        result += col;
      }
      result += ")";
    }
    // Comma separated values.
    result += " VALUES (";
    bool first = true;
    for (const std::string &val : ast.GetValues()) {
      if (!first) {
        result += ", ";
      }
      first = false;
      result += val;
    }
    result += ");";
    return result;
  }

  std::string VisitSelect(const Select &ast) override {
    std::string result = "SELECT ";
    bool first = true;
    for (const std::string &col : ast.GetColumns()) {
      if (!first) {
        result += ", ";
      }
      first = false;
      result += col;
    }
    result += " FROM " + ast.table_name();
    if (ast.HasWhereClause()) {
      result += " WHERE " + ast.VisitChildren(this).at(0);
    }
    result += ";";
    return result;
  }

  std::string VisitColumnExpression(const ColumnExpression &ast) override {
    return ast.column();
  }

  std::string VisitLiteralExpression(const LiteralExpression &ast) override {
    return ast.value();
  }

  std::string VisitBinaryExpression(const BinaryExpression &ast) override {
    std::string op = "";
    switch (ast.type()) {
      case Expression::Type::AND:
        op = " AND ";
        break;
      case Expression::Type::EQ:
        op = " = ";
        break;
      default:
        assert(false);
    }
    std::vector<std::string> children = ast.VisitChildren(this);
    return children.at(0) + op + children.at(1);
  }

  std::string VisitDelete(const Delete &ast) override {
    std::string result = "DELETE FROM " + ast.table_name();
    if (ast.HasWhereClause()) {
      result += " WHERE " + ast.VisitChildren(this).at(0);
    }
    result += ";";
    return result;
  }
};

}  // namespace sqlast
}  // namespace shards

#endif  // SHARDS_SQLAST_AST_H_
