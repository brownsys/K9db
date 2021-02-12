// Insert, delete and select statements.
#ifndef PELTON_SQLAST_AST_STATEMENTS_H_
#define PELTON_SQLAST_AST_STATEMENTS_H_

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "pelton/sqlast/ast_abstract.h"
#include "pelton/sqlast/ast_expressions.h"

namespace pelton {
namespace sqlast {

// Insert statement.
class Insert : public AbstractStatement {
 public:
  explicit Insert(const std::string &table_name)
      : AbstractStatement(AbstractStatement::Type::INSERT),
        table_name_(table_name) {}

  // Accessors.
  const std::string &table_name() const;
  std::string &table_name();

  // Columns and Values.
  bool HasColumns() const;
  void AddColumn(const std::string &colname);
  const std::vector<std::string> &GetColumns() const;
  void SetValues(std::vector<std::string> &&values);
  const std::vector<std::string> &GetValues() const;

  std::string RemoveValue(const std::string &colname);
  std::string RemoveValue(size_t index);

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

class Update : public AbstractStatement {
 public:
  explicit Update(const std::string &table_name)
      : AbstractStatement(AbstractStatement::Type::UPDATE),
        table_name_(table_name) {}

  Update(const Update &update)
      : AbstractStatement(AbstractStatement::Type::UPDATE) {
    this->table_name_ = update.table_name_;
    this->columns_ = update.columns_;
    this->values_ = update.values_;
    if (update.where_clause_.has_value()) {
      this->where_clause_ = std::optional(
          std::make_unique<BinaryExpression>(*update.where_clause_->get()));
    }
  }

  const std::string &table_name() const;
  std::string &table_name();

  void AddColumnValue(const std::string &column, const std::string &value);
  std::string RemoveColumnValue(const std::string &column);
  bool AssignsTo(const std::string &column) const;

  const std::vector<std::string> &GetColumns() const;
  std::vector<std::string> &GetColumns();
  const std::vector<std::string> &GetValues() const;
  std::vector<std::string> &GetValues();

  bool HasWhereClause() const;
  const BinaryExpression *const GetWhereClause() const;
  BinaryExpression *const GetWhereClause();
  void SetWhereClause(std::unique_ptr<BinaryExpression> &&where);
  void RemoveWhereClause();

  template <class T>
  T Visit(AbstractVisitor<T> *visitor) const {
    return visitor->VisitUpdate(*this);
  }
  template <class T>
  T Visit(MutableVisitor<T> *visitor) {
    return visitor->VisitUpdate(this);
  }
  template <class T>
  std::vector<T> VisitChildren(AbstractVisitor<T> *visitor) const {
    std::vector<T> result;
    if (this->where_clause_.has_value()) {
      result.push_back(std::move(this->where_clause_->get()->Visit(visitor)));
    }
    return result;
  }
  template <class T>
  std::vector<T> VisitChildren(MutableVisitor<T> *visitor) {
    std::vector<T> result;
    if (this->where_clause_.has_value()) {
      result.push_back(std::move(this->where_clause_->get()->Visit(visitor)));
    }
    return result;
  }

 private:
  std::string table_name_;
  std::vector<std::string> columns_;
  std::vector<std::string> values_;
  std::optional<std::unique_ptr<BinaryExpression>> where_clause_;
};

class Select : public AbstractStatement {
 public:
  explicit Select(const std::string &table_name)
      : AbstractStatement(AbstractStatement::Type::SELECT),
        table_name_(table_name) {}

  Select(const Select &sel)
      : AbstractStatement(AbstractStatement::Type::SELECT) {
    this->table_name_ = sel.table_name_;
    this->columns_ = sel.columns_;
    if (sel.where_clause_.has_value()) {
      this->where_clause_ = std::optional(
          std::make_unique<BinaryExpression>(*sel.where_clause_->get()));
    }
  }

  const std::string &table_name() const;
  std::string &table_name();

  void AddColumn(const std::string &column);
  const std::vector<std::string> &GetColumns() const;
  std::vector<std::string> &GetColumns();
  void RemoveColumn(const std::string &column);

  bool HasWhereClause() const;
  const BinaryExpression *const GetWhereClause() const;
  BinaryExpression *const GetWhereClause();
  void SetWhereClause(std::unique_ptr<BinaryExpression> &&where);
  void RemoveWhereClause();

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
    std::vector<T> result;
    if (this->where_clause_.has_value()) {
      result.push_back(std::move(this->where_clause_->get()->Visit(visitor)));
    }
    return result;
  }
  template <class T>
  std::vector<T> VisitChildren(MutableVisitor<T> *visitor) {
    std::vector<T> result;
    if (this->where_clause_.has_value()) {
      result.push_back(std::move(this->where_clause_->get()->Visit(visitor)));
    }
    return result;
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

  Delete(const Delete &del)
      : AbstractStatement(AbstractStatement::Type::DELETE) {
    this->table_name_ = del.table_name_;
    if (del.where_clause_.has_value()) {
      this->where_clause_ = std::optional(
          std::make_unique<BinaryExpression>(*del.where_clause_->get()));
    }
  }

  const std::string &table_name() const;
  std::string &table_name();

  bool HasWhereClause() const;
  const BinaryExpression *const GetWhereClause() const;
  BinaryExpression *const GetWhereClause();
  void SetWhereClause(std::unique_ptr<BinaryExpression> &&where);
  void RemoveWhereClause();

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
    std::vector<T> result;
    if (this->where_clause_.has_value()) {
      result.push_back(std::move(this->where_clause_->get()->Visit(visitor)));
    }
    return result;
  }
  template <class T>
  std::vector<T> VisitChildren(MutableVisitor<T> *visitor) {
    std::vector<T> result;
    if (this->where_clause_.has_value()) {
      result.push_back(std::move(this->where_clause_->get()->Visit(visitor)));
    }
    return result;
  }

 private:
  std::string table_name_;
  std::optional<std::unique_ptr<BinaryExpression>> where_clause_;
};

}  // namespace sqlast
}  // namespace pelton

#endif  // PELTON_SQLAST_AST_STATEMENTS_H_
