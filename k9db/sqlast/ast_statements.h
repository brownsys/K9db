// Insert, delete and select statements.
#ifndef K9DB_SQLAST_AST_STATEMENTS_H_
#define K9DB_SQLAST_AST_STATEMENTS_H_

#include <memory>
#include <optional>
#include <string>
#include <utility>
// NOLINTNEXTLINE
#include <variant>
#include <vector>

#include "absl/status/statusor.h"
#include "k9db/sqlast/ast_abstract.h"
#include "k9db/sqlast/ast_expressions.h"
#include "k9db/sqlast/ast_value.h"

namespace k9db {
namespace sqlast {

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
  bool HasColumns() const;

  // Columns and values.
  void SetColumns(std::vector<std::string> &&colname);
  void SetValues(std::vector<Value> &&values);

  const std::vector<std::string> &GetColumns() const { return this->columns_; }
  const std::vector<Value> &GetValues() const { return this->values_; }

  // Get by index or by column name, if stmt HasColumns.
  const Value &GetValue(const std::string &colname, size_t index) const;

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
  std::vector<Value> values_;
};

class Replace : public Insert {
 public:
  explicit Replace(const std::string &table_name) : Insert(table_name) {
    this->type_ = AbstractStatement::Type::REPLACE;
  }

  /* Inherits all the functions of insert. */

  // Visitor pattern.
  template <class T>
  T Visit(AbstractVisitor<T> *visitor) const {
    return visitor->VisitReplace(*this);
  }
  template <class T>
  T Visit(MutableVisitor<T> *visitor) {
    return visitor->VisitReplace(this);
  }
};

class Select : public AbstractStatement {
 public:
  // An entry in the projection list could either be a column name or a literal.
  class ResultColumn {
   public:
    explicit ResultColumn(const std::string &column) : data_(column) {}
    explicit ResultColumn(const Value &literal) : data_(literal) {}

    bool IsColumn() const { return this->data_.index() == 0; }
    const Value &GetLiteral() const { return std::get<Value>(this->data_); }
    const std::string &GetColumn() const {
      return std::get<std::string>(this->data_);
    }

    std::string ToString() const {
      if (this->IsColumn()) {
        return this->GetColumn();
      } else {
        return this->GetLiteral().AsSQLString();
      }
    }

   private:
    std::variant<std::string, Value> data_;
  };

  explicit Select(const std::string &table_name)
      : AbstractStatement(AbstractStatement::Type::SELECT),
        table_name_(table_name) {
    this->offset_ = 0;
    this->limit_ = -1;
  }

  const std::string &table_name() const { return this->table_name_; }
  std::string &table_name() { return this->table_name_; }

  const size_t &offset() const { return this->offset_; }
  size_t &offset() { return this->offset_; }
  const int &limit() const { return this->limit_; }
  int &limit() { return this->limit_; }

  void AddColumn(const ResultColumn &column);
  const std::vector<ResultColumn> &GetColumns() const { return this->columns_; }

  bool HasWhereClause() const { return this->where_clause_.has_value(); }
  void SetWhereClause(std::unique_ptr<BinaryExpression> &&where);
  const BinaryExpression *GetWhereClause() const;

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
      result.push_back(this->where_clause_->get()->Visit(visitor));
    }
    return result;
  }
  template <class T>
  std::vector<T> VisitChildren(MutableVisitor<T> *visitor) {
    std::vector<T> result;
    if (this->where_clause_.has_value()) {
      result.push_back(this->where_clause_->get()->Visit(visitor));
    }
    return result;
  }

 private:
  std::string table_name_;
  // Either column name or literal.
  std::vector<ResultColumn> columns_;
  std::optional<std::unique_ptr<BinaryExpression>> where_clause_;
  size_t offset_;
  int limit_;
};

class Delete : public AbstractStatement {
 public:
  explicit Delete(const std::string &table_name)
      : AbstractStatement(AbstractStatement::Type::DELETE),
        table_name_(table_name) {}

  const std::string &table_name() const { return this->table_name_; }
  std::string &table_name() { return this->table_name_; }

  bool HasWhereClause() const { return this->where_clause_.has_value(); }
  void SetWhereClause(std::unique_ptr<BinaryExpression> &&where);
  const BinaryExpression *GetWhereClause() const;

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
      result.push_back(this->where_clause_->get()->Visit(visitor));
    }
    return result;
  }
  template <class T>
  std::vector<T> VisitChildren(MutableVisitor<T> *visitor) {
    std::vector<T> result;
    if (this->where_clause_.has_value()) {
      result.push_back(this->where_clause_->get()->Visit(visitor));
    }
    return result;
  }

 private:
  std::string table_name_;
  std::optional<std::unique_ptr<BinaryExpression>> where_clause_;
};

class Update : public AbstractStatement {
 public:
  explicit Update(const std::string &table_name)
      : AbstractStatement(AbstractStatement::Type::UPDATE),
        table_name_(table_name) {}

  const std::string &table_name() const { return this->table_name_; }
  std::string &table_name() { return this->table_name_; }

  void AddColumnValue(const std::string &column,
                      std::unique_ptr<Expression> &&value);

  const std::vector<std::string> &GetColumns() const { return this->columns_; }
  const std::vector<std::unique_ptr<Expression>> &GetValues() const {
    return this->values_;
  }

  bool HasWhereClause() const { return this->where_clause_.has_value(); }
  void SetWhereClause(std::unique_ptr<BinaryExpression> &&where);
  const BinaryExpression *GetWhereClause() const;

  // Construct a delete statement over the same table and with the same WHERE
  // clause.
  Delete DeleteDomain() const;

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
    for (const std::unique_ptr<Expression> &value : this->values_) {
      result.push_back(value->Visit(visitor));
    }
    if (this->where_clause_.has_value()) {
      result.push_back(this->where_clause_->get()->Visit(visitor));
    }
    return result;
  }
  template <class T>
  std::vector<T> VisitChildren(MutableVisitor<T> *visitor) {
    std::vector<T> result;
    for (std::unique_ptr<Expression> &value : this->values_) {
      result.push_back(value->Visit(visitor));
    }
    if (this->where_clause_.has_value()) {
      result.push_back(this->where_clause_->get()->Visit(visitor));
    }
    return result;
  }

 private:
  std::string table_name_;
  std::vector<std::string> columns_;
  std::vector<std::unique_ptr<Expression>> values_;
  std::optional<std::unique_ptr<BinaryExpression>> where_clause_;
};

class GDPRStatement : public AbstractStatement {
 public:
  enum class Operation { GET, FORGET };

  GDPRStatement(Operation operation, const std::string &shard_kind,
                const Value &user_id)
      : AbstractStatement(AbstractStatement::Type::GDPR),
        operation_(operation),
        shard_kind_(shard_kind),
        user_id_(user_id) {}

  // Accessors.
  const Operation &operation() const { return this->operation_; }
  const std::string &shard_kind() const { return this->shard_kind_; }
  const Value &user_id() const { return this->user_id_; }

  template <class T>
  T Visit(AbstractVisitor<T> *visitor) const {
    return visitor->VisitGDPRStatement(*this);
  }
  template <class T>
  T Visit(MutableVisitor<T> *visitor) {
    return visitor->VisitGDPRStatement(this);
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
  Operation operation_;
  std::string shard_kind_;
  Value user_id_;
};

class ExplainQuery : public AbstractStatement {
 public:
  explicit ExplainQuery(std::unique_ptr<AbstractStatement> &&query)
      : AbstractStatement(AbstractStatement::Type::EXPLAIN_QUERY),
        query_(std::move(query)) {}

  const AbstractStatement *GetQuery() const { return this->query_.get(); }

  template <class T>
  T Visit(AbstractVisitor<T> *visitor) const {
    return visitor->VisitExplainQuery(*this);
  }
  template <class T>
  T Visit(MutableVisitor<T> *visitor) {
    return visitor->VisitExplainQuery(this);
  }
  template <class T>
  std::vector<T> VisitChildren(AbstractVisitor<T> *visitor) const {
    return {this->query_->Visit(visitor)};
  }
  template <class T>
  std::vector<T> VisitChildren(MutableVisitor<T> *visitor) {
    return {this->query_->Visit(visitor)};
  }

 private:
  std::unique_ptr<AbstractStatement> query_;
};

}  // namespace sqlast
}  // namespace k9db

#endif  // K9DB_SQLAST_AST_STATEMENTS_H_
