// Insert, delete and select statements.
#ifndef PELTON_SQLAST_AST_STATEMENTS_H_
#define PELTON_SQLAST_AST_STATEMENTS_H_

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "pelton/sqlast/ast_abstract.h"
#include "pelton/sqlast/ast_expressions.h"

namespace pelton {
namespace sqlast {

// Insert statement.
class Insert : public AbstractStatement {
 public:
  Insert(const std::string &table_name, bool replace)
      : AbstractStatement(AbstractStatement::Type::INSERT),
        table_name_(table_name),
        replace_(replace),
        returning_(false) {}

  // Accessors.
  const std::string &table_name() const;
  std::string &table_name();
  bool replace() const;
  bool &replace();
  bool returning() const { return this->returning_; }
  Insert MakeReturning() const {
    Insert stmt = *this;
    stmt.returning_ = true;
    return stmt;
  }

  // Columns and Values.
  bool HasColumns() const;
  void AddColumn(const std::string &colname);
  const std::vector<std::string> &GetColumns() const;
  void SetValues(std::vector<std::string> &&values);
  const std::vector<std::string> &GetValues() const;

  void AddValue(const std::string &val);
  void AddValue(std::string &&val);

  absl::StatusOr<std::string> RemoveValue(const std::string &colname);
  std::string RemoveValue(size_t index);
  absl::StatusOr<std::string> GetValue(const std::string &colname) const;
  const std::string &GetValue(size_t index) const;

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
  bool replace_;
  bool returning_;
};

class Select : public AbstractStatement {
 public:
  explicit Select(const std::string &table_name)
      : AbstractStatement(AbstractStatement::Type::SELECT),
        table_name_(table_name) {
    this->offset_ = 0;
    this->limit_ = -1;
  }

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

  const size_t &offset() const { return this->offset_; }
  size_t &offset() { return this->offset_; }
  const int &limit() const { return this->limit_; }
  int &limit() { return this->limit_; }

  void AddColumn(const std::string &column);
  const std::vector<std::string> &GetColumns() const;
  std::vector<std::string> &GetColumns();
  int RemoveColumn(const std::string &column);

  bool HasWhereClause() const;
  const BinaryExpression *const GetWhereClause() const;
  BinaryExpression *const GetWhereClause();
  void SetWhereClause(std::unique_ptr<BinaryExpression> &&where);
  void RemoveWhereClause();

  // We use Select to represent queries selecting data from flows, as well
  // as those selecting data from underlying sharded tables. Not all fetures
  // all support when accessing underlying sharded tables. This function
  // checks whether all of the contents of this query are supported for shard
  // access.
  bool SupportedByShards() const {
    return this->offset_ == 0 && this->limit_ == -1;
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
  size_t offset_;
  int limit_;
};

class Delete : public AbstractStatement {
 public:
  explicit Delete(const std::string &table_name, bool returning = false)
      : AbstractStatement(AbstractStatement::Type::DELETE),
        table_name_(table_name),
        returning_(returning) {}

  Delete(const Delete &del)
      : AbstractStatement(AbstractStatement::Type::DELETE) {
    this->table_name_ = del.table_name_;
    this->returning_ = del.returning_;
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

  Select SelectDomain() const;

  // Whether the delete statement is also a returning query.
  bool returning() const { return this->returning_; }
  Delete MakeReturning() const {
    Delete stmt = *this;
    stmt.returning_ = true;
    return stmt;
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
  bool returning_;
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
  absl::StatusOr<std::string> RemoveColumnValue(const std::string &column);
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

  Select SelectDomain() const;
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

class CreateView : public AbstractStatement {
 public:
  CreateView(const std::string &view_name, const std::string &query)
      : AbstractStatement(AbstractStatement::Type::CREATE_VIEW),
        view_name_(view_name),
        query_(query) {}

  // Accessors.
  const std::string &view_name() const { return this->view_name_; }
  const std::string &query() const { return this->query_; }

 private:
  std::string view_name_;
  std::string query_;
};

class GDPRStatement : public AbstractStatement {
 public:
  enum class Operation { GET, FORGET };

  GDPRStatement(Operation operation, const std::string &shard_kind,
                const std::string &user_id)
      : AbstractStatement(AbstractStatement::Type::GDPR),
        operation_(operation),
        shard_kind_(shard_kind),
        user_id_(user_id) {}

  // Accessors.
  const Operation &operation() const { return this->operation_; }
  const std::string &shard_kind() const { return this->shard_kind_; }
  const std::string &user_id() const { return this->user_id_; }

 private:
  Operation operation_;
  std::string shard_kind_;
  std::string user_id_;
};

}  // namespace sqlast
}  // namespace pelton

#endif  // PELTON_SQLAST_AST_STATEMENTS_H_
