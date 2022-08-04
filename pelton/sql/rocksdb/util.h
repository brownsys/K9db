// Connection to rocksdb.
#ifndef PELTON_SQL_ROCKSDB_UTIL_H__
#define PELTON_SQL_ROCKSDB_UTIL_H__

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sql/abstract_connection.h"
#include "pelton/sqlast/ast.h"
#include "rocksdb/slice.h"

namespace pelton {
namespace sql {

// Comparisons over rocksdb::Slice.
bool SlicesEq(const rocksdb::Slice &l, const rocksdb::Slice &r);
bool SlicesGt(const rocksdb::Slice &l, const rocksdb::Slice &r);

std::string Update(const std::unordered_map<std::string, std::string> &update,
                   const dataflow::SchemaRef &schema, const std::string &str);

// Transforms Where clauses into a map from variable name to condition values.
// std::string -> std::vector<std::string>
// values inside std::vector are basically an OR.
using ValuePair = std::pair<bool, std::vector<const std::string *>>;
class ValueMapper : public sqlast::AbstractVisitor<ValuePair> {
 public:
  ValueMapper(size_t pk, const std::vector<size_t> &indices,
              const dataflow::SchemaRef &schema)
      : AbstractVisitor() {
    for (size_t col : indices) {
      const std::string &name = schema.NameOf(col);
      this->indices_.emplace(name, col);
      this->cols_.emplace(name, schema.TypeOf(col));
    }
    const std::string &name = schema.NameOf(pk);
    this->indices_.emplace(name, pk);
    this->cols_.emplace(name, schema.TypeOf(pk));
  }

  // Get the value(s) of a column.
  bool HasBefore(const std::string &col) const {
    return this->before_.count(col) == 1;
  }
  bool HasAfter(const std::string &col) const {
    return this->after_.count(col) == 1;
  }
  const std::vector<rocksdb::Slice> &Before(const std::string &col) const {
    return this->before_.at(col);
  }
  const std::vector<rocksdb::Slice> &After(const std::string &col) const {
    return this->after_.at(col);
  }
  void SetPK(std::string pk, rocksdb::Slice value) {
    this->before_[pk] = {value};
  }

  // Visitors.
  ValuePair VisitCreateTable(const sqlast::CreateTable &ast) override;
  ValuePair VisitColumnDefinition(const sqlast::ColumnDefinition &ast) override;
  ValuePair VisitColumnConstraint(const sqlast::ColumnConstraint &ast) override;
  ValuePair VisitCreateIndex(const sqlast::CreateIndex &ast) override;
  ValuePair VisitInsert(const sqlast::Insert &ast) override;
  ValuePair VisitUpdate(const sqlast::Update &ast) override;
  ValuePair VisitSelect(const sqlast::Select &ast) override;
  ValuePair VisitDelete(const sqlast::Delete &ast) override;
  ValuePair VisitColumnExpression(const sqlast::ColumnExpression &ast) override;
  ValuePair VisitLiteralExpression(
      const sqlast::LiteralExpression &ast) override;
  ValuePair VisitLiteralListExpression(
      const sqlast::LiteralListExpression &ast) override;
  ValuePair VisitBinaryExpression(const sqlast::BinaryExpression &ast) override;

 private:
  std::unordered_map<std::string, size_t> indices_;
  std::unordered_map<std::string, sqlast::ColumnDefinition::Type> cols_;
  std::unordered_map<std::string, std::vector<rocksdb::Slice>> before_;
  std::unordered_map<std::string, std::vector<rocksdb::Slice>> after_;
};

class FilterVisitor : public sqlast::AbstractVisitor<bool> {
 public:
  explicit FilterVisitor(const rocksdb::Slice &slice,
                         const dataflow::SchemaRef &schema);

  // Visitors.
  bool VisitCreateTable(const sqlast::CreateTable &ast) override;
  bool VisitColumnDefinition(const sqlast::ColumnDefinition &ast) override;
  bool VisitColumnConstraint(const sqlast::ColumnConstraint &ast) override;
  bool VisitCreateIndex(const sqlast::CreateIndex &ast) override;
  bool VisitInsert(const sqlast::Insert &ast) override;
  bool VisitUpdate(const sqlast::Update &ast) override;
  bool VisitSelect(const sqlast::Select &ast) override;
  bool VisitDelete(const sqlast::Delete &ast) override;
  bool VisitColumnExpression(const sqlast::ColumnExpression &ast) override;
  bool VisitLiteralExpression(const sqlast::LiteralExpression &ast) override;
  bool VisitLiteralListExpression(
      const sqlast::LiteralListExpression &ast) override;
  bool VisitBinaryExpression(const sqlast::BinaryExpression &ast) override;

 private:
  std::pair<rocksdb::Slice, rocksdb::Slice> ToSlice(
      const sqlast::Expression *const left,
      const sqlast::Expression *const right) const;

  rocksdb::Slice slice_;
  dataflow::SchemaRef schema_;
  std::unordered_map<size_t, std::pair<size_t, size_t>> positions_;
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_ROCKSDB_UTIL_H__
