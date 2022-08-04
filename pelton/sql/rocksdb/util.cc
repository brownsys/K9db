#include "pelton/sql/rocksdb/util.h"

#include <memory>

#include "glog/logging.h"

namespace pelton {
namespace sql {

#define __NAME true
#define __VALUE false
#define __ROCKSSEP 30
#define __ROCKSNULL 0

// Comparisons over rocksdb::Slice.
bool SlicesEq(const rocksdb::Slice &l, const rocksdb::Slice &r) {
  if (l.size() != r.size()) {
    return false;
  }
  for (size_t i = 0; i < l.size(); i++) {
    if (l.data()[i] != r.data()[i]) {
      return false;
    }
  }
  return true;
}
bool SlicesGt(const rocksdb::Slice &l, const rocksdb::Slice &r) {
  if (l.size() > r.size()) {
    return true;
  } else if (l.size() < r.size()) {
    return false;
  }
  for (size_t i = 0; i < l.size(); i++) {
    if (l.data()[i] > r.data()[i]) {
      return true;
    } else if (l.data()[i] < r.data()[i]) {
      return false;
    }
  }
  return false;
}

std::string Update(const std::unordered_map<std::string, std::string> &update,
                   const dataflow::SchemaRef &schema, const std::string &str) {
  std::string replaced = "";
  size_t begin = 0;
  size_t end = 0;
  for (size_t i = 0; i < schema.size(); i++) {
    while (str[end] != __ROCKSSEP) {
      end++;
    }
    const std::string &name = schema.NameOf(i);
    if (update.count(name) == 1) {
      rocksdb::Slice slice = EncodeValue(schema.TypeOf(i), &update.at(name));
      replaced.reserve(replaced.size() + slice.size() + 1);
      for (size_t j = 0; j < slice.size(); j++) {
        replaced.push_back(slice.data()[j]);
      }
      replaced.push_back(__ROCKSSEP);
    } else {
      replaced.reserve(replaced.size() + end - begin + 1);
      for (size_t j = begin; j < end; j++) {
        replaced.push_back(str.at(j));
      }
      replaced.push_back(__ROCKSSEP);
    }
    begin = end + 1;
    end = begin;
  }
  return replaced;
}

// Find all values mapped to any column from a column set.
ValuePair ValueMapper::VisitCreateTable(const sqlast::CreateTable &ast) {
  LOG(FATAL) << "ValueMapper unsupported statement";
}
ValuePair ValueMapper::VisitColumnDefinition(
    const sqlast::ColumnDefinition &ast) {
  LOG(FATAL) << "ValueMapper unsupported statement";
}
ValuePair ValueMapper::VisitColumnConstraint(
    const sqlast::ColumnConstraint &ast) {
  LOG(FATAL) << "ValueMapper unsupported statement";
}
ValuePair ValueMapper::VisitCreateIndex(const sqlast::CreateIndex &ast) {
  LOG(FATAL) << "ValueMapper unsupported statement";
}
ValuePair ValueMapper::VisitInsert(const sqlast::Insert &ast) {
  // What gets inserted.
  if (ast.HasColumns()) {
    const std::vector<std::string> &cols = ast.GetColumns();
    for (size_t i = 0; i < cols.size(); i++) {
      const std::string &name = cols.at(i);
      if (this->cols_.count(name) == 1) {
        rocksdb::Slice slice =
            EncodeValue(this->cols_.at(name), &ast.GetValue(i));
        this->after_.emplace(name, std::vector<rocksdb::Slice>{slice});
      }
    }
  } else {
    for (const auto &[name, i] : this->indices_) {
      rocksdb::Slice slice =
          EncodeValue(this->cols_.at(name), &ast.GetValue(i));
      this->after_.emplace(name, std::vector<rocksdb::Slice>{slice});
    }
  }
  return {};
}
ValuePair ValueMapper::VisitUpdate(const sqlast::Update &ast) {
  // Visit the where condition.
  ast.VisitChildren(this);

  // Visit what gets set.
  const std::vector<std::string> &cols = ast.GetColumns();
  const std::vector<std::string> &values = ast.GetValues();
  for (size_t i = 0; i < cols.size(); i++) {
    const std::string &col = cols.at(i);
    if (this->cols_.count(col) == 1) {
      rocksdb::Slice slice = EncodeValue(this->cols_.at(col), &values.at(i));
      this->after_.emplace(cols.at(i), std::vector<rocksdb::Slice>{slice});
    }
  }
  return {};
}
ValuePair ValueMapper::VisitSelect(const sqlast::Select &ast) {
  ast.VisitChildren(this);
  return {};
}
ValuePair ValueMapper::VisitDelete(const sqlast::Delete &ast) {
  ast.VisitChildren(this);
  return {};
}
ValuePair ValueMapper::VisitColumnExpression(
    const sqlast::ColumnExpression &ast) {
  return std::make_pair(__NAME,
                        std::vector<const std::string *>{&ast.column()});
}
ValuePair ValueMapper::VisitLiteralExpression(
    const sqlast::LiteralExpression &ast) {
  return std::make_pair(__VALUE,
                        std::vector<const std::string *>{&ast.value()});
}
ValuePair ValueMapper::VisitLiteralListExpression(
    const sqlast::LiteralListExpression &ast) {
  std::vector<const std::string *> vals;
  for (const std::string &v : ast.values()) {
    vals.push_back(&v);
  }
  return std::make_pair(__VALUE, std::move(vals));
}
ValuePair ValueMapper::VisitBinaryExpression(
    const sqlast::BinaryExpression &ast) {
  // Visit children.
  auto result = ast.VisitChildren(this);
  auto &left = result.at(0);
  auto &right = result.at(1);
  bool left_name = left.first == __NAME;
  bool right_name = right.first == __NAME;

  // By expression type.
  switch (ast.type()) {
    case sqlast::Expression::Type::IN:
    case sqlast::Expression::Type::IS:
    case sqlast::Expression::Type::EQ: {
      if (left_name && !right_name) {
        if (this->cols_.count(*left.second.at(0)) == 1) {
          const std::string *name = left.second.at(0);
          sqlast::ColumnDefinition::Type type = this->cols_.at(*name);
          std::vector<rocksdb::Slice> slices;
          for (const std::string *value : right.second) {
            slices.push_back(EncodeValue(type, value));
          }
          this->before_.emplace(*name, std::move(slices));
        }
      }
      if (right_name && !left_name) {
        if (this->cols_.count(*right.second.at(0)) == 1) {
          const std::string *name = right.second.at(0);
          sqlast::ColumnDefinition::Type type = this->cols_.at(*name);
          std::vector<rocksdb::Slice> slices;
          for (const std::string *value : left.second) {
            slices.push_back(EncodeValue(type, value));
          }
          this->before_.emplace(*name, std::move(slices));
        }
      }
      break;
    }
    case sqlast::Expression::Type::GREATER_THAN:
    case sqlast::Expression::Type::AND:
      break;
    default:
      LOG(FATAL) << "UNREACHABLE";
  }
  return {};
}

// Evaluate the where condition on given slice.
FilterVisitor::FilterVisitor(const rocksdb::Slice &slice,
                             const dataflow::SchemaRef &schema)
    : AbstractVisitor(), slice_(slice), schema_(schema) {
  size_t begin = 0;
  size_t end = 0;
  const char *data = slice.data();
  for (size_t i = 0; i < schema.size(); i++) {
    while (data[end] != __ROCKSSEP) {
      end++;
    }
    this->positions_.emplace(i, std::make_pair(begin, end - begin));
    begin = end + 1;
    end = begin;
  }
}

bool FilterVisitor::VisitCreateTable(const sqlast::CreateTable &ast) {
  LOG(FATAL) << "FilterVisitor unsupported statement";
}
bool FilterVisitor::VisitColumnDefinition(const sqlast::ColumnDefinition &ast) {
  LOG(FATAL) << "FilterVisitor unsupported statement";
}
bool FilterVisitor::VisitColumnConstraint(const sqlast::ColumnConstraint &ast) {
  LOG(FATAL) << "FilterVisitor unsupported statement";
}
bool FilterVisitor::VisitCreateIndex(const sqlast::CreateIndex &ast) {
  LOG(FATAL) << "FilterVisitor unsupported statement";
}
bool FilterVisitor::VisitInsert(const sqlast::Insert &ast) {
  LOG(FATAL) << "FilterVisitor unsupported statement";
}
bool FilterVisitor::VisitUpdate(const sqlast::Update &ast) {
  if (!ast.HasWhereClause()) {
    return true;
  }
  return ast.GetWhereClause()->Visit(this);
}
bool FilterVisitor::VisitSelect(const sqlast::Select &ast) {
  if (!ast.HasWhereClause()) {
    return true;
  }
  return ast.GetWhereClause()->Visit(this);
}
bool FilterVisitor::VisitDelete(const sqlast::Delete &ast) {
  if (!ast.HasWhereClause()) {
    return true;
  }
  return ast.GetWhereClause()->Visit(this);
}
bool FilterVisitor::VisitColumnExpression(const sqlast::ColumnExpression &ast) {
  LOG(FATAL) << "FilterVisitor unsupported statement";
}
bool FilterVisitor::VisitLiteralExpression(
    const sqlast::LiteralExpression &ast) {
  LOG(FATAL) << "FilterVisitor unsupported statement";
}
bool FilterVisitor::VisitLiteralListExpression(
    const sqlast::LiteralListExpression &ast) {
  LOG(FATAL) << "FilterVisitor unsupported statement";
}
bool FilterVisitor::VisitBinaryExpression(const sqlast::BinaryExpression &ast) {
  switch (ast.type()) {
    case sqlast::Expression::Type::IS:
    case sqlast::Expression::Type::EQ: {
      auto [left, right] = this->ToSlice(ast.GetLeft(), ast.GetRight());
      return SlicesEq(left, right);
    }
    case sqlast::Expression::Type::GREATER_THAN: {
      auto [left, right] = this->ToSlice(ast.GetLeft(), ast.GetRight());
      return SlicesGt(left, right);
    }
    case sqlast::Expression::Type::IN: {
      CHECK(ast.GetLeft()->type() == sqlast::Expression::Type::COLUMN)
          << "In left is not a column!";
      CHECK(ast.GetRight()->type() == sqlast::Expression::Type::LIST)
          << "In right is not a list!";
      const sqlast::ColumnExpression *const column =
          static_cast<const sqlast::ColumnExpression *const>(ast.GetLeft());
      size_t idx = this->schema_.IndexOf(column->column());
      sqlast::ColumnDefinition::Type type = this->schema_.TypeOf(idx);
      const auto &[begin, end] = this->positions_.at(idx);
      rocksdb::Slice left{this->slice_.data() + begin, end};
      const sqlast::LiteralListExpression *const list =
          static_cast<const sqlast::LiteralListExpression *const>(
              ast.GetRight());
      for (const std::string &value : list->values()) {
        rocksdb::Slice right = EncodeValue(type, &value);
        if (SlicesEq(left, right)) {
          return true;
        }
      }
      return false;
    }
    case sqlast::Expression::Type::AND: {
      auto result = ast.VisitChildren(this);
      return result.at(0) && result.at(1);
    }
    default:
      LOG(FATAL) << "UNREACHABLE";
  }
  return false;
}

std::pair<rocksdb::Slice, rocksdb::Slice> FilterVisitor::ToSlice(
    const sqlast::Expression *const left,
    const sqlast::Expression *const right) const {
  // One expression must be a column!
  bool col_is_left = (left->type() == sqlast::Expression::Type::COLUMN);
  const sqlast::Expression *const colexpr = col_is_left ? left : right;
  const sqlast::Expression *const othexpr = col_is_left ? right : left;
  CHECK(colexpr->type() == sqlast::Expression::Type::COLUMN) << "No column!";

  // Turn column into slice.
  const sqlast::ColumnExpression *const column =
      static_cast<const sqlast::ColumnExpression *const>(colexpr);
  size_t idx = this->schema_.IndexOf(column->column());
  sqlast::ColumnDefinition::Type type = this->schema_.TypeOf(idx);
  const auto &[begin, end] = this->positions_.at(idx);
  rocksdb::Slice colslice = rocksdb::Slice{this->slice_.data() + begin, end};

  // Turn other expression to slice.
  rocksdb::Slice othslice;
  switch (othexpr->type()) {
    case sqlast::Expression::Type::COLUMN: {
      const sqlast::ColumnExpression *const column =
          static_cast<const sqlast::ColumnExpression *const>(othexpr);
      size_t idx = this->schema_.IndexOf(column->column());
      const auto &[begin, end] = this->positions_.at(idx);
      othslice = rocksdb::Slice{this->slice_.data() + begin, end};
      break;
    }
    case sqlast::Expression::Type::LITERAL: {
      const sqlast::LiteralExpression *const literal =
          static_cast<const sqlast::LiteralExpression *const>(othexpr);
      othslice = EncodeValue(type, &literal->value());
      break;
    }
    default: {
      LOG(FATAL) << "UNREACHABLE";
    }
  }
  return std::make_pair(col_is_left ? colslice : othslice,
                        col_is_left ? othslice : colslice);
}

}  // namespace sql
}  // namespace pelton
