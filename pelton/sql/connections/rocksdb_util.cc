#include "pelton/sql/connections/rocksdb_util.h"

#include <memory>

#include "glog/logging.h"

namespace pelton {
namespace sql {

#define __NAME true
#define __VALUE false
#define __ROCKSSEP 30
#define __ROCKSNULL 0

namespace {

// NOLINTNEXTLINE
const std::string NULLSTR = "NULL";
const char NULLBUF[1] = {__ROCKSNULL};

// Encoding values as rocksdb::Slice.
rocksdb::Slice EncodeValue(sqlast::ColumnDefinition::Type type,
                           const std::string *val) {
  if (*val == NULLSTR) {
    return {NULLBUF, sizeof(NULLBUF)};
  }
  switch (type) {
    case sqlast::ColumnDefinition::Type::INT:
    case sqlast::ColumnDefinition::Type::UINT:
      return rocksdb::Slice{val->data(), val->size()};
    case sqlast::ColumnDefinition::Type::TEXT:
      return rocksdb::Slice{val->data() + 1, val->size() - 2};
    case sqlast::ColumnDefinition::Type::DATETIME:
      if (val->starts_with('"') || val->starts_with('\'')) {
        return rocksdb::Slice{val->data() + 1, val->size() - 2};
      }
      return rocksdb::Slice{val->data(), val->size()};
    default:
      LOG(FATAL) << "UNREACHABLE";
  }
}
// Decode value from a slice into a record.
void DecodeValue(sqlast::ColumnDefinition::Type type,
                 const rocksdb::Slice &slice, size_t idx,
                 dataflow::Record *record) {
  if (slice.size() == 1 && slice.data()[0] == __ROCKSNULL) {
    record->SetNull(true, idx);
    return;
  }
  std::string value(slice.data(), slice.size());
  switch (type) {
    case sqlast::ColumnDefinition::Type::UINT: {
      record->SetUInt(std::stoul(value), idx);
      break;
    }
    case sqlast::ColumnDefinition::Type::INT: {
      record->SetInt(std::stol(value), idx);
      break;
    }
    case sqlast::ColumnDefinition::Type::TEXT: {
      record->SetString(std::make_unique<std::string>(std::move(value)), idx);
      break;
    }
    case sqlast::ColumnDefinition::Type::DATETIME: {
      record->SetDateTime(std::make_unique<std::string>(std::move(value)), idx);
      break;
    }
  }
}

}  // namespace

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

// Returns true if the next value in buff represents a null.
bool IsNull(const char *buf, size_t size) {
  return size > 0 && buf[0] == __ROCKSNULL;
}

// Encode the values inside an Insert statement as a rocksdb value string.
std::string EncodeInsert(const sqlast::Insert &stmt,
                         const dataflow::SchemaRef &schema) {
  std::string encoded = "";
  for (size_t i = 0; i < schema.size(); i++) {
    int idx = static_cast<size_t>(i);
    if (stmt.HasColumns()) {
      idx = stmt.GetColumnIndex(schema.NameOf(i));
    }
    const std::string *value = &NULLSTR;
    if (idx > -1) {
      value = &stmt.GetValue(idx);
    }
    rocksdb::Slice slice = EncodeValue(schema.TypeOf(i), value);
    encoded += std::string(slice.data(), slice.size());
    encoded += __ROCKSSEP;
  }
  return encoded;
}

// Decode the value from rocksdb to a record.
dataflow::Record DecodeRecord(const rocksdb::Slice &slice,
                              const dataflow::SchemaRef &schema,
                              const std::vector<AugInfo> &augments,
                              const std::vector<int> &projections) {
  dataflow::Record record{schema, false};
  if (projections.size() == 0) {
    size_t begin = 0;
    size_t end = 0;
    const char *data = slice.data();
    for (size_t i = 0; i < schema.size(); i++) {
      rocksdb::Slice value;
      if (augments.size() == 1 && i == augments.front().index) {
        // Augmented value.
        const std::string &aug = augments.front().value;
        value = rocksdb::Slice(aug.data(), aug.size());
      } else {
        // Value must be read from input slice.
        while (data[end] != __ROCKSSEP) {
          end++;
        }
        // We are done with one value.
        value = rocksdb::Slice(data + begin, end - begin);
        // Continue to the next value.
        begin = end + 1;
        end = begin;
      }
      DecodeValue(schema.TypeOf(i), value, i, &record);
    }
  } else {
    // map[i] -> region of slice for column i.
    std::vector<std::pair<size_t, size_t>> map;
    size_t begin = 0;
    for (size_t end = 0; end < slice.size(); end++) {
      if (slice.data()[end] == __ROCKSSEP) {
        map.emplace_back(begin, end - begin);
        begin = end + 1;
      }
    }
    for (size_t i = 0; i < projections.size(); i++) {
      size_t target = projections.at(i);
      rocksdb::Slice value;
      if (target == -1) {
        const std::string &aug = augments.front().value;
        value = rocksdb::Slice(aug.data(), aug.size());
      } else if (target == -2) {
        // Literal projection.
        value = EncodeValue(schema.TypeOf(i), &schema.NameOf(i));
      } else {
        auto &pair = map.at(target);
        value = rocksdb::Slice(slice.data() + pair.first, pair.second);
      }
      DecodeValue(schema.TypeOf(i), value, i, &record);
    }
  }
  return record;
}

rocksdb::Slice ExtractColumn(const rocksdb::Slice &slice, size_t col) {
  size_t begin = 0;
  size_t end = 0;
  const char *data = slice.data();
  for (size_t i = 0; i <= col; i++) {
    while (data[end] != __ROCKSSEP) {
      end++;
    }
    if (i < col) {
      begin = end + 1;
      end = begin;
    }
  }
  return rocksdb::Slice{data + begin, end - begin};
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

// Schema Manipulation.
std::vector<int> ProjectionSchema(const dataflow::SchemaRef &db_schema,
                                  const dataflow::SchemaRef &out_schema,
                                  const std::vector<AugInfo> &augments) {
  std::vector<int> projections;
  for (size_t i = 0; i < out_schema.size(); i++) {
    if (augments.size() == 1 && i == augments.front().index) {
      projections.push_back(-1);
    } else {
      const std::string &name = out_schema.NameOf(i);
      if ((name[0] >= '0' && name[0] <= '9') || name[0] == '\'') {
        projections.push_back(-2);
      } else {
        size_t idx = db_schema.IndexOf(out_schema.NameOf(i));
        projections.push_back(static_cast<int>(idx));
      }
    }
  }
  return projections;
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

rocksdb::Slice ShardPrefixTransform::Transform(
    const rocksdb::Slice &key) const {
  size_t count = 0;
  for (size_t i = 0; i < key.size(); i++) {
    if (key.data()[i] == 30) {
      count++;
      if (count == this->seps_) {
        return rocksdb::Slice(key.data(), i + 1);
      }
    }
  }
  LOG(FATAL) << "Cannot find separator in Transform" << key.ToString();
}

}  // namespace sql
}  // namespace pelton
