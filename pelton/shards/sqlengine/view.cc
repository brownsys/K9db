// Creation and management of dataflows.

#include "pelton/shards/sqlengine/view.h"

#include <cstring>
#include <iterator>
#include <list>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/ops/matview.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/value.h"
#include "pelton/planner/planner.h"
#include "pelton/util/merge_sort.h"
#include "pelton/util/perf.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace view {

namespace {

using Constraint = std::vector<std::pair<size_t, std::string>>;

struct LookupCondition {
  std::optional<dataflow::Record> greater_record;
  std::optional<dataflow::Key> greater_key;
  std::optional<std::vector<dataflow::Key>> equality_keys;
};

std::vector<dataflow::Record> LookupRecords(dataflow::MatViewOperator *matview,
                                            const LookupCondition &condition,
                                            int limit, size_t offset) {
  int olimit = limit > -1 ? limit + offset : -1;

  // By ordered key.
  if (condition.greater_key) {
    auto ord_view = static_cast<dataflow::KeyOrderedMatViewOperator *>(matview);
    auto result = ord_view->LookupGreater(*condition.greater_key, olimit);
    util::Trim(&result, limit, offset);
    return result;
  }

  // By ordered record and potentially key(s).
  if (matview->RecordOrdered()) {
    auto ord_view =
        static_cast<dataflow::RecordOrderedMatViewOperator *>(matview);
    if (!condition.greater_record) {
      if (!condition.equality_keys) {
        auto result = ord_view->All(olimit);
        util::Trim(&result, limit, offset);
        return result;
      } else if (condition.equality_keys->size() == 1) {
        return ord_view->Lookup(condition.equality_keys->at(0), limit, offset);
      } else {
        std::list<dataflow::Record> result;
        for (const auto &key : *condition.equality_keys) {
          util::MergeInto(&result, ord_view->Lookup(key, olimit),
                          ord_view->comparator(), olimit);
        }
        return util::ToVector(&result, limit, offset);
      }
    } else {
      if (!condition.equality_keys) {
        auto result = ord_view->All(*condition.greater_record, olimit);
        util::Trim(&result, limit, offset);
        return result;
      } else if (condition.equality_keys->size() == 1) {
        return ord_view->LookupGreater(condition.equality_keys->at(0),
                                       *condition.greater_record, limit,
                                       offset);
      } else {
        std::list<dataflow::Record> result;
        for (const auto &key : *condition.equality_keys) {
          util::MergeInto(
              &result,
              ord_view->LookupGreater(key, *condition.greater_record, olimit),
              ord_view->comparator(), olimit);
        }
        return util::ToVector(&result, limit, offset);
      }
    }
  }

  // By key(s).
  if (!condition.equality_keys) {
    auto result = matview->All(olimit);
    util::Trim(&result, limit, offset);
    return result;
  } else if (condition.equality_keys->size() == 1) {
    return matview->Lookup(condition.equality_keys->at(0), limit, offset);
  } else {
    std::vector<dataflow::Record> result;
    for (const auto &key : *condition.equality_keys) {
      auto vec = matview->Lookup(key, olimit);
      result.insert(result.end(), std::make_move_iterator(vec.begin()),
                    std::make_move_iterator(vec.end()));
    }
    util::Trim(&result, limit, offset);
    return result;
  }
}

// Constructing a comparator record from values.
dataflow::Record MakeRecord(const Constraint &constraint,
                            const std::vector<uint32_t> &comp_col,
                            const dataflow::SchemaRef &schema) {
  dataflow::Record record{schema, true};
  // Fill in key.
  for (size_t col : comp_col) {
    bool found_column = false;
    for (const auto &[index, value] : constraint) {
      if (index == col) {
        found_column = true;
        record.SetValue(value, index);
        break;
      }
    }
    if (!found_column) {
      LOG(FATAL) << "Underspecified record order in view lookup";
    }
  }
  return record;
}
// Constructing keys from values.
dataflow::Key MakeKey(const Constraint &constraint,
                      const std::vector<uint32_t> &key_cols,
                      const dataflow::SchemaRef &schema) {
  dataflow::Key key(key_cols.size());
  // Fill in key.
  for (size_t col : key_cols) {
    bool found_column = false;
    for (const auto &[index, value] : constraint) {
      if (index == col) {
        found_column = true;
        if (value == "NULL") {
          key.AddNull(schema.TypeOf(index));
        } else {
          switch (schema.TypeOf(index)) {
            case sqlast::ColumnDefinition::Type::UINT:
              key.AddValue(static_cast<uint64_t>(std::stoull(value)));
              break;
            case sqlast::ColumnDefinition::Type::INT:
              key.AddValue(static_cast<int64_t>(std::stoll(value)));
              break;
            case sqlast::ColumnDefinition::Type::TEXT:
            case sqlast::ColumnDefinition::Type::DATETIME:
              key.AddValue(dataflow::Record::Dequote(value));
              break;
            default:
              LOG(FATAL) << "Unsupported type in view read";
          }
        }
        break;
      }
    }
    if (!found_column) {
      LOG(FATAL) << "Underspecified key in view lookup";
    }
  }
  return key;
}

// sqlast casts and extractions.
const sqlast::ColumnExpression *ExtractColumnFromConstraint(
    const sqlast::BinaryExpression *expr) {
  if (expr->GetLeft()->type() == expr->GetRight()->type()) {
    LOG(FATAL) << "Binary expression left and right have same type!";
  }
  if (expr->GetLeft()->type() == sqlast::Expression::Type::COLUMN) {
    return static_cast<const sqlast::ColumnExpression *>(expr->GetLeft());
  } else if (expr->GetRight()->type() == sqlast::Expression::Type::COLUMN) {
    return static_cast<const sqlast::ColumnExpression *>(expr->GetRight());
  } else {
    LOG(FATAL) << "No column in binary expression!";
  }
}
const sqlast::LiteralExpression *ExtractLiteralFromConstraint(
    const sqlast::BinaryExpression *expr) {
  if (expr->GetLeft()->type() == expr->GetRight()->type()) {
    LOG(FATAL) << "Binary expression left and right have same type!";
  }
  if (expr->GetLeft()->type() == sqlast::Expression::Type::LITERAL) {
    return static_cast<const sqlast::LiteralExpression *>(expr->GetLeft());
  } else if (expr->GetRight()->type() == sqlast::Expression::Type::LITERAL) {
    return static_cast<const sqlast::LiteralExpression *>(expr->GetRight());
  } else {
    LOG(FATAL) << "No literal in binary expression!";
  }
}

// Constraint extractions.
Constraint FindGreaterThanConstraints(dataflow::MatViewOperator *matview,
                                      const sqlast::BinaryExpression *expr) {
  Constraint result;
  switch (expr->type()) {
    // Make sure Greater than makes sense.
    case sqlast::Expression::Type::GREATER_THAN: {
      if (expr->GetLeft()->type() != sqlast::Expression::Type::COLUMN) {
        LOG(FATAL) << "Bad greater than in view select";
      }
      const auto &column = ExtractColumnFromConstraint(expr)->column();
      size_t col = matview->output_schema().IndexOf(column);
      const std::string &val = ExtractLiteralFromConstraint(expr)->value();
      result.emplace_back(col, val);
      break;
    }
    // Check sub expressions of AND.
    case sqlast::Expression::Type::AND: {
      auto l = static_cast<const sqlast::BinaryExpression *>(expr->GetLeft());
      auto r = static_cast<const sqlast::BinaryExpression *>(expr->GetRight());
      auto c1 = FindGreaterThanConstraints(matview, l);
      auto c2 = FindGreaterThanConstraints(matview, r);
      result.insert(result.end(), std::make_move_iterator(c1.begin()),
                    std::make_move_iterator(c1.end()));
      result.insert(result.end(), std::make_move_iterator(c2.begin()),
                    std::make_move_iterator(c2.end()));
      break;
    }
    // Do nothing for EQ or IN.
    case sqlast::Expression::Type::EQ:
      break;
    case sqlast::Expression::Type::IN:
      break;
    // LITERAL, COLUMN, LIST.
    default:
      LOG(FATAL) << "Bad where condition in view select";
  }
  return result;
}

std::vector<Constraint> FindEqualityConstraints(
    dataflow::MatViewOperator *matview, const sqlast::BinaryExpression *expr) {
  std::vector<Constraint> result;
  switch (expr->type()) {
    // EQ: easy, one constraint!
    case sqlast::Expression::Type::EQ: {
      const auto &column = ExtractColumnFromConstraint(expr)->column();
      size_t col = matview->output_schema().IndexOf(column);
      const std::string &val = ExtractLiteralFromConstraint(expr)->value();
      result.push_back({std::make_pair(col, val)});
      break;
    }
    // IN: each value is a separate OR constraint!
    case sqlast::Expression::Type::IN: {
      const auto &column = ExtractColumnFromConstraint(expr)->column();
      size_t col = matview->output_schema().IndexOf(column);
      auto l =
          static_cast<const sqlast::LiteralListExpression *>(expr->GetRight());
      for (const std::string &val : l->values()) {
        result.push_back({std::make_pair(col, val)});
      }
      break;
    }

    // Combine AND conditions.
    case sqlast::Expression::Type::AND: {
      auto l = static_cast<const sqlast::BinaryExpression *>(expr->GetLeft());
      auto r = static_cast<const sqlast::BinaryExpression *>(expr->GetRight());
      auto v1 = FindEqualityConstraints(matview, l);
      auto v2 = FindEqualityConstraints(matview, r);
      for (const Constraint &c1 : v1) {
        for (const Constraint &c2 : v2) {
          Constraint combined;
          combined.reserve(c1.size() + c2.size());
          combined.insert(combined.end(), std::make_move_iterator(c1.begin()),
                          std::make_move_iterator(c1.end()));
          combined.insert(combined.end(), std::make_move_iterator(c2.begin()),
                          std::make_move_iterator(c2.end()));
          result.push_back(std::move(combined));
        }
      }
      break;
    }

    // Do nothing for GREATER_THAN.
    case sqlast::Expression::Type::GREATER_THAN:
      break;
    // LITERAL, COLUMN, LIST.
    default:
      LOG(FATAL) << "Bad where condition in view select";
  }
  return result;
}

// Find the different components of the WHERE condition.
LookupCondition ConstraintKeys(dataflow::MatViewOperator *matview,
                               const sqlast::BinaryExpression *where) {
  LookupCondition condition;
  const dataflow::SchemaRef &schema = matview->output_schema();
  const std::vector<uint32_t> &key_cols = matview->key_cols();

  Constraint greater = FindGreaterThanConstraints(matview, where);
  if (greater.size() > 0) {
    if (matview->RecordOrdered()) {
      auto ordered_view =
          static_cast<dataflow::RecordOrderedMatViewOperator *>(matview);
      const std::vector<uint32_t> &columns = ordered_view->comparator().cols();
      condition.greater_record = MakeRecord(greater, columns, schema);
    } else if (matview->KeyOrdered()) {
      condition.greater_key = MakeKey(greater, key_cols, schema);
    } else {
      LOG(FATAL) << "Ordered lookup into unordered view";
    }
  }

  std::vector<Constraint> equality = FindEqualityConstraints(matview, where);
  if (equality.size() > 0) {
    condition.equality_keys = std::vector<dataflow::Key>();
    for (const Constraint &constraint : equality) {
      condition.equality_keys->push_back(MakeKey(constraint, key_cols, schema));
    }
  }

  // Special pattern: ... WHERE key = <v> AND key > <x>.
  if (condition.equality_keys && condition.greater_key) {
    LOG(FATAL) << "Where clause has equality and greater conditions on key";
  }

  return condition;
}

}  // namespace

absl::Status CreateView(const sqlast::CreateView &stmt, SharderState *state,
                        dataflow::DataFlowState *dataflow_state) {
  perf::Start("CreateView");
  // Plan the query using calcite and generate a concrete graph for it.
  std::unique_ptr<dataflow::DataFlowGraphPartition> graph =
      planner::PlanGraph(dataflow_state, stmt.query());

  // Add The flow to state so that data is fed into it on INSERT/UPDATE/DELETE.
  dataflow_state->AddFlow(stmt.view_name(), std::move(graph));

  perf::End("CreateView");
  return absl::OkStatus();
}

absl::StatusOr<sql::SqlResult> SelectView(
    const sqlast::Select &stmt, SharderState *state,
    dataflow::DataFlowState *dataflow_state) {
  // TODO(babman): fix this.
  perf::Start("SelectView");

  // Get the corresponding flow.
  const std::string &view_name = stmt.table_name();
  const dataflow::DataFlowGraphPartition &flow =
      dataflow_state->GetFlow(view_name);

  // Only support flow ending with a single output matview for now.
  if (flow.outputs().size() == 0) {
    return absl::InvalidArgumentError("Read from flow with no matviews");
  } else if (flow.outputs().size() > 1) {
    return absl::InvalidArgumentError("Read from flow with several matviews");
  }
  dataflow::MatViewOperator *matview = flow.outputs().at(0);
  const dataflow::SchemaRef &schema = matview->output_schema();

  std::vector<dataflow::Record> records;

  // Check parameters.
  size_t offset = stmt.offset();
  int limit = stmt.limit();

  if (stmt.HasWhereClause()) {
    LookupCondition condition = ConstraintKeys(matview, stmt.GetWhereClause());
    records = LookupRecords(matview, condition, limit, offset);
  } else {
    records = LookupRecords(matview, {}, limit, offset);
  }

  perf::End("SelectView");
  return sql::SqlResult(std::make_unique<sql::_result::SqlInlineResultSet>(
      schema, std::move(records)));
}

}  // namespace view
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
