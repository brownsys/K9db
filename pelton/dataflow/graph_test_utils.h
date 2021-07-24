#ifndef PELTON_DATAFLOW_GRAPH_TEST_UTILS_H_
#define PELTON_DATAFLOW_GRAPH_TEST_UTILS_H_

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/ops/aggregate.h"
#include "pelton/dataflow/ops/equijoin.h"
#include "pelton/dataflow/ops/filter.h"
#include "pelton/dataflow/ops/identity.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/ops/matview.h"
#include "pelton/dataflow/ops/project.h"
#include "pelton/dataflow/ops/union.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/ints.h"

namespace pelton {
namespace dataflow {

using CType = sqlast::ColumnDefinition::Type;

// Make schemas.
SchemaRef MakeLeftSchema() {
  std::vector<std::string> names = {"ID", "Item", "Category"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  return SchemaFactory::Create(names, types, keys);
}

SchemaRef MakeRightSchema() {
  std::vector<std::string> names = {"Category", "Description"};
  std::vector<CType> types = {CType::INT, CType::TEXT};
  std::vector<ColumnID> keys;
  return SchemaFactory::Create(names, types, keys);
}

// Make records.
inline std::vector<Record> MakeLeftRecords(const SchemaRef &schema) {
  // Allocate some unique_ptrs.
  std::unique_ptr<std::string> si1 = std::make_unique<std::string>("item0");
  std::unique_ptr<std::string> si2 = std::make_unique<std::string>("item1");
  std::unique_ptr<std::string> si3 = std::make_unique<std::string>("item2");
  std::unique_ptr<std::string> si4 = std::make_unique<std::string>("item3");
  std::unique_ptr<std::string> si5 = std::make_unique<std::string>("item4");
  // Make records.
  std::vector<Record> records;
  records.emplace_back(schema);
  records.emplace_back(schema);
  records.emplace_back(schema);
  records.emplace_back(schema);
  records.emplace_back(schema);
  // Record 1.
  records.at(0).SetUInt(0UL, 0);
  records.at(0).SetString(std::move(si1), 1);
  records.at(0).SetInt(1L, 2);
  // Record 2.
  records.at(1).SetUInt(4UL, 0);
  records.at(1).SetString(std::move(si2), 1);
  records.at(1).SetInt(3L, 2);
  // Record 3.
  records.at(2).SetUInt(5UL, 0);
  records.at(2).SetString(std::move(si3), 1);
  records.at(2).SetInt(5L, 2);
  // Record 4.
  records.at(3).SetUInt(7UL, 0);
  records.at(3).SetString(std::move(si4), 1);
  records.at(3).SetInt(1L, 2);
  // Record 5.
  records.at(4).SetUInt(2UL, 0);
  records.at(4).SetString(std::move(si5), 1);
  records.at(4).SetInt(1L, 2);
  return records;
}
inline std::vector<Record> MakeRightRecords(const SchemaRef &schema) {
  // Allocate some unique_ptrs.
  std::unique_ptr<std::string> sd1 = std::make_unique<std::string>("descrp0");
  std::unique_ptr<std::string> sd2 = std::make_unique<std::string>("descrp1");
  std::unique_ptr<std::string> sd3 = std::make_unique<std::string>("descrp2");
  // Make records.
  std::vector<Record> records;
  records.emplace_back(schema);
  records.emplace_back(schema);
  records.emplace_back(schema);
  // Record 1.
  records.at(0).SetInt(5L, 0);
  records.at(0).SetString(std::move(sd1), 1);
  // Record 2.
  records.at(1).SetInt(1L, 0);
  records.at(1).SetString(std::move(sd2), 1);
  // Record 3.
  records.at(2).SetInt(-2L, 0);
  records.at(2).SetString(std::move(sd3), 1);
  return records;
}
inline std::vector<Record> MakeJoinRecords(const SchemaRef &schema) {
  // Allocate some unique_ptrs.
  std::unique_ptr<std::string> si1 = std::make_unique<std::string>("item0");
  std::unique_ptr<std::string> si3 = std::make_unique<std::string>("item2");
  std::unique_ptr<std::string> si4 = std::make_unique<std::string>("item3");
  std::unique_ptr<std::string> si5 = std::make_unique<std::string>("item4");
  std::unique_ptr<std::string> sd1 = std::make_unique<std::string>("descrp0");
  std::unique_ptr<std::string> sd2 = std::make_unique<std::string>("descrp1");
  std::unique_ptr<std::string> sd2_ = std::make_unique<std::string>("descrp1");
  std::unique_ptr<std::string> sd2__ = std::make_unique<std::string>("descrp1");
  // Make records.
  std::vector<Record> records;
  records.emplace_back(schema);
  records.emplace_back(schema);
  records.emplace_back(schema);
  records.emplace_back(schema);
  // Record 1.
  records.at(0).SetUInt(5UL, 0);
  records.at(0).SetString(std::move(si3), 1);
  records.at(0).SetInt(5L, 2);
  records.at(0).SetString(std::move(sd1), 3);
  // Record 2.
  records.at(1).SetUInt(0UL, 0);
  records.at(1).SetString(std::move(si1), 1);
  records.at(1).SetInt(1L, 2);
  records.at(1).SetString(std::move(sd2), 3);
  // Record 3.
  records.at(2).SetUInt(7UL, 0);
  records.at(2).SetString(std::move(si4), 1);
  records.at(2).SetInt(1L, 2);
  records.at(2).SetString(std::move(sd2_), 3);
  // Record 4.
  records.at(3).SetUInt(2UL, 0);
  records.at(3).SetString(std::move(si5), 1);
  records.at(3).SetInt(1L, 2);
  records.at(3).SetString(std::move(sd2__), 3);
  return records;
}
inline std::vector<Record> MakeFilterRecords(const SchemaRef &schema) {
  // Allocate some unique_ptrs.
  std::unique_ptr<std::string> si4 = std::make_unique<std::string>("item3");
  // Make records.
  std::vector<Record> records;
  records.emplace_back(schema);
  records.at(0).SetUInt(7UL, 0);
  records.at(0).SetString(std::move(si4), 1);
  records.at(0).SetInt(1L, 2);
  return records;
}
inline std::vector<Record> MakeProjectRecords(const SchemaRef &schema) {
  // Make records.
  std::vector<Record> records;
  records.emplace_back(schema, true, 0_u, 1_s);
  records.emplace_back(schema, true, 4_u, 3_s);
  records.emplace_back(schema, true, 5_u, 5_s);
  records.emplace_back(schema, true, 7_u, 1_s);
  records.emplace_back(schema, true, 2_u, 1_s);
  return records;
}
inline std::vector<Record> MakeProjectOnFilterRecords(const SchemaRef &schema) {
  // Make records.
  std::vector<Record> records;
  records.emplace_back(schema, true, 7_u, 1_s);
  return records;
}
inline std::vector<Record> MakeProjectOnEquiJoinRecords(
    const SchemaRef &schema) {
  std::unique_ptr<std::string> sd1 = std::make_unique<std::string>("descrp0");
  std::unique_ptr<std::string> sd2 = std::make_unique<std::string>("descrp1");
  std::unique_ptr<std::string> sd2_ = std::make_unique<std::string>("descrp1");
  std::unique_ptr<std::string> sd2__ = std::make_unique<std::string>("descrp1");

  // Make records.
  std::vector<Record> records;
  records.emplace_back(schema, true, 5_u, sd1);
  records.emplace_back(schema, true, 0_u, sd2);
  records.emplace_back(schema, true, 7_u, sd2_);
  records.emplace_back(schema, true, 2_u, sd2__);
  return records;
}
inline std::vector<Record> MakeAggregateRecords(const SchemaRef &schema) {
  // Make records.
  std::vector<Record> records;
  records.emplace_back(schema, true, 1L, (uint64_t)3ULL);
  records.emplace_back(schema, true, 3L, (uint64_t)1ULL);
  records.emplace_back(schema, true, 5L, (uint64_t)1ULL);
  return records;
}
inline std::vector<Record> MakeAggregateOnEquiJoinRecords(
    const SchemaRef &schema) {
  // Make records.
  std::vector<Record> records;
  records.emplace_back(schema, true, 1L, (uint64_t)3ULL);
  records.emplace_back(schema, true, 5L, (uint64_t)1ULL);
  return records;
}
inline std::vector<Record> MakeDiamondRecords(const SchemaRef &schema) {
  // Make records.
  std::vector<Record> records;
  records.emplace_back(schema, true, 1L, (uint64_t)3ULL);
  records.emplace_back(schema, true, 5L, (uint64_t)1ULL);
  return records;
}

// Make different types of flows/graphs.
std::shared_ptr<DataFlowGraph> MakeTrivialGraph(ColumnID keycol,
                                                const SchemaRef &schema) {
  std::vector<ColumnID> keys = {keycol};
  auto g = std::make_shared<DataFlowGraph>();

  auto in = std::make_shared<InputOperator>("test-table", schema);
  auto matview = std::make_shared<UnorderedMatViewOperator>(keys);

  EXPECT_TRUE(g->AddInputNode(in));
  EXPECT_TRUE(g->AddOutputOperator(matview, in));
  EXPECT_EQ(g->inputs().size(), 1);
  EXPECT_EQ(g->outputs().size(), 1);
  EXPECT_EQ(g->inputs().at("test-table").get(), in.get());
  EXPECT_EQ(g->outputs().at(0).get(), matview.get());
  return g;
}

std::shared_ptr<DataFlowGraph> MakeFilterGraph(ColumnID keycol,
                                               const SchemaRef &schema) {
  std::vector<ColumnID> keys = {keycol};
  auto g = std::make_shared<DataFlowGraph>();

  auto in = std::make_shared<InputOperator>("test-table", schema);
  auto filter = std::make_shared<FilterOperator>();
  filter->AddOperation(5UL, 0, FilterOperator::Operation::GREATER_THAN);
  auto matview = std::make_shared<KeyOrderedMatViewOperator>(keys);

  EXPECT_TRUE(g->AddInputNode(in));
  EXPECT_TRUE(g->AddNode(filter, in));
  EXPECT_TRUE(g->AddOutputOperator(matview, filter));
  EXPECT_EQ(g->inputs().size(), 1);
  EXPECT_EQ(g->outputs().size(), 1);
  EXPECT_EQ(g->inputs().at("test-table").get(), in.get());
  EXPECT_EQ(g->outputs().at(0).get(), matview.get());
  return g;
}

std::shared_ptr<DataFlowGraph> MakeUnionGraph(ColumnID keycol,
                                              const SchemaRef &schema) {
  std::vector<ColumnID> keys = {keycol};
  auto g = std::make_shared<DataFlowGraph>();

  auto in1 = std::make_shared<InputOperator>("test-table1", schema);
  auto in2 = std::make_shared<InputOperator>("test-table2", schema);
  auto union_ = std::make_shared<UnionOperator>();
  auto matview = std::make_shared<UnorderedMatViewOperator>(keys);

  EXPECT_TRUE(g->AddInputNode(in1));
  EXPECT_TRUE(g->AddInputNode(in2));
  EXPECT_TRUE(g->AddNode(union_, {in1, in2}));
  EXPECT_TRUE(g->AddOutputOperator(matview, union_));
  EXPECT_EQ(g->inputs().size(), 2);
  EXPECT_EQ(g->outputs().size(), 1);
  EXPECT_EQ(g->inputs().at("test-table1").get(), in1.get());
  EXPECT_EQ(g->inputs().at("test-table2").get(), in2.get());
  EXPECT_EQ(g->outputs().at(0).get(), matview.get());
  return g;
}

std::shared_ptr<DataFlowGraph> MakeEquiJoinGraph(ColumnID ok, ColumnID lk,
                                                 ColumnID rk,
                                                 const SchemaRef &lschema,
                                                 const SchemaRef &rschema) {
  std::vector<ColumnID> keys = {ok};
  auto g = std::make_shared<DataFlowGraph>();

  auto in1 = std::make_shared<InputOperator>("test-table1", lschema);
  auto in2 = std::make_shared<InputOperator>("test-table2", rschema);
  auto join = std::make_shared<EquiJoinOperator>(lk, rk);
  auto matview = std::make_shared<UnorderedMatViewOperator>(keys);

  EXPECT_TRUE(g->AddInputNode(in1));
  EXPECT_TRUE(g->AddInputNode(in2));
  EXPECT_TRUE(g->AddNode(join, {in1, in2}));
  EXPECT_TRUE(g->AddOutputOperator(matview, join));
  EXPECT_EQ(g->inputs().size(), 2);
  EXPECT_EQ(g->outputs().size(), 1);
  EXPECT_EQ(g->inputs().at("test-table1").get(), in1.get());
  EXPECT_EQ(g->inputs().at("test-table2").get(), in2.get());
  EXPECT_EQ(g->outputs().at(0).get(), matview.get());
  return g;
}

std::shared_ptr<DataFlowGraph> MakeProjectGraph(ColumnID keycol,
                                                const SchemaRef &schema) {
  std::vector<ColumnID> keys = {keycol};
  auto g = std::make_shared<DataFlowGraph>();

  auto in = std::make_shared<InputOperator>("test-table", schema);
  auto project = std::make_shared<ProjectOperator>();
  project->AddColumnProjection(schema.NameOf(0), 0);
  project->AddColumnProjection(schema.NameOf(2), 2);
  auto matview = std::make_shared<UnorderedMatViewOperator>(keys);

  EXPECT_TRUE(g->AddInputNode(in));
  EXPECT_TRUE(g->AddNode(project, in));
  EXPECT_TRUE(g->AddOutputOperator(matview, project));
  EXPECT_EQ(g->inputs().size(), 1);
  EXPECT_EQ(g->outputs().size(), 1);
  EXPECT_EQ(g->inputs().at("test-table").get(), in.get());
  EXPECT_EQ(g->outputs().at(0).get(), matview.get());
  return g;
}

std::shared_ptr<DataFlowGraph> MakeProjectOnFilterGraph(
    ColumnID keycol, const SchemaRef &schema) {
  std::vector<ColumnID> keys = {keycol};
  auto g = std::make_shared<DataFlowGraph>();

  auto in = std::make_shared<InputOperator>("test-table", schema);
  auto filter = std::make_shared<FilterOperator>();
  filter->AddOperation(5_u, 0, FilterOperator::Operation::GREATER_THAN);
  auto project = std::make_shared<ProjectOperator>();
  project->AddColumnProjection(schema.NameOf(0), 0);
  project->AddColumnProjection(schema.NameOf(2), 2);
  auto matview = std::make_shared<UnorderedMatViewOperator>(keys);

  EXPECT_TRUE(g->AddInputNode(in));
  EXPECT_TRUE(g->AddNode(filter, in));
  EXPECT_TRUE(g->AddNode(project, filter));
  EXPECT_TRUE(g->AddOutputOperator(matview, project));
  EXPECT_EQ(g->inputs().size(), 1);
  EXPECT_EQ(g->outputs().size(), 1);
  EXPECT_EQ(g->inputs().at("test-table").get(), in.get());
  EXPECT_EQ(g->outputs().at(0).get(), matview.get());
  return g;
}

std::shared_ptr<DataFlowGraph> MakeProjectOnEquiJoinGraph(
    ColumnID ok, ColumnID lk, ColumnID rk, const SchemaRef &lschema,
    const SchemaRef &rschema) {
  std::vector<ColumnID> keys = {ok};
  auto g = std::make_shared<DataFlowGraph>();

  auto in1 = std::make_shared<InputOperator>("test-table1", lschema);
  auto in2 = std::make_shared<InputOperator>("test-table2", rschema);
  auto join = std::make_shared<EquiJoinOperator>(lk, rk);
  auto project = std::make_shared<ProjectOperator>();
  project->AddColumnProjection(lschema.NameOf(0), 0);
  project->AddColumnProjection(rschema.NameOf(0), 3);
  auto matview = std::make_shared<UnorderedMatViewOperator>(keys);

  EXPECT_TRUE(g->AddInputNode(in1));
  EXPECT_TRUE(g->AddInputNode(in2));
  EXPECT_TRUE(g->AddNode(join, {in1, in2}));
  EXPECT_TRUE(g->AddNode(project, join));
  EXPECT_TRUE(g->AddOutputOperator(matview, project));
  EXPECT_EQ(g->inputs().size(), 2);
  EXPECT_EQ(g->outputs().size(), 1);
  EXPECT_EQ(g->inputs().at("test-table1").get(), in1.get());
  EXPECT_EQ(g->inputs().at("test-table2").get(), in2.get());
  EXPECT_EQ(g->outputs().at(0).get(), matview.get());
  return g;
}

std::shared_ptr<DataFlowGraph> MakeAggregateGraph(ColumnID keycol,
                                                  const SchemaRef &schema) {
  std::vector<ColumnID> keys = {keycol};
  std::vector<ColumnID> group_columns = {2};
  auto g = std::make_shared<DataFlowGraph>();

  auto in = std::make_shared<InputOperator>("test-table", schema);
  auto aggregate = std::make_shared<AggregateOperator>(
      group_columns, AggregateOperator::Function::COUNT, -1);
  auto matview = std::make_shared<UnorderedMatViewOperator>(keys);

  EXPECT_TRUE(g->AddInputNode(in));
  EXPECT_TRUE(g->AddNode(aggregate, in));
  EXPECT_TRUE(g->AddOutputOperator(matview, aggregate));
  EXPECT_EQ(g->inputs().size(), 1);
  EXPECT_EQ(g->outputs().size(), 1);
  EXPECT_EQ(g->inputs().at("test-table").get(), in.get());
  EXPECT_EQ(g->outputs().at(0).get(), matview.get());
  return g;
}

std::shared_ptr<DataFlowGraph> MakeAggregateOnEquiJoinGraph(
    ColumnID ok, ColumnID lk, ColumnID rk, const SchemaRef &lschema,
    const SchemaRef &rschema) {
  std::vector<ColumnID> keys = {ok};
  std::vector<ColumnID> group_columns = {2};
  auto g = std::make_shared<DataFlowGraph>();

  auto in1 = std::make_shared<InputOperator>("test-table1", lschema);
  auto in2 = std::make_shared<InputOperator>("test-table2", rschema);
  auto join = std::make_shared<EquiJoinOperator>(lk, rk);
  auto aggregate = std::make_shared<AggregateOperator>(
      group_columns, AggregateOperator::Function::COUNT, -1);
  auto matview = std::make_shared<UnorderedMatViewOperator>(keys);

  EXPECT_TRUE(g->AddInputNode(in1));
  EXPECT_TRUE(g->AddInputNode(in2));
  EXPECT_TRUE(g->AddNode(join, {in1, in2}));
  EXPECT_TRUE(g->AddNode(aggregate, join));
  EXPECT_TRUE(g->AddOutputOperator(matview, aggregate));
  EXPECT_EQ(g->inputs().size(), 2);
  EXPECT_EQ(g->outputs().size(), 1);
  EXPECT_EQ(g->inputs().at("test-table1").get(), in1.get());
  EXPECT_EQ(g->inputs().at("test-table2").get(), in2.get());
  EXPECT_EQ(g->outputs().at(0).get(), matview.get());
  return g;
}

// This graph produces output that is the same as that of
// AggregateOnEquijoinGraph
std::shared_ptr<DataFlowGraph> MakeDiamondGraph(ColumnID ok, ColumnID lk,
                                                ColumnID rk,
                                                const SchemaRef &lschema,
                                                const SchemaRef &rschema) {
  std::vector<ColumnID> keys = {ok};
  std::vector<ColumnID> group_columns = {2};
  auto g = std::make_shared<DataFlowGraph>();

  auto in1 = std::make_shared<InputOperator>("test-table1", lschema);
  auto in2 = std::make_shared<InputOperator>("test-table2", rschema);
  auto join = std::make_shared<EquiJoinOperator>(lk, rk);
  auto aggregate = std::make_shared<AggregateOperator>(
      group_columns, AggregateOperator::Function::COUNT, -1);
  // Project operators of the diamond simply project all the columns and the
  // filter operators (along with the union) perfom a logical OR on the two
  // filter conditions.
  auto project_left = std::make_shared<ProjectOperator>();
  project_left->AddColumnProjection("Category", 0);
  project_left->AddColumnProjection("Count", 1);
  auto project_right = std::make_shared<ProjectOperator>();
  project_right->AddColumnProjection("Category", 0);
  project_right->AddColumnProjection("Count", 1);
  auto filter_left = std::make_shared<FilterOperator>();
  filter_left->AddOperation(2_s, 0, FilterOperator::Operation::LESS_THAN);
  auto filter_right = std::make_shared<FilterOperator>();
  filter_right->AddOperation(4_s, 0, FilterOperator::Operation::GREATER_THAN);
  auto union_ = std::make_shared<UnionOperator>();
  auto matview = std::make_shared<UnorderedMatViewOperator>(keys);

  EXPECT_TRUE(g->AddInputNode(in1));
  EXPECT_TRUE(g->AddInputNode(in2));
  EXPECT_TRUE(g->AddNode(join, {in1, in2}));
  EXPECT_TRUE(g->AddNode(aggregate, join));
  EXPECT_TRUE(g->AddNode(project_left, aggregate));
  EXPECT_TRUE(g->AddNode(project_right, aggregate));
  EXPECT_TRUE(g->AddNode(filter_left, project_left));
  EXPECT_TRUE(g->AddNode(filter_right, project_right));
  EXPECT_TRUE(g->AddNode(union_, {filter_left, filter_right}));
  EXPECT_TRUE(g->AddOutputOperator(matview, union_));
  EXPECT_EQ(g->inputs().size(), 2);
  EXPECT_EQ(g->outputs().size(), 1);
  EXPECT_EQ(g->inputs().at("test-table1").get(), in1.get());
  EXPECT_EQ(g->inputs().at("test-table2").get(), in2.get());
  EXPECT_EQ(g->outputs().at(0).get(), matview.get());
  return g;
}

inline void MatViewContentsEquals(std::shared_ptr<MatViewOperator> matview,
                                  const std::vector<Record> &records) {
  EXPECT_EQ(matview->count(), records.size());
  for (const Record &record : records) {
    EXPECT_EQ(record, *matview->Lookup(record.GetKey()).begin());
  }
}

// This method is used specifically for tests involving aggregate graphs.
// The output records in those specific test cases are not keyed because the
// group by columns are not a subset of the keyed columns of input records.
// Hence the following method compares matview contents indexed on @param index,
// which should be 0 for most of the aggregate tests.
inline void MatViewContentsEqualsIndexed(
    std::shared_ptr<MatViewOperator> matview,
    const std::vector<Record> &records, size_t index) {
  EXPECT_EQ(matview->count(), records.size());
  std::vector<ColumnID> indexed_keys = {static_cast<ColumnID>(index)};
  for (const Record &record : records) {
    EXPECT_EQ(record, *matview->Lookup(record.GetValues(indexed_keys)).begin());
  }
}

// Expects that a matview and vector are equal (as multi-sets).
inline void EXPECT_EQ_MSET(std::shared_ptr<dataflow::MatViewOperator> output,
                           const std::vector<dataflow::Record> &r) {
  std::vector<dataflow::Record> tmp;
  for (const auto &key : output->Keys()) {
    for (const auto &record : output->Lookup(key)) {
      tmp.push_back(record.Copy());
    }
  }

  for (const auto &v : r) {
    auto it = std::find(tmp.begin(), tmp.end(), v);
    // v must be found in tmp.
    EXPECT_NE(it, tmp.end());
    // Erase v from tmp, ensures that if an equal record is encountered in the
    // future, it will match a different record in r (multiset equality).
    tmp.erase(it);
  }

  EXPECT_TRUE(tmp.empty());
}

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_GRAPH_TEST_UTILS_H_
