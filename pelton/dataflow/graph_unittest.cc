#include "pelton/dataflow/graph.h"

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/dataflow/ops/equijoin.h"
#include "pelton/dataflow/ops/filter.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/ops/matview.h"
#include "pelton/dataflow/ops/project.h"
#include "pelton/dataflow/ops/union.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace dataflow {

using CType = sqlast::ColumnDefinition::Type;

// Make schemas.
SchemaOwner MakeLeftSchema() {
  std::vector<std::string> names = {"ID", "Item", "Category"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  return SchemaOwner{names, types, keys};
}

SchemaOwner MakeRightSchema() {
  std::vector<std::string> names = {"Category", "Description"};
  std::vector<CType> types = {CType::INT, CType::TEXT};
  std::vector<ColumnID> keys;
  return SchemaOwner{names, types, keys};
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
  records.emplace_back(schema, true, (uint64_t)0ULL, 1L);
  records.emplace_back(schema, true, (uint64_t)4ULL, 3L);
  records.emplace_back(schema, true, (uint64_t)5ULL, 5L);
  records.emplace_back(schema, true, (uint64_t)7ULL, 1L);
  records.emplace_back(schema, true, (uint64_t)2ULL, 1L);
  return records;
}
inline std::vector<Record> MakeProjectOnFilterRecords(const SchemaRef &schema) {
  // Make records.
  std::vector<Record> records;
  records.emplace_back(schema, true, (uint64_t)7ULL, 1L);
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
  records.emplace_back(schema, true, (uint64_t)5ULL, sd1);
  records.emplace_back(schema, true, (uint64_t)0ULL, sd2);
  records.emplace_back(schema, true, (uint64_t)7ULL, sd2_);
  records.emplace_back(schema, true, (uint64_t)2ULL, sd2__);
  return records;
}

// Make different types of flows/graphs.
DataFlowGraph MakeTrivialGraph(ColumnID keycol, const SchemaRef &schema) {
  std::vector<ColumnID> keys = {keycol};
  DataFlowGraph g;

  auto in = std::make_shared<InputOperator>("test-table", schema);
  auto matview = std::make_shared<UnorderedMatViewOperator>(keys);

  EXPECT_TRUE(g.AddInputNode(in));
  EXPECT_TRUE(g.AddOutputOperator(matview, in));
  EXPECT_EQ(g.inputs().size(), 1);
  EXPECT_EQ(g.outputs().size(), 1);
  EXPECT_EQ(g.inputs().at("test-table").get(), in.get());
  EXPECT_EQ(g.outputs().at(0).get(), matview.get());
  return g;
}

DataFlowGraph MakeFilterGraph(ColumnID keycol, const SchemaRef &schema) {
  std::vector<ColumnID> keys = {keycol};
  DataFlowGraph g;

  auto in = std::make_shared<InputOperator>("test-table", schema);
  auto filter = std::make_shared<FilterOperator>();
  filter->AddOperation(5UL, 0, FilterOperator::Operation::GREATER_THAN);
  auto matview = std::make_shared<UnorderedMatViewOperator>(keys);

  EXPECT_TRUE(g.AddInputNode(in));
  EXPECT_TRUE(g.AddNode(filter, in));
  EXPECT_TRUE(g.AddOutputOperator(matview, filter));
  EXPECT_EQ(g.inputs().size(), 1);
  EXPECT_EQ(g.outputs().size(), 1);
  EXPECT_EQ(g.inputs().at("test-table").get(), in.get());
  EXPECT_EQ(g.outputs().at(0).get(), matview.get());
  return g;
}

DataFlowGraph MakeUnionGraph(ColumnID keycol, const SchemaRef &schema) {
  std::vector<ColumnID> keys = {keycol};
  DataFlowGraph g;

  auto in1 = std::make_shared<InputOperator>("test-table1", schema);
  auto in2 = std::make_shared<InputOperator>("test-table2", schema);
  auto union_ = std::make_shared<UnionOperator>();
  auto matview = std::make_shared<UnorderedMatViewOperator>(keys);

  EXPECT_TRUE(g.AddInputNode(in1));
  EXPECT_TRUE(g.AddInputNode(in2));
  EXPECT_TRUE(g.AddNode(union_, {in1, in2}));
  EXPECT_TRUE(g.AddOutputOperator(matview, union_));
  EXPECT_EQ(g.inputs().size(), 2);
  EXPECT_EQ(g.outputs().size(), 1);
  EXPECT_EQ(g.inputs().at("test-table1").get(), in1.get());
  EXPECT_EQ(g.inputs().at("test-table2").get(), in2.get());
  EXPECT_EQ(g.outputs().at(0).get(), matview.get());
  return g;
}

DataFlowGraph MakeEquiJoinGraph(ColumnID ok, ColumnID lk, ColumnID rk,
                                const SchemaRef &lschema,
                                const SchemaRef &rschema) {
  std::vector<ColumnID> keys = {ok};
  DataFlowGraph g;

  auto in1 = std::make_shared<InputOperator>("test-table1", lschema);
  auto in2 = std::make_shared<InputOperator>("test-table2", rschema);
  auto join = std::make_shared<EquiJoinOperator>(lk, rk);
  auto matview = std::make_shared<UnorderedMatViewOperator>(keys);

  EXPECT_TRUE(g.AddInputNode(in1));
  EXPECT_TRUE(g.AddInputNode(in2));
  EXPECT_TRUE(g.AddNode(join, {in1, in2}));
  EXPECT_TRUE(g.AddOutputOperator(matview, join));
  EXPECT_EQ(g.inputs().size(), 2);
  EXPECT_EQ(g.outputs().size(), 1);
  EXPECT_EQ(g.inputs().at("test-table1").get(), in1.get());
  EXPECT_EQ(g.inputs().at("test-table2").get(), in2.get());
  EXPECT_EQ(g.outputs().at(0).get(), matview.get());
  return g;
}

DataFlowGraph MakeProjectGraph(ColumnID keycol, const SchemaRef &schema) {
  std::vector<ColumnID> keys = {keycol};
  std::vector<ColumnID> column_ids = {0, 2};
  DataFlowGraph g;

  auto in = std::make_shared<InputOperator>("test-table", schema);
  auto project = std::make_shared<ProjectOperator>(column_ids);
  auto matview = std::make_shared<MatViewOperator>(keys);

  EXPECT_TRUE(g.AddInputNode(in));
  EXPECT_TRUE(g.AddNode(project, in));
  EXPECT_TRUE(g.AddOutputOperator(matview, project));
  EXPECT_EQ(g.inputs().size(), 1);
  EXPECT_EQ(g.outputs().size(), 1);
  EXPECT_EQ(g.inputs().at("test-table").get(), in.get());
  EXPECT_EQ(g.outputs().at(0).get(), matview.get());
  return g;
}

DataFlowGraph MakeProjectOnFilterGraph(ColumnID keycol,
                                       const SchemaRef &schema) {
  std::vector<ColumnID> keys = {keycol};
  std::vector<ColumnID> column_ids = {0, 2};
  DataFlowGraph g;

  auto in = std::make_shared<InputOperator>("test-table", schema);
  auto filter = std::make_shared<FilterOperator>();
  filter->AddOperation(5UL, 0, FilterOperator::Operation::GREATER_THAN);
  auto project = std::make_shared<ProjectOperator>(column_ids);
  auto matview = std::make_shared<MatViewOperator>(keys);

  EXPECT_TRUE(g.AddInputNode(in));
  EXPECT_TRUE(g.AddNode(filter, in));
  EXPECT_TRUE(g.AddNode(project, filter));
  EXPECT_TRUE(g.AddOutputOperator(matview, project));
  EXPECT_EQ(g.inputs().size(), 1);
  EXPECT_EQ(g.outputs().size(), 1);
  EXPECT_EQ(g.inputs().at("test-table").get(), in.get());
  EXPECT_EQ(g.outputs().at(0).get(), matview.get());
  return g;
}

DataFlowGraph MakeProjectOnEquiJoinGraph(ColumnID ok, ColumnID lk, ColumnID rk,
                                         const SchemaRef &lschema,
                                         const SchemaRef &rschema) {
  std::vector<ColumnID> keys = {ok};
  std::vector<ColumnID> column_ids = {0, 3};
  DataFlowGraph g;

  auto in1 = std::make_shared<InputOperator>("test-table1", lschema);
  auto in2 = std::make_shared<InputOperator>("test-table2", rschema);
  auto join = std::make_shared<EquiJoinOperator>(lk, rk);
  auto project = std::make_shared<ProjectOperator>(column_ids);
  auto matview = std::make_shared<MatViewOperator>(keys);

  EXPECT_TRUE(g.AddInputNode(in1));
  EXPECT_TRUE(g.AddInputNode(in2));
  EXPECT_TRUE(g.AddNode(join, {in1, in2}));
  EXPECT_TRUE(g.AddNode(project, join));
  EXPECT_TRUE(g.AddOutputOperator(matview, project));
  EXPECT_EQ(g.inputs().size(), 2);
  EXPECT_EQ(g.outputs().size(), 1);
  EXPECT_EQ(g.inputs().at("test-table1").get(), in1.get());
  EXPECT_EQ(g.inputs().at("test-table2").get(), in2.get());
  EXPECT_EQ(g.outputs().at(0).get(), matview.get());
  return g;
}

inline void MatViewContentsEquals(std::shared_ptr<MatViewOperator> matview,
                                  const std::vector<Record> &records) {
  EXPECT_EQ(matview->count(), records.size());
  for (const Record &record : records) {
    EXPECT_EQ(record, *matview->Lookup(record.GetKey()).begin());
  }
}

// Tests!
TEST(DataFlowGraphTest, TestTrivialGraph) {
  // Schema must survive records.
  SchemaOwner schema = MakeLeftSchema();
  // Make graph.
  DataFlowGraph g = MakeTrivialGraph(0, SchemaRef(schema));
  // Make records.
  std::vector<Record> records = MakeLeftRecords(SchemaRef(schema));
  // Process records.
  EXPECT_TRUE(g.Process("test-table", records));
  // Outputs must be equal.
  MatViewContentsEquals(g.outputs().at(0), records);
}

TEST(DataFlowGraphTest, TestFilterGraph) {
  // Schema must survive records.
  SchemaOwner schema = MakeLeftSchema();
  // Make graph.
  DataFlowGraph g = MakeFilterGraph(0, SchemaRef(schema));
  // Make records.
  std::vector<Record> records = MakeLeftRecords(SchemaRef(schema));
  // Process records.
  EXPECT_TRUE(g.Process("test-table", records));
  // Filter records.
  auto op = std::dynamic_pointer_cast<FilterOperator>(g.GetNode(1));
  std::vector<Record> filtered = MakeFilterRecords(op->output_schema());
  // Outputs must be equal.
  MatViewContentsEquals(g.outputs().at(0), filtered);
}

TEST(DataFlowGraphTest, TestUnionGraph) {
  // Schema must survive records.
  SchemaOwner schema = MakeLeftSchema();
  // Make graph.
  DataFlowGraph g = MakeUnionGraph(0, SchemaRef(schema));
  // Make records.
  std::vector<Record> records = MakeLeftRecords(SchemaRef(schema));
  std::vector<Record> first_half;
  first_half.push_back(records.at(0).Copy());
  first_half.push_back(records.at(1).Copy());
  std::vector<Record> second_half;
  second_half.push_back(records.at(2).Copy());
  second_half.push_back(records.at(3).Copy());
  second_half.push_back(records.at(4).Copy());
  // Process records.
  EXPECT_TRUE(g.Process("test-table1", first_half));
  EXPECT_TRUE(g.Process("test-table2", second_half));
  // Outputs must be equal.
  MatViewContentsEquals(g.outputs().at(0), records);
}

TEST(DataFlowGraphTest, TestEquiJoinGraph) {
  // Schema must survive records.
  SchemaOwner lschema = MakeLeftSchema();
  SchemaOwner rschema = MakeRightSchema();
  // Make graph.
  DataFlowGraph g =
      MakeEquiJoinGraph(0, 2, 0, SchemaRef(lschema), SchemaRef(rschema));
  // Make records.
  std::vector<Record> left = MakeLeftRecords(SchemaRef(lschema));
  std::vector<Record> right = MakeRightRecords(SchemaRef(rschema));
  // Process records.
  EXPECT_TRUE(g.Process("test-table1", left));
  EXPECT_TRUE(g.Process("test-table2", right));
  // Compute expected result.
  auto op = std::dynamic_pointer_cast<EquiJoinOperator>(g.GetNode(2));
  std::vector<Record> result = MakeJoinRecords(op->output_schema());
  // Outputs must be equal.
  MatViewContentsEquals(g.outputs().at(0), result);
}

TEST(DataFlowGraphTest, TestProjectGraph) {
  // Schema must survive records.
  SchemaOwner schema = MakeLeftSchema();
  // Make graph.
  DataFlowGraph g = MakeProjectGraph(0, SchemaRef(schema));
  // Make records.
  std::vector<Record> records = MakeLeftRecords(SchemaRef(schema));
  // Process records.
  EXPECT_TRUE(g.Process("test-table", records));
  // Compute expected result.
  auto op = std::dynamic_pointer_cast<ProjectOperator>(g.GetNode(1));
  std::vector<Record> result = MakeProjectRecords(op->output_schema());
  // Outputs must be equal.
  MatViewContentsEquals(g.outputs().at(0), result);
}

TEST(DataFlowGraphTest, TestProjectOnFilterGraph) {
  // Schema must survive records.
  SchemaOwner schema = MakeLeftSchema();
  // Make graph.
  DataFlowGraph g = MakeProjectOnFilterGraph(0, SchemaRef(schema));
  // Make records.
  std::vector<Record> records = MakeLeftRecords(SchemaRef(schema));
  // Process records.
  EXPECT_TRUE(g.Process("test-table", records));
  // Compute expected result.
  auto op = std::dynamic_pointer_cast<ProjectOperator>(g.GetNode(2));
  std::vector<Record> result = MakeProjectOnFilterRecords(op->output_schema());
  // Outputs must be equal.
  MatViewContentsEquals(g.outputs().at(0), result);
}

TEST(DataFlowGraphTest, TestProjectOnEquiJoinGraph) {
  // Schema must survive records.
  SchemaOwner lschema = MakeLeftSchema();
  SchemaOwner rschema = MakeRightSchema();
  // Make graph.
  DataFlowGraph g = MakeProjectOnEquiJoinGraph(0, 2, 0, SchemaRef(lschema),
                                               SchemaRef(rschema));
  // Make records.
  std::vector<Record> left = MakeLeftRecords(SchemaRef(lschema));
  std::vector<Record> right = MakeRightRecords(SchemaRef(rschema));
  // Process records.
  EXPECT_TRUE(g.Process("test-table1", left));
  EXPECT_TRUE(g.Process("test-table2", right));
  // Compute expected result.
  auto op = std::dynamic_pointer_cast<ProjectOperator>(g.GetNode(3));
  std::vector<Record> result =
      MakeProjectOnEquiJoinRecords(op->output_schema());
  // Outputs must be equal.
  MatViewContentsEquals(g.outputs().at(0), result);
}
#ifndef PELTON_VALGRIND_MODE
TEST(RecordTest, TestDuplicateInputGraph) {
  // Create a schema.
  SchemaOwner schema = MakeLeftSchema();

  // Make a graph.
  DataFlowGraph g;

  // Add two input operators to the graph for the same table, expect an error.
  auto in1 = std::make_shared<InputOperator>("test-table", SchemaRef(schema));
  auto in2 = std::make_shared<InputOperator>("test-table", SchemaRef(schema));
  EXPECT_TRUE(g.AddInputNode(in1));
  ASSERT_DEATH({ g.AddInputNode(in2); }, "input already exists");
}
#endif

}  // namespace dataflow
}  // namespace pelton
