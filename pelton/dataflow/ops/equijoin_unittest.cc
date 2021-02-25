#include "pelton/dataflow/ops/equijoin.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/dataflow/edge.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/ops/identity.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

using CType = sqlast::ColumnDefinition::Type;

inline SchemaOwner Schema1() {
  std::vector<std::string> names = {"ID", "Item", "Category"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  return SchemaOwner{names, types, keys};
}

inline SchemaOwner Schema2() {
  std::vector<std::string> names = {"Count", "Category", "Description"};
  std::vector<CType> types = {CType::UINT, CType::INT, CType::TEXT};
  std::vector<ColumnID> keys = {1};
  return SchemaOwner{names, types, keys};
}

inline SchemaOwner Schema3() {
  std::vector<std::string> names = {"ID2", "Category", "Text"};
  std::vector<CType> types = {CType::UINT, CType::INT, CType::TEXT};
  std::vector<ColumnID> keys = {0};
  return SchemaOwner{names, types, keys};
}

inline SchemaOwner Schema4() {
  std::vector<std::string> names = {"Category", "Text"};
  std::vector<CType> types = {CType::INT, CType::TEXT};
  std::vector<ColumnID> keys = {0};
  return SchemaOwner{names, types, keys};
}

TEST(EquiJoinOperatorTest, JoinedSchemaTest) {
  EquiJoinOperator op1{2, 1};
  EquiJoinOperator op2{1, 1};
  EquiJoinOperator op3{1, 2};
  EquiJoinOperator op4{0, 2};
  SchemaOwner schema1 = Schema1();
  SchemaOwner schema2 = Schema2();
  SchemaOwner schema3 = Schema3();
  SchemaOwner schema4 = Schema4();

  // Compute the joined schema.
  op1.input_schemas_.push_back(SchemaRef(schema1));
  op1.input_schemas_.push_back(SchemaRef(schema2));
  op1.ComputeOutputSchema();
  op2.input_schemas_.push_back(SchemaRef(schema2));
  op2.input_schemas_.push_back(SchemaRef(schema3));
  op2.ComputeOutputSchema();
  op3.input_schemas_.push_back(SchemaRef(schema3));
  op3.input_schemas_.push_back(SchemaRef(schema1));
  op3.ComputeOutputSchema();
  op4.input_schemas_.push_back(SchemaRef(schema4));
  op4.input_schemas_.push_back(SchemaRef(schema1));
  op4.ComputeOutputSchema();

  const SchemaRef &joined1 = op1.output_schema();
  const SchemaRef &joined2 = op2.output_schema();
  const SchemaRef &joined3 = op3.output_schema();
  const SchemaRef &joined4 = op4.output_schema();

  std::vector<std::string> joined_names1 = {"ID", "Item", "Category", "Count",
                                            "Description"};
  std::vector<CType> joined_types1 = {CType::UINT, CType::TEXT, CType::INT,
                                      CType::UINT, CType::TEXT};
  std::vector<ColumnID> joined_keys1 = {0, 2};

  std::vector<std::string> joined_names2 = {"Count", "Category", "Description",
                                            "ID2", "Text"};
  std::vector<CType> joined_types2 = {CType::UINT, CType::INT, CType::TEXT,
                                      CType::UINT, CType::TEXT};
  std::vector<ColumnID> joined_keys2 = {1, 3};

  std::vector<std::string> joined_names3 = {"ID2", "Category", "Text", "ID",
                                            "Item"};
  std::vector<CType> joined_types3 = {CType::UINT, CType::INT, CType::TEXT,
                                      CType::UINT, CType::TEXT};
  std::vector<ColumnID> joined_keys3 = {0, 3};

  std::vector<std::string> joined_names4 = {"Category", "Text", "ID", "Item"};
  std::vector<CType> joined_types4 = {CType::INT, CType::TEXT, CType::UINT,
                                      CType::TEXT};
  std::vector<ColumnID> joined_keys4 = {0, 2};

  // Test that the joined schema is what we expect.
  EXPECT_EQ(joined1.column_names(), joined_names1);
  EXPECT_EQ(joined1.column_types(), joined_types1);
  EXPECT_EQ(joined1.keys(), joined_keys1);
  EXPECT_EQ(joined2.column_names(), joined_names2);
  EXPECT_EQ(joined2.column_types(), joined_types2);
  EXPECT_EQ(joined2.keys(), joined_keys2);
  EXPECT_EQ(joined3.column_names(), joined_names3);
  EXPECT_EQ(joined3.column_types(), joined_types3);
  EXPECT_EQ(joined3.keys(), joined_keys3);
  EXPECT_EQ(joined4.column_names(), joined_names4);
  EXPECT_EQ(joined4.column_types(), joined_types4);
  EXPECT_EQ(joined4.keys(), joined_keys4);
}

TEST(EquiJoinOperatorTest, BasicJoinTest) {
  // Make two joinable records.
  std::unique_ptr<std::string> s1 = std::make_unique<std::string>("item0");
  std::unique_ptr<std::string> s2 = std::make_unique<std::string>("descrp0");
  SchemaOwner lschema = Schema1();
  SchemaOwner rschema = Schema2();
  std::vector<Record> lrecords;
  std::vector<Record> rrecords;
  lrecords.emplace_back(SchemaRef(lschema));
  rrecords.emplace_back(SchemaRef(rschema));
  lrecords.at(0).SetUInt(0UL, 0);
  lrecords.at(0).SetString(std::move(s1), 1);
  lrecords.at(0).SetInt(-5L, 2);
  rrecords.at(0).SetUInt(100UL, 0);
  rrecords.at(0).SetInt(-5LL, 1);
  rrecords.at(0).SetString(std::move(s2), 2);

  // Setup join operator with two parents, left with id 0 and right with id 1.
  std::shared_ptr<IdentityOperator> iop1 = std::make_shared<IdentityOperator>();
  std::shared_ptr<IdentityOperator> iop2 = std::make_shared<IdentityOperator>();
  std::shared_ptr<EquiJoinOperator> op =
      std::make_shared<EquiJoinOperator>(2, 1);
  iop1->SetIndex(0);
  iop2->SetIndex(1);
  op->parents_ = {std::make_shared<Edge>(iop1, op),
                  std::make_shared<Edge>(iop2, op)};
  op->input_schemas_ = {SchemaRef(lschema), SchemaRef(rschema)};
  op->ComputeOutputSchema();

  // Process records.
  std::vector<Record> output;
  EXPECT_TRUE(op->Process(0, lrecords, &output));
  EXPECT_EQ(output.size(), 0);
  EXPECT_EQ(op->left_table_.count(), 1);
  EXPECT_EQ(op->left_table_.Get(Key(-5L)), lrecords);
  EXPECT_EQ(op->right_table_.count(), 0);
  EXPECT_TRUE(op->Process(1, rrecords, &output));
  EXPECT_EQ(op->left_table_.count(), 1);
  EXPECT_EQ(op->left_table_.Get(Key(-5L)), lrecords);
  EXPECT_EQ(op->right_table_.count(), 1);
  EXPECT_EQ(op->right_table_.Get(Key(-5L)), rrecords);
  EXPECT_EQ(output.size(), 1);
  EXPECT_EQ(output.at(0).GetUInt(0), 0UL);
  EXPECT_EQ(output.at(0).GetString(1), "item0");
  EXPECT_EQ(output.at(0).GetInt(2), -5L);
  EXPECT_EQ(output.at(0).GetUInt(3), 100UL);
  EXPECT_EQ(output.at(0).GetString(4), "descrp0");
}

TEST(EquiJoinOperatorTest, BasicUnjoinableTest) {
  // Make two unjoinable records.
  std::unique_ptr<std::string> s1 = std::make_unique<std::string>("item0");
  std::unique_ptr<std::string> s2 = std::make_unique<std::string>("descrp0");
  SchemaOwner lschema = Schema1();
  SchemaOwner rschema = Schema2();
  std::vector<Record> lrecords;
  std::vector<Record> rrecords;
  lrecords.emplace_back(SchemaRef(lschema));
  rrecords.emplace_back(SchemaRef(rschema));
  lrecords.at(0).SetUInt(0UL, 0);
  lrecords.at(0).SetString(std::move(s1), 1);
  lrecords.at(0).SetInt(-5L, 2);
  rrecords.at(0).SetUInt(100UL, 0);
  rrecords.at(0).SetInt(50L, 1);
  rrecords.at(0).SetString(std::move(s2), 2);

  // Setup join operator with two parents, left with id 0 and right with id 1.
  std::shared_ptr<IdentityOperator> iop1 = std::make_shared<IdentityOperator>();
  std::shared_ptr<IdentityOperator> iop2 = std::make_shared<IdentityOperator>();
  std::shared_ptr<EquiJoinOperator> op =
      std::make_shared<EquiJoinOperator>(2, 1);
  iop1->SetIndex(0);
  iop2->SetIndex(1);
  op->parents_ = {std::make_shared<Edge>(iop1, op),
                  std::make_shared<Edge>(iop2, op)};
  op->input_schemas_ = {SchemaRef(lschema), SchemaRef(rschema)};
  op->ComputeOutputSchema();

  // Process records.
  std::vector<Record> output;
  EXPECT_TRUE(op->Process(0, lrecords, &output));
  EXPECT_EQ(output.size(), 0);
  EXPECT_EQ(op->left_table_.count(), 1);
  EXPECT_EQ(op->left_table_.Get(Key(-5L)), lrecords);
  EXPECT_EQ(op->right_table_.count(), 0);
  EXPECT_TRUE(op->Process(1, rrecords, &output));
  EXPECT_EQ(op->left_table_.count(), 1);
  EXPECT_EQ(op->left_table_.Get(Key(-5L)), lrecords);
  EXPECT_EQ(op->right_table_.count(), 1);
  EXPECT_EQ(op->right_table_.Get(Key(50L)), rrecords);
  EXPECT_EQ(output.size(), 0);
}

TEST(EquiJoinOperatorTest, FullJoinTest) {
  // Make several sets of joinable records.
  std::unique_ptr<std::string> si1 = std::make_unique<std::string>("item0");
  std::unique_ptr<std::string> si2 = std::make_unique<std::string>("item1");
  std::unique_ptr<std::string> si3 = std::make_unique<std::string>("item2");
  std::unique_ptr<std::string> si4 = std::make_unique<std::string>("item3");
  std::unique_ptr<std::string> si5 = std::make_unique<std::string>("item4");
  std::unique_ptr<std::string> sd1 = std::make_unique<std::string>("descrp0");
  std::unique_ptr<std::string> sd2 = std::make_unique<std::string>("descrp1");
  std::unique_ptr<std::string> sd3 = std::make_unique<std::string>("descrp2");
  std::unique_ptr<std::string> ji1 = std::make_unique<std::string>("item0");
  std::unique_ptr<std::string> ji2 = std::make_unique<std::string>("item1");
  std::unique_ptr<std::string> ji4 = std::make_unique<std::string>("item3");
  std::unique_ptr<std::string> ji5 = std::make_unique<std::string>("item4");
  std::unique_ptr<std::string> jd1 = std::make_unique<std::string>("descrp0");
  std::unique_ptr<std::string> jd1_ = std::make_unique<std::string>("descrp0");
  std::unique_ptr<std::string> jd3 = std::make_unique<std::string>("descrp2");
  std::unique_ptr<std::string> jd3_ = std::make_unique<std::string>("descrp2");

  SchemaOwner lschema = Schema1();
  SchemaOwner rschema = Schema2();
  std::vector<Record> lrecords1;
  std::vector<Record> lrecords2;
  std::vector<Record> lrecords3;
  std::vector<Record> rrecords1;
  std::vector<Record> rrecords2;
  lrecords1.emplace_back(SchemaRef(lschema));
  lrecords1.emplace_back(SchemaRef(lschema));
  lrecords2.emplace_back(SchemaRef(lschema));
  lrecords2.emplace_back(SchemaRef(lschema));
  lrecords3.emplace_back(SchemaRef(lschema));
  rrecords1.emplace_back(SchemaRef(rschema));
  rrecords1.emplace_back(SchemaRef(rschema));
  rrecords2.emplace_back(SchemaRef(rschema));
  // Left 1.
  lrecords1.at(0).SetUInt(0UL, 0);
  lrecords1.at(0).SetString(std::move(si1), 1);
  lrecords1.at(0).SetInt(1L, 2);
  // Left 2.
  lrecords1.at(1).SetUInt(1UL, 0);
  lrecords1.at(1).SetString(std::move(si2), 1);
  lrecords1.at(1).SetInt(2L, 2);
  // Left 3.
  lrecords2.at(0).SetUInt(2UL, 0);
  lrecords2.at(0).SetString(std::move(si3), 1);
  lrecords2.at(0).SetInt(0L, 2);
  // Left 4.
  lrecords2.at(1).SetUInt(3UL, 0);
  lrecords2.at(1).SetString(std::move(si4), 1);
  lrecords2.at(1).SetInt(2L, 2);
  // Left 5.
  lrecords3.at(0).SetUInt(4UL, 0);
  lrecords3.at(0).SetString(std::move(si5), 1);
  lrecords3.at(0).SetInt(1L, 2);
  // Right 1, joins with left 1 and 5.
  rrecords1.at(0).SetUInt(100UL, 0);
  rrecords1.at(0).SetInt(1L, 1);
  rrecords1.at(0).SetString(std::move(sd1), 2);
  // Right 2, does not join.
  rrecords1.at(1).SetUInt(100UL, 0);
  rrecords1.at(1).SetInt(-1L, 1);
  rrecords1.at(1).SetString(std::move(sd2), 2);
  // Right 3, joins with left 2 and 4.
  rrecords2.at(0).SetUInt(50UL, 0);
  rrecords2.at(0).SetInt(2L, 1);
  rrecords2.at(0).SetString(std::move(sd3), 2);

  // Setup join operator with two parents, left with id 0 and right with id 1.
  std::shared_ptr<IdentityOperator> iop1 = std::make_shared<IdentityOperator>();
  std::shared_ptr<IdentityOperator> iop2 = std::make_shared<IdentityOperator>();
  std::shared_ptr<EquiJoinOperator> op =
      std::make_shared<EquiJoinOperator>(2, 1);
  iop1->SetIndex(0);
  iop2->SetIndex(1);
  op->parents_ = {std::make_shared<Edge>(iop1, op),
                  std::make_shared<Edge>(iop2, op)};
  op->input_schemas_ = {SchemaRef(lschema), SchemaRef(rschema)};
  op->ComputeOutputSchema();

  // Process records.
  std::vector<Record> output;
  // Batch 1.
  EXPECT_TRUE(op->Process(0, lrecords1, &output));
  EXPECT_EQ(op->left_table_.count(), 2);
  EXPECT_EQ(op->right_table_.count(), 0);
  EXPECT_EQ(output.size(), 0);
  // Batch 2.
  EXPECT_TRUE(op->Process(1, rrecords1, &output));
  EXPECT_EQ(op->left_table_.count(), 2);
  EXPECT_EQ(op->right_table_.count(), 2);
  EXPECT_EQ(output.size(), 1);
  // Batch 3.
  EXPECT_TRUE(op->Process(0, lrecords2, &output));
  EXPECT_EQ(op->left_table_.count(), 4);
  EXPECT_EQ(op->right_table_.count(), 2);
  EXPECT_EQ(output.size(), 1);
  // Batch 4.
  EXPECT_TRUE(op->Process(0, lrecords3, &output));
  EXPECT_EQ(op->left_table_.count(), 5);
  EXPECT_EQ(op->right_table_.count(), 2);
  EXPECT_EQ(output.size(), 2);
  // Batch 5.
  EXPECT_TRUE(op->Process(1, rrecords2, &output));
  EXPECT_EQ(op->left_table_.count(), 5);
  EXPECT_EQ(op->right_table_.count(), 3);
  EXPECT_EQ(output.size(), 4);

  // Create the joined records.
  std::vector<Record> jrecords;
  jrecords.emplace_back(op->output_schema());
  jrecords.emplace_back(op->output_schema());
  jrecords.emplace_back(op->output_schema());
  jrecords.emplace_back(op->output_schema());
  // 1 <-> 1.
  jrecords.at(0).SetUInt(0UL, 0);
  jrecords.at(0).SetString(std::move(ji1), 1);
  jrecords.at(0).SetInt(1L, 2);
  jrecords.at(0).SetUInt(100UL, 3);
  jrecords.at(0).SetString(std::move(jd1), 4);
  // 5 <-> 1.
  jrecords.at(1).SetUInt(4UL, 0);
  jrecords.at(1).SetString(std::move(ji5), 1);
  jrecords.at(1).SetInt(1L, 2);
  jrecords.at(1).SetUInt(100UL, 3);
  jrecords.at(1).SetString(std::move(jd1_), 4);
  // 2 <-> 3.
  jrecords.at(2).SetUInt(1UL, 0);
  jrecords.at(2).SetString(std::move(ji2), 1);
  jrecords.at(2).SetInt(2L, 2);
  jrecords.at(2).SetUInt(50UL, 3);
  jrecords.at(2).SetString(std::move(jd3), 4);
  // 4 <-> 3.
  jrecords.at(3).SetUInt(3UL, 0);
  jrecords.at(3).SetString(std::move(ji4), 1);
  jrecords.at(3).SetInt(2L, 2);
  jrecords.at(3).SetUInt(50UL, 3);
  jrecords.at(3).SetString(std::move(jd3_), 4);

  // Test the output records are identical.
  EXPECT_EQ(output, jrecords);
}

}  // namespace dataflow
}  // namespace pelton
