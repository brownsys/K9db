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
#include "pelton/util/ints.h"

namespace pelton {
namespace dataflow {

using CType = sqlast::ColumnDefinition::Type;

inline SchemaRef Schema1() {
  std::vector<std::string> names = {"ID", "Item", "Category"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  return SchemaFactory::Create(names, types, keys);
}

inline SchemaRef Schema2() {
  std::vector<std::string> names = {"Count", "Category", "Description"};
  std::vector<CType> types = {CType::UINT, CType::INT, CType::TEXT};
  std::vector<ColumnID> keys = {1};
  return SchemaFactory::Create(names, types, keys);
}

inline SchemaRef Schema3() {
  std::vector<std::string> names = {"ID2", "Category", "Text"};
  std::vector<CType> types = {CType::UINT, CType::INT, CType::TEXT};
  std::vector<ColumnID> keys = {0};
  return SchemaFactory::Create(names, types, keys);
}

inline SchemaRef Schema4() {
  std::vector<std::string> names = {"Category", "Text"};
  std::vector<CType> types = {CType::INT, CType::TEXT};
  std::vector<ColumnID> keys = {0};
  return SchemaFactory::Create(names, types, keys);
}

inline void EXPECT_IT_EQ(RecordIterable &&l, const std::vector<Record> &r) {
  size_t i = 0;
  for (const Record &record : l) {
    EXPECT_EQ(record, r.at(i++));
  }
  EXPECT_EQ(i, r.size());
}

TEST(EquiJoinOperatorTest, JoinedSchemaTest) {
  EquiJoinOperator op1{2, 1};
  EquiJoinOperator op2{1, 1};
  EquiJoinOperator op3{1, 2};
  EquiJoinOperator op4{0, 2};
  SchemaRef schema1 = Schema1();
  SchemaRef schema2 = Schema2();
  SchemaRef schema3 = Schema3();
  SchemaRef schema4 = Schema4();

  // Compute the joined schema.
  op1.input_schemas_.push_back(schema1);
  op1.input_schemas_.push_back(schema2);
  op1.ComputeOutputSchema();
  op2.input_schemas_.push_back(schema2);
  op2.input_schemas_.push_back(schema3);
  op2.ComputeOutputSchema();
  op3.input_schemas_.push_back(schema3);
  op3.input_schemas_.push_back(schema1);
  op3.ComputeOutputSchema();
  op4.input_schemas_.push_back(schema4);
  op4.input_schemas_.push_back(schema1);
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
  SchemaRef lschema = Schema1();
  SchemaRef rschema = Schema2();
  std::vector<Record> lrecords;
  std::vector<Record> rrecords;
  lrecords.emplace_back(lschema);
  rrecords.emplace_back(rschema);
  lrecords.at(0).SetUInt(0, 0);
  lrecords.at(0).SetString(std::move(s1), 1);
  lrecords.at(0).SetInt(-5, 2);
  rrecords.at(0).SetUInt(100, 0);
  rrecords.at(0).SetInt(-5, 1);
  rrecords.at(0).SetString(std::move(s2), 2);

  // Setup join operator with two parents, left with id 0 and right with id 1.
  std::shared_ptr<IdentityOperator> iop1 = std::make_shared<IdentityOperator>();
  std::shared_ptr<IdentityOperator> iop2 = std::make_shared<IdentityOperator>();
  std::shared_ptr<EquiJoinOperator> op =
      std::make_shared<EquiJoinOperator>(2, 1);
  iop1->SetIndex(0);
  iop2->SetIndex(1);
  op->parents_ = {0,1};
  op->input_schemas_ = {lschema, rschema};
  op->ComputeOutputSchema();

  // Process records.
  std::vector<Record> output;
  EXPECT_TRUE(op->Process(0, lrecords, &output));
  EXPECT_EQ(output.size(), 0);
  EXPECT_EQ(op->left_table_.count(), 1);
  EXPECT_IT_EQ(op->left_table_.Lookup(lrecords.at(0).GetValues({2})), lrecords);
  EXPECT_EQ(op->right_table_.count(), 0);
  EXPECT_TRUE(op->Process(1, rrecords, &output));
  EXPECT_EQ(op->left_table_.count(), 1);
  EXPECT_IT_EQ(op->left_table_.Lookup(lrecords.at(0).GetValues({2})), lrecords);
  EXPECT_EQ(op->right_table_.count(), 1);
  EXPECT_IT_EQ(op->right_table_.Lookup(rrecords.at(0).GetValues({1})),
               rrecords);
  EXPECT_EQ(output.size(), 1);
  EXPECT_EQ(output.at(0).GetUInt(0), 0);
  EXPECT_EQ(output.at(0).GetString(1), "item0");
  EXPECT_EQ(output.at(0).GetInt(2), -5);
  EXPECT_EQ(output.at(0).GetUInt(3), 100);
  EXPECT_EQ(output.at(0).GetString(4), "descrp0");
}

TEST(EquiJoinOperatorTest, BasicUnjoinableTest) {
  // Make two unjoinable records.
  std::unique_ptr<std::string> s1 = std::make_unique<std::string>("item0");
  std::unique_ptr<std::string> s2 = std::make_unique<std::string>("descrp0");
  SchemaRef lschema = Schema1();
  SchemaRef rschema = Schema2();
  std::vector<Record> lrecords;
  std::vector<Record> rrecords;
  lrecords.emplace_back(lschema);
  rrecords.emplace_back(rschema);
  lrecords.at(0).SetUInt(0, 0);
  lrecords.at(0).SetString(std::move(s1), 1);
  lrecords.at(0).SetInt(-5, 2);
  rrecords.at(0).SetUInt(100, 0);
  rrecords.at(0).SetInt(50, 1);
  rrecords.at(0).SetString(std::move(s2), 2);

  // Setup join operator with two parents, left with id 0 and right with id 1.
  std::shared_ptr<IdentityOperator> iop1 = std::make_shared<IdentityOperator>();
  std::shared_ptr<IdentityOperator> iop2 = std::make_shared<IdentityOperator>();
  std::shared_ptr<EquiJoinOperator> op =
      std::make_shared<EquiJoinOperator>(2, 1);
  iop1->SetIndex(0);
  iop2->SetIndex(1);
  op->parents_ = {0,1};
  op->input_schemas_ = {lschema, rschema};
  op->ComputeOutputSchema();

  // Process records.
  std::vector<Record> output;
  EXPECT_TRUE(op->Process(0, lrecords, &output));
  EXPECT_EQ(output.size(), 0);
  EXPECT_EQ(op->left_table_.count(), 1);
  EXPECT_IT_EQ(op->left_table_.Lookup(lrecords.at(0).GetValues({2})), lrecords);
  EXPECT_EQ(op->right_table_.count(), 0);
  EXPECT_TRUE(op->Process(1, rrecords, &output));
  EXPECT_EQ(op->left_table_.count(), 1);
  EXPECT_IT_EQ(op->left_table_.Lookup(lrecords.at(0).GetValues({2})), lrecords);
  EXPECT_EQ(op->right_table_.count(), 1);
  EXPECT_IT_EQ(op->right_table_.Lookup(rrecords.at(0).GetValues({1})),
               rrecords);
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

  SchemaRef lschema = Schema1();
  SchemaRef rschema = Schema2();
  std::vector<Record> lrecords1;
  std::vector<Record> lrecords2;
  std::vector<Record> lrecords3;
  std::vector<Record> rrecords1;
  std::vector<Record> rrecords2;
  lrecords1.emplace_back(lschema);
  lrecords1.emplace_back(lschema);
  lrecords2.emplace_back(lschema);
  lrecords2.emplace_back(lschema);
  lrecords3.emplace_back(lschema);
  rrecords1.emplace_back(rschema);
  rrecords1.emplace_back(rschema);
  rrecords2.emplace_back(rschema);
  // Left 1.
  lrecords1.at(0).SetUInt(0, 0);
  lrecords1.at(0).SetString(std::move(si1), 1);
  lrecords1.at(0).SetInt(1, 2);
  // Left 2.
  lrecords1.at(1).SetUInt(1, 0);
  lrecords1.at(1).SetString(std::move(si2), 1);
  lrecords1.at(1).SetInt(2, 2);
  // Left 3.
  lrecords2.at(0).SetUInt(2, 0);
  lrecords2.at(0).SetString(std::move(si3), 1);
  lrecords2.at(0).SetInt(0, 2);
  // Left 4.
  lrecords2.at(1).SetUInt(3, 0);
  lrecords2.at(1).SetString(std::move(si4), 1);
  lrecords2.at(1).SetInt(2, 2);
  // Left 5.
  lrecords3.at(0).SetUInt(4, 0);
  lrecords3.at(0).SetString(std::move(si5), 1);
  lrecords3.at(0).SetInt(1, 2);
  // Right 1, joins with left 1 and 5.
  rrecords1.at(0).SetUInt(100, 0);
  rrecords1.at(0).SetInt(1, 1);
  rrecords1.at(0).SetString(std::move(sd1), 2);
  // Right 2, does not join.
  rrecords1.at(1).SetUInt(100, 0);
  rrecords1.at(1).SetInt(-1, 1);
  rrecords1.at(1).SetString(std::move(sd2), 2);
  // Right 3, joins with left 2 and 4.
  rrecords2.at(0).SetUInt(50, 0);
  rrecords2.at(0).SetInt(2, 1);
  rrecords2.at(0).SetString(std::move(sd3), 2);

  // Setup join operator with two parents, left with id 0 and right with id 1.
  std::shared_ptr<IdentityOperator> iop1 = std::make_shared<IdentityOperator>();
  std::shared_ptr<IdentityOperator> iop2 = std::make_shared<IdentityOperator>();
  std::shared_ptr<EquiJoinOperator> op =
      std::make_shared<EquiJoinOperator>(2, 1);
  iop1->SetIndex(0);
  iop2->SetIndex(1);
  op->parents_ = {0,1};
  op->input_schemas_ = {lschema, rschema};
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
  jrecords.at(0).SetUInt(0, 0);
  jrecords.at(0).SetString(std::move(ji1), 1);
  jrecords.at(0).SetInt(1, 2);
  jrecords.at(0).SetUInt(100, 3);
  jrecords.at(0).SetString(std::move(jd1), 4);
  // 5 <-> 1.
  jrecords.at(1).SetUInt(4, 0);
  jrecords.at(1).SetString(std::move(ji5), 1);
  jrecords.at(1).SetInt(1, 2);
  jrecords.at(1).SetUInt(100, 3);
  jrecords.at(1).SetString(std::move(jd1_), 4);
  // 2 <-> 3.
  jrecords.at(2).SetUInt(1, 0);
  jrecords.at(2).SetString(std::move(ji2), 1);
  jrecords.at(2).SetInt(2, 2);
  jrecords.at(2).SetUInt(50, 3);
  jrecords.at(2).SetString(std::move(jd3), 4);
  // 4 <-> 3.
  jrecords.at(3).SetUInt(3, 0);
  jrecords.at(3).SetString(std::move(ji4), 1);
  jrecords.at(3).SetInt(2, 2);
  jrecords.at(3).SetUInt(50, 3);
  jrecords.at(3).SetString(std::move(jd3_), 4);

  // Test the output records are identical.
  EXPECT_EQ(output, jrecords);
}

TEST(EquiJoinOperatorTest, BasicLeftJoinTest) {
  // Make two joinable records.
  std::unique_ptr<std::string> s1 = std::make_unique<std::string>("item0");
  std::unique_ptr<std::string> s2 = std::make_unique<std::string>("descrp0");
  SchemaRef lschema = Schema1();
  SchemaRef rschema = Schema2();
  std::vector<Record> lrecords;
  std::vector<Record> rrecords;
  lrecords.emplace_back(lschema, true, 0_u, std::move(s1), -5_s);
  rrecords.emplace_back(rschema, true, 100_u, -5_s, std::move(s2));

  // Setup join operator with two parents, left with id 0 and right with id 1.
  std::shared_ptr<IdentityOperator> iop1 = std::make_shared<IdentityOperator>();
  std::shared_ptr<IdentityOperator> iop2 = std::make_shared<IdentityOperator>();
  std::shared_ptr<EquiJoinOperator> op =
      std::make_shared<EquiJoinOperator>(2, 1, EquiJoinOperator::Mode::LEFT);
  iop1->SetIndex(0);
  iop2->SetIndex(1);
  op->parents_ = {0,1};
  op->input_schemas_ = {lschema, rschema};
  op->ComputeOutputSchema();

  std::vector<Record> expected_records;
  expected_records.emplace_back(op->output_schema(), true, 0_u,
                                std::make_unique<std::string>("item0"), -5_s,
                                NullValue(), NullValue());
  expected_records.emplace_back(op->output_schema(), false, 0_u,
                                std::make_unique<std::string>("item0"), -5_s,
                                NullValue(), NullValue());
  expected_records.emplace_back(
      op->output_schema(), true, 0_u, std::make_unique<std::string>("item0"),
      -5_s, 100_u, std::make_unique<std::string>("descrp0"));

  // Process records.
  std::vector<Record> output;
  EXPECT_TRUE(op->Process(0, lrecords, &output));
  EXPECT_EQ(output.size(), 1);
  EXPECT_EQ(op->left_table_.count(), 1);
  EXPECT_IT_EQ(op->left_table_.Lookup(lrecords.at(0).GetValues({2})), lrecords);
  EXPECT_EQ(op->right_table_.count(), 0);
  EXPECT_TRUE(op->Process(1, rrecords, &output));
  EXPECT_EQ(op->left_table_.count(), 1);
  EXPECT_IT_EQ(op->left_table_.Lookup(lrecords.at(0).GetValues({2})), lrecords);
  EXPECT_EQ(op->right_table_.count(), 1);
  EXPECT_IT_EQ(op->right_table_.Lookup(rrecords.at(0).GetValues({1})),
               rrecords);
  EXPECT_EQ(output.size(), 3);
  EXPECT_EQ(output, expected_records);
}

TEST(EquiJoinOperatorTest, BasicRightJoinTest) {
  // Make two joinable records.
  std::unique_ptr<std::string> s1 = std::make_unique<std::string>("item0");
  std::unique_ptr<std::string> s2 = std::make_unique<std::string>("descrp0");
  SchemaRef lschema = Schema1();
  SchemaRef rschema = Schema2();
  std::vector<Record> lrecords;
  std::vector<Record> rrecords;
  lrecords.emplace_back(lschema, true, 0_u, std::move(s1), -5_s);
  rrecords.emplace_back(rschema, true, 100_u, -5_s, std::move(s2));

  // Setup join operator with two parents, left with id 0 and right with id 1.
  std::shared_ptr<IdentityOperator> iop1 = std::make_shared<IdentityOperator>();
  std::shared_ptr<IdentityOperator> iop2 = std::make_shared<IdentityOperator>();
  std::shared_ptr<EquiJoinOperator> op =
      std::make_shared<EquiJoinOperator>(2, 1, EquiJoinOperator::Mode::RIGHT);
  iop1->SetIndex(0);
  iop2->SetIndex(1);
  op->parents_ = {0,1};
  op->input_schemas_ = {lschema, rschema};
  op->ComputeOutputSchema();

  std::vector<Record> expected_records;
  expected_records.emplace_back(op->output_schema(), true, NullValue(),
                                NullValue(), NullValue(), 100_u,
                                std::make_unique<std::string>("descrp0"));
  expected_records.emplace_back(op->output_schema(), false, NullValue(),
                                NullValue(), NullValue(), 100_u,
                                std::make_unique<std::string>("descrp0"));
  expected_records.emplace_back(
      op->output_schema(), true, 0_u, std::make_unique<std::string>("item0"),
      -5_s, 100_u, std::make_unique<std::string>("descrp0"));

  // Process records.
  std::vector<Record> output;
  EXPECT_TRUE(op->Process(1, rrecords, &output));
  EXPECT_EQ(op->left_table_.count(), 0);
  EXPECT_EQ(op->right_table_.count(), 1);
  EXPECT_IT_EQ(op->right_table_.Lookup(rrecords.at(0).GetValues({1})),
               rrecords);
  EXPECT_EQ(output.size(), 1);
  EXPECT_TRUE(op->Process(0, lrecords, &output));
  EXPECT_EQ(output.size(), 3);
  EXPECT_EQ(op->left_table_.count(), 1);
  EXPECT_IT_EQ(op->left_table_.Lookup(lrecords.at(0).GetValues({2})), lrecords);
  EXPECT_EQ(op->right_table_.count(), 1);
  EXPECT_EQ(output, expected_records);
}

TEST(EquiJoinOperatorTest, LeftJoinTest) {
  // Make several sets of joinable records.
  std::unique_ptr<std::string> si1 = std::make_unique<std::string>("item0");
  std::unique_ptr<std::string> si2 = std::make_unique<std::string>("item1");
  std::unique_ptr<std::string> si3 = std::make_unique<std::string>("item2");
  std::unique_ptr<std::string> si4 = std::make_unique<std::string>("item3");
  std::unique_ptr<std::string> si5 = std::make_unique<std::string>("item4");
  std::unique_ptr<std::string> sd1 = std::make_unique<std::string>("descrp0");
  std::unique_ptr<std::string> sd2 = std::make_unique<std::string>("descrp1");
  std::unique_ptr<std::string> sd3 = std::make_unique<std::string>("descrp2");

  SchemaRef lschema = Schema1();
  SchemaRef rschema = Schema2();
  std::vector<Record> lrecords1;
  std::vector<Record> lrecords2;
  std::vector<Record> lrecords3;
  std::vector<Record> rrecords1;
  std::vector<Record> rrecords2;
  lrecords1.emplace_back(lschema, true, 0_u, std::move(si1), 1_s);
  lrecords1.emplace_back(lschema, true, 1_u, std::move(si2), 2_s);
  lrecords2.emplace_back(lschema, true, 2_u, std::move(si3), 0_s);
  lrecords2.emplace_back(lschema, true, 3_u, std::move(si4), 2_s);
  lrecords3.emplace_back(lschema, true, 4_u, std::move(si5), 1_s);
  rrecords1.emplace_back(rschema, true, 100_u, 1_s, std::move(sd1));
  rrecords1.emplace_back(rschema, true, 100_u, -1_s, std::move(sd2));
  rrecords2.emplace_back(rschema, true, 50_u, 2_s, std::move(sd3));

  // Setup join operator with two parents, left with id 0 and right with id 1.
  std::shared_ptr<IdentityOperator> iop1 = std::make_shared<IdentityOperator>();
  std::shared_ptr<IdentityOperator> iop2 = std::make_shared<IdentityOperator>();
  std::shared_ptr<EquiJoinOperator> op =
      std::make_shared<EquiJoinOperator>(2, 1, EquiJoinOperator::Mode::LEFT);
  iop1->SetIndex(0);
  iop2->SetIndex(1);
  op->parents_ = {0,1};
  op->input_schemas_ = {lschema, rschema};
  op->ComputeOutputSchema();

  // Process records.
  std::vector<Record> output;
  // Batch 1.
  EXPECT_TRUE(op->Process(0, lrecords1, &output));
  EXPECT_EQ(op->left_table_.count(), 2);
  EXPECT_EQ(op->right_table_.count(), 0);
  EXPECT_EQ(output.size(), 2);
  // Batch 2.
  EXPECT_TRUE(op->Process(1, rrecords1, &output));
  EXPECT_EQ(op->left_table_.count(), 2);
  EXPECT_EQ(op->right_table_.count(), 2);
  EXPECT_EQ(output.size(), 4);
  // Batch 3.
  EXPECT_TRUE(op->Process(0, lrecords2, &output));
  EXPECT_EQ(op->left_table_.count(), 4);
  EXPECT_EQ(op->right_table_.count(), 2);
  EXPECT_EQ(output.size(), 6);
  // Batch 4.
  EXPECT_TRUE(op->Process(0, lrecords3, &output));
  EXPECT_EQ(op->left_table_.count(), 5);
  EXPECT_EQ(op->right_table_.count(), 2);
  EXPECT_EQ(output.size(), 7);
  // Batch 5.
  EXPECT_TRUE(op->Process(1, rrecords2, &output));
  EXPECT_EQ(op->left_table_.count(), 5);
  EXPECT_EQ(op->right_table_.count(), 3);
  EXPECT_EQ(output.size(), 11);

  std::vector<Record> expected_records;
  expected_records.emplace_back(op->output_schema(), true, 0_u,
                                std::make_unique<std::string>("item0"), 1_s,
                                NullValue(), NullValue());
  expected_records.emplace_back(op->output_schema(), true, 1_u,
                                std::make_unique<std::string>("item1"), 2_s,
                                NullValue(), NullValue());
  expected_records.emplace_back(op->output_schema(), false, 0_u,
                                std::make_unique<std::string>("item0"), 1_s,
                                NullValue(), NullValue());
  expected_records.emplace_back(
      op->output_schema(), true, 0_u, std::make_unique<std::string>("item0"),
      1_s, 100_u, std::make_unique<std::string>("descrp0"));
  expected_records.emplace_back(op->output_schema(), true, 2_u,
                                std::make_unique<std::string>("item2"), 0_s,
                                NullValue(), NullValue());
  expected_records.emplace_back(op->output_schema(), true, 3_u,
                                std::make_unique<std::string>("item3"), 2_s,
                                NullValue(), NullValue());
  expected_records.emplace_back(
      op->output_schema(), true, 4_u, std::make_unique<std::string>("item4"),
      1_s, 100_u, std::make_unique<std::string>("descrp0"));
  expected_records.emplace_back(op->output_schema(), false, 1_u,
                                std::make_unique<std::string>("item1"), 2_s,
                                NullValue(), NullValue());
  expected_records.emplace_back(op->output_schema(), false, 3_u,
                                std::make_unique<std::string>("item3"), 2_s,
                                NullValue(), NullValue());
  expected_records.emplace_back(op->output_schema(), true, 1_u,
                                std::make_unique<std::string>("item1"), 2_s,
                                50_u, std::make_unique<std::string>("descrp2"));
  expected_records.emplace_back(op->output_schema(), true, 3_u,
                                std::make_unique<std::string>("item3"), 2_s,
                                50_u, std::make_unique<std::string>("descrp2"));
  EXPECT_EQ(output, expected_records);
}

}  // namespace dataflow
}  // namespace pelton
