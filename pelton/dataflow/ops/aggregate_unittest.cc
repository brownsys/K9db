#include "pelton/dataflow/ops/aggregate.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/ints.h"

namespace pelton {
namespace dataflow {

using CType = sqlast::ColumnDefinition::Type;

bool comparePositives(const Record &r1, const Record &r2) {
  if (r1.IsPositive() > r2.IsPositive()) {
    return false;
  }
  return true;
}

bool compareRecordsLT(const Record &r1, const Record &r2) {
  EXPECT_EQ(r1.schema(), r2.schema());
  for (size_t i = 0; i < r1.schema().size(); i++) {
    switch (r1.schema().TypeOf(i)) {
      case sqlast::ColumnDefinition::Type::UINT:
        if ((r1.GetUInt(i)) < (r2.GetUInt(i))) {
          return true;
        } else if ((r1.GetUInt(i)) == (r2.GetUInt(i))) {
          // need to check next value in the record
          continue;
        } else if ((r1.GetUInt(i)) > (r2.GetUInt(i))) {
          // should only reach here if some N previous values of the records are
          // equal
          return false;
        }
      case sqlast::ColumnDefinition::Type::INT:
        if ((r1.GetInt(i)) < (r2.GetInt(i))) {
          return true;
        } else if ((r1.GetInt(i)) == (r2.GetInt(i))) {
          // need to check next value in the record
          continue;
        } else if ((r1.GetInt(i)) > (r2.GetInt(i))) {
          // should only reach here if some N previous values of the records are
          // equal
          return false;
        }
      case sqlast::ColumnDefinition::Type::TEXT:
        if ((r1.GetString(i)) < (r2.GetString(i))) {
          return true;
        } else if ((r1.GetString(i)) == (r2.GetString(i))) {
          // need to check next value in the record
          continue;
        } else if ((r1.GetString(i)) > (r2.GetString(i))) {
          // should only reach here if some N previous values of the records are
          // equal
          return false;
        }
      default:
        LOG(FATAL) << "Unsupported data type";
    }
  }
  // the two records are equal
  return true;
}

void splitSort(std::vector<Record> *rs) {
  // The sorting for postitive and negative records is done separately
  // in order to avoid the algorithm flipping records at the positive boundary

  // find the split index (w.r.t the positive boundary)
  size_t splitIndex;
  for (size_t i = 0; i < rs->size(); i++) {
    if (rs->at(i).IsPositive() == 1) {
      splitIndex = i;
      break;
    }
  }

  if (splitIndex == 0) {
    std::sort(rs->begin(), rs->end(), compareRecordsLT);
    return;
  }

  // sort the positives and negatives separately
  std::sort(rs->begin(), rs->begin() + splitIndex, compareRecordsLT);
  std::sort(rs->begin() + splitIndex, rs->end(), compareRecordsLT);
}

void compareRecordStreams(std::vector<Record> *rs1, std::vector<Record> *rs2) {
  EXPECT_EQ(rs1->size(), rs2->size());

  // first sort the records based positves
  std::sort(rs1->begin(), rs1->end(), comparePositives);
  std::sort(rs2->begin(), rs2->end(), comparePositives);

  // then sort the subsets based on the actual values
  splitSort(rs1);
  splitSort(rs2);

  // Two records are equal if their positives, schemas and values are equal
  EXPECT_EQ(*rs1, *rs2);
}

inline SchemaRef CreateSchemaPrimaryKey() {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::INT, CType::INT, CType::INT};
  std::vector<ColumnID> keys = {0};
  return SchemaFactory::Create(names, types, keys);
}

inline SchemaRef CreateSchemaString() {
  // Create a schema.
  std::vector<std::string> names = {"ID", "Department", "Quantity", "Location"};
  std::vector<CType> types = {CType::INT, CType::TEXT, CType::INT, CType::INT};
  std::vector<ColumnID> keys = {0};
  return SchemaFactory::Create(names, types, keys);
}

inline SchemaRef CreateSchemaCompositeKey() {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3", "Col4", "Col5"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT, CType::INT,
                              CType::INT};
  std::vector<ColumnID> keys = {1, 4};
  return SchemaFactory::Create(names, types, keys);
}

TEST(AggregateOperatorTest, MultipleGroupColumnsCountPositive) {
  SchemaRef schema = CreateSchemaString();
  std::vector<ColumnID> group_columns = {1, 3};
  ColumnID aggregate_column = -1;
  AggregateOperator::Function aggregate_function =
      AggregateOperator::Function::COUNT;
  // create aggregate operator..
  AggregateOperator aggregate =
      AggregateOperator(group_columns, aggregate_function, aggregate_column);
  aggregate.input_schemas_.push_back(schema);
  aggregate.ComputeOutputSchema();

  // Records to be fed
  std::vector<Record> records;
  records.emplace_back(schema, true, 1_s, std::make_unique<std::string>("CS"),
                       9_s, 2_s);
  records.emplace_back(schema, true, 2_s, std::make_unique<std::string>("CS"),
                       7_s, 2_s);
  records.emplace_back(schema, true, 3_s,
                       std::make_unique<std::string>("Mechanical"), 5_s, 6_s);
  records.emplace_back(schema, true, 4_s,
                       std::make_unique<std::string>("Mechanical"), 7_s, 6_s);

  // expected output
  std::vector<Record> expected_records;
  expected_records.emplace_back(aggregate.output_schema_, true,
                                std::make_unique<std::string>("CS"), 2_s, 2_u);
  expected_records.emplace_back(aggregate.output_schema_, true,
                                std::make_unique<std::string>("Mechanical"),
                                6_s, 2_u);

  // Feed records and test
  std::vector<Record> outputs = aggregate.Process(
      UNDEFINED_NODE_INDEX, std::move(records), Promise::None);
  compareRecordStreams(&expected_records, &outputs);
}

TEST(AggregateOperatorTest, MultipleGroupColumnsSumPositive) {
  SchemaRef schema = CreateSchemaString();
  std::vector<ColumnID> group_columns = {1, 3};
  ColumnID aggregate_column = 2;
  AggregateOperator::Function aggregate_function =
      AggregateOperator::Function::SUM;
  // create aggregate operator..
  AggregateOperator aggregate =
      AggregateOperator(group_columns, aggregate_function, aggregate_column);
  aggregate.input_schemas_.push_back(schema);
  aggregate.ComputeOutputSchema();

  // Records to be fed
  std::vector<Record> records;
  records.emplace_back(schema, true, 1_s, std::make_unique<std::string>("CS"),
                       9_s, 2_s);
  records.emplace_back(schema, true, 2_s, std::make_unique<std::string>("CS"),
                       7_s, 2_s);
  records.emplace_back(schema, true, 3_s,
                       std::make_unique<std::string>("Mechanical"), 5_s, 6_s);
  records.emplace_back(schema, true, 4_s,
                       std::make_unique<std::string>("Mechanical"), 7_s, 6_s);

  // expected output
  std::vector<Record> expected_records;
  expected_records.emplace_back(aggregate.output_schema_, true,
                                std::make_unique<std::string>("CS"), 2_s, 16_s);
  expected_records.emplace_back(aggregate.output_schema_, true,
                                std::make_unique<std::string>("Mechanical"),
                                6_s, 12_s);

  // Feed records and test
  std::vector<Record> outputs = aggregate.Process(
      UNDEFINED_NODE_INDEX, std::move(records), Promise::None);
  compareRecordStreams(&expected_records, &outputs);
}

TEST(AggregateOperatorTest, CountPositiveNegative) {
  SchemaRef schema = CreateSchemaPrimaryKey();
  std::vector<ColumnID> group_columns = {1};
  ColumnID aggregate_column = -1;
  AggregateOperator::Function aggregate_function =
      AggregateOperator::Function::COUNT;
  // create aggregate operator..
  AggregateOperator aggregate =
      AggregateOperator(group_columns, aggregate_function, aggregate_column);
  aggregate.input_schemas_.push_back(schema);
  aggregate.ComputeOutputSchema();

  // Description: The test consists of two stages:
  // STAGE1: records are fed for the first time, expect all output records to
  // be positive; STAGE2: a mix of positive and negative records are fed,
  // output records can contain either positive or negative records.

  // Records to be fed
  std::vector<Record> records1;
  records1.emplace_back(schema, true, 1_s, 2_s, 9_s);
  records1.emplace_back(schema, true, 2_s, 2_s, 7_s);
  records1.emplace_back(schema, true, 3_s, 5_s, 5_s);
  records1.emplace_back(schema, true, 4_s, 5_s, 6_s);

  // STAGE1
  std::vector<Record> outputs1 = aggregate.Process(
      UNDEFINED_NODE_INDEX, std::move(records1), Promise::None);

  // Records to be fed
  std::vector<Record> records2;
  records2.emplace_back(schema, false, 5_s, 2_s, 6_s);
  records2.emplace_back(schema, true, 6_s, 5_s, 7_s);
  records2.emplace_back(schema, true, 7_s, 7_s, 7_s);
  records2.emplace_back(schema, false, 5_s, 2_s, 6_s);

  // expected output
  std::vector<Record> expected_records;
  expected_records.emplace_back(aggregate.output_schema_, false, 2_s, 2_u);
  expected_records.emplace_back(aggregate.output_schema_, false, 5_s, 2_u);
  expected_records.emplace_back(aggregate.output_schema_, true, 5_s, 3_u);
  expected_records.emplace_back(aggregate.output_schema_, true, 7_s, 1_u);

  // STAGE2
  std::vector<Record> outputs2 = aggregate.Process(
      UNDEFINED_NODE_INDEX, std::move(records2), Promise::None);
  compareRecordStreams(&expected_records, &outputs2);
}

TEST(AggregateOperatorTest, SumPositiveNegative) {
  SchemaRef schema = CreateSchemaPrimaryKey();
  std::vector<ColumnID> group_columns = {1};
  ColumnID aggregate_column = 2;
  AggregateOperator::Function aggregate_function =
      AggregateOperator::Function::SUM;
  // create aggregate operator..
  AggregateOperator aggregate =
      AggregateOperator(group_columns, aggregate_function, aggregate_column);
  aggregate.input_schemas_.push_back(schema);
  aggregate.ComputeOutputSchema();

  // Description: The test consists of two stages:
  // STAGE1: records are fed for the first time, expect all output records to
  // be positive; STAGE2: a mix of positive and negative records are fed,
  // output records can contain either positive or negative records.

  // Records to be fed
  std::vector<Record> records1;
  records1.emplace_back(schema, true, 1_s, 2_s, 9_s);
  records1.emplace_back(schema, true, 2_s, 2_s, 7_s);
  records1.emplace_back(schema, true, 3_s, 5_s, 5_s);
  records1.emplace_back(schema, true, 4_s, 5_s, 6_s);

  // STAGE1
  std::vector<Record> outputs1 = aggregate.Process(
      UNDEFINED_NODE_INDEX, std::move(records1), Promise::None);

  // Records to be fed
  std::vector<Record> records2;
  records2.emplace_back(schema, false, 5_s, 2_s, 6_s);
  records2.emplace_back(schema, true, 6_s, 7_s, 7_s);

  // expected output
  std::vector<Record> expected_records;
  expected_records.emplace_back(aggregate.output_schema_, false, 2_s, 16_s);
  expected_records.emplace_back(aggregate.output_schema_, true, 2_s, 10_s);
  expected_records.emplace_back(aggregate.output_schema_, true, 7_s, 7_s);

  // STAGE2
  std::vector<Record> outputs2 = aggregate.Process(
      UNDEFINED_NODE_INDEX, std::move(records2), Promise::None);
  compareRecordStreams(&expected_records, &outputs2);
}

TEST(AggregateOperatorTest, CountPositiveNegativeSingleBatch) {
  SchemaRef schema = CreateSchemaPrimaryKey();
  std::vector<ColumnID> group_columns = {1};
  ColumnID aggregate_column = -1;
  AggregateOperator::Function aggregate_function =
      AggregateOperator::Function::COUNT;
  // create aggregate operator..
  AggregateOperator aggregate =
      AggregateOperator(group_columns, aggregate_function, aggregate_column);
  aggregate.input_schemas_.push_back(schema);
  aggregate.ComputeOutputSchema();

  // Records to be fed
  std::vector<Record> records;
  records.emplace_back(schema, true, 1_s, 2_s, 9_s);
  records.emplace_back(schema, true, 2_s, 2_s, 7_s);
  records.emplace_back(schema, true, 3_s, 5_s, 5_s);
  records.emplace_back(schema, true, 4_s, 5_s, 6_s);
  records.emplace_back(schema, true, 5_s, 7_s, 7_s);
  records.emplace_back(schema, false, 6_s, 7_s, 7_s);

  // expected output
  std::vector<Record> expected_records;
  expected_records.emplace_back(aggregate.output_schema_, true, 2_s, 2_u);
  expected_records.emplace_back(aggregate.output_schema_, true, 5_s, 2_u);

  std::vector<Record> outputs = aggregate.Process(
      UNDEFINED_NODE_INDEX, std::move(records), Promise::None);
  compareRecordStreams(&expected_records, &outputs);
}

TEST(AggregateOperatorTest, SumPositiveNegativeSingleBatch) {
  SchemaRef schema = CreateSchemaPrimaryKey();
  std::vector<ColumnID> group_columns = {1};
  ColumnID aggregate_column = 2;
  AggregateOperator::Function aggregate_function =
      AggregateOperator::Function::SUM;
  // create aggregate operator..
  AggregateOperator aggregate =
      AggregateOperator(group_columns, aggregate_function, aggregate_column);
  aggregate.input_schemas_.push_back(schema);
  aggregate.ComputeOutputSchema();

  // Records to be fed
  std::vector<Record> records;
  records.emplace_back(schema, true, 1_s, 2_s, 9_s);
  records.emplace_back(schema, true, 2_s, 2_s, 7_s);
  records.emplace_back(schema, true, 3_s, 5_s, 5_s);
  records.emplace_back(schema, true, 4_s, 5_s, 6_s);
  records.emplace_back(schema, true, 5_s, 7_s, 7_s);
  records.emplace_back(schema, false, 6_s, 7_s, 7_s);

  // expected output
  std::vector<Record> expected_records;
  expected_records.emplace_back(aggregate.output_schema_, true, 2_s, 16_s);
  expected_records.emplace_back(aggregate.output_schema_, true, 5_s, 11_s);
  expected_records.emplace_back(aggregate.output_schema_, true, 7_s, 0_s);

  std::vector<Record> outputs = aggregate.Process(
      UNDEFINED_NODE_INDEX, std::move(records), Promise::None);
  compareRecordStreams(&expected_records, &outputs);
}

TEST(AggregateOperatorTest, OutputSchemaPrimaryKeyTest) {
  // TEST 1: Primary keyed schema's keycolumn included in projected schema
  SchemaRef schema = CreateSchemaPrimaryKey();
  std::vector<ColumnID> group_columns = {0};
  // create project operator..
  AggregateOperator aggregate1 =
      AggregateOperator(group_columns, AggregateOperator::Function::SUM, 2);
  aggregate1.input_schemas_.push_back(schema);
  aggregate1.ComputeOutputSchema();
  // expected data in output schema
  std::vector<std::string> expected_names = {"Col1", "Sum"};
  std::vector<CType> expected_types = {CType::INT, CType::INT};
  // test schema data
  EXPECT_EQ(aggregate1.output_schema_.column_names(), expected_names);
  EXPECT_EQ(aggregate1.output_schema_.column_types(), expected_types);
  EXPECT_EQ(aggregate1.output_schema_.keys(), (std::vector<ColumnID>{0}));

  // TEST 2: Primary keyed schema's keycolumn not included in projected schema
  group_columns = {1};
  AggregateOperator aggregate2 =
      AggregateOperator(group_columns, AggregateOperator::Function::SUM, 2);
  aggregate2.input_schemas_.push_back(schema);
  aggregate2.ComputeOutputSchema();
  // expected data in output schema
  expected_names = {"Col2", "Sum"};
  expected_types = {CType::INT, CType::INT};
  EXPECT_EQ(aggregate2.output_schema_.column_names(), expected_names);
  EXPECT_EQ(aggregate2.output_schema_.column_types(), expected_types);
  EXPECT_EQ(aggregate2.output_schema_.keys(), (std::vector<ColumnID>{0}));
}

TEST(AggregateOperatorTest, OutputSchemaCompositeKeyTest) {
  // TEST 1: Primary keyed schema's keycolumn included in projected schema
  SchemaRef schema = CreateSchemaCompositeKey();
  std::vector<ColumnID> group_columns = {0, 1, 4};
  // create project operator..
  AggregateOperator aggregate1 =
      AggregateOperator(group_columns, AggregateOperator::Function::SUM, 2);
  aggregate1.input_schemas_.push_back(schema);
  aggregate1.ComputeOutputSchema();
  // expected data in output schema
  std::vector<std::string> expected_names = {"Col1", "Col2", "Col5", "Sum"};
  std::vector<CType> expected_types = {CType::UINT, CType::TEXT, CType::INT,
                                       CType::INT};
  // test schema data
  EXPECT_EQ(aggregate1.output_schema_.column_names(), expected_names);
  EXPECT_EQ(aggregate1.output_schema_.column_types(), expected_types);
  EXPECT_EQ(aggregate1.output_schema_.keys(), (std::vector<ColumnID>{0, 1, 2}));

  // TEST 2: Primary keyed schema's keycolumn not included in projected schema
  group_columns = {0, 1};
  AggregateOperator aggregate2 =
      AggregateOperator(group_columns, AggregateOperator::Function::SUM, 2);
  aggregate2.input_schemas_.push_back(schema);
  aggregate2.ComputeOutputSchema();
  // expected data in output schema
  expected_names = {"Col1", "Col2", "Sum"};
  expected_types = {CType::UINT, CType::TEXT, CType::INT};
  EXPECT_EQ(aggregate2.output_schema_.column_names(), expected_names);
  EXPECT_EQ(aggregate2.output_schema_.column_types(), expected_types);
  EXPECT_EQ(aggregate2.output_schema_.keys(), (std::vector<ColumnID>{0, 1}));
}

}  // namespace dataflow
}  // namespace pelton
