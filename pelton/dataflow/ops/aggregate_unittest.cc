#include "pelton/dataflow/ops/aggregate.h"

#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"

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

void splitSort(std::vector<Record> &rs) {
  // The sorting for postitive and negative records is done separately
  // in order to avoid the algorithm flipping records at the positive boundary

  // find the split index (w.r.t the positive boundary)
  size_t splitIndex;
  for (size_t i = 0; i < rs.size(); i++) {
    if (rs[i].IsPositive() == 1) {
      splitIndex = i;
      break;
    }
  }

  if (splitIndex == 0) {
    std::sort(rs.begin(), rs.end(), compareRecordsLT);
    return;
  }

  // sort the positives and negatives separately
  std::sort(rs.begin(), rs.begin() + splitIndex, compareRecordsLT);
  std::sort(rs.begin() + splitIndex, rs.end(), compareRecordsLT);
}

void compareRecordStreams(std::vector<Record> &rs1, std::vector<Record> &rs2) {
  EXPECT_EQ(rs1.size(), rs2.size());

  // first sort the records based positves
  std::sort(rs1.begin(), rs1.end(), comparePositives);
  std::sort(rs2.begin(), rs2.end(), comparePositives);

  // then sort the subsets based on the actual values
  splitSort(rs1);
  splitSort(rs2);

  // Two records are equal if their positives, schemas and values are equal
  EXPECT_EQ(rs1, rs2);
}

inline SchemaOwner CreateSchema() {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::INT, CType::INT, CType::INT};
  std::vector<ColumnID> keys = {0};
  return SchemaOwner{names, types, keys};
}

inline SchemaOwner CreateSchemaString() {
  // Create a schema.
  std::vector<std::string> names = {"ID", "Department", "Quantity", "Location"};
  std::vector<CType> types = {CType::INT, CType::TEXT, CType::INT, CType::INT};
  std::vector<ColumnID> keys = {0};
  return SchemaOwner{names, types, keys};
}

TEST(AggregateOperatorTest, MultipleGroupColumnsCountPositive) {
  SchemaOwner schema = CreateSchemaString();
  std::vector<ColumnID> group_columns = {1, 3};
  ColumnID aggregate_column = -1;
  AggregateOperator::Function aggregate_function =
      AggregateOperator::Function::COUNT;
  // create aggregate operator..
  AggregateOperator aggregate =
      AggregateOperator(group_columns, aggregate_function, aggregate_column);
  aggregate.input_schemas_.push_back(SchemaRef(schema));
  aggregate.ComputeOutputSchema();

  // Records to be fed
  std::vector<Record> records;
  records.emplace_back(SchemaRef(schema), true, 1L,
                       std::make_unique<std::string>("CS"), 9L, 2L);
  records.emplace_back(SchemaRef(schema), true, 2L,
                       std::make_unique<std::string>("CS"), 7L, 2L);
  records.emplace_back(SchemaRef(schema), true, 3L,
                       std::make_unique<std::string>("Mechanical"), 5L, 6L);
  records.emplace_back(SchemaRef(schema), true, 4L,
                       std::make_unique<std::string>("Mechanical"), 7L, 6L);

  // expected output
  std::vector<Record> expected_records;
  expected_records.emplace_back(aggregate.output_schema_, true, std::make_unique<std::string>("CS"), 2L, (uint64_t)2ULL);
  expected_records.emplace_back(aggregate.output_schema_, true, std::make_unique<std::string>("Mechanical"), 6L, (uint64_t)2ULL);

  // Feed records and test
  std::vector<Record> outputs;
  EXPECT_TRUE(aggregate.Process(UNDEFINED_NODE_INDEX, records, &outputs));
  compareRecordStreams(expected_records, outputs);
}

TEST(AggregateOperatorTest, MultipleGroupColumnsSumPositive) {
  SchemaOwner schema = CreateSchemaString();
  std::vector<ColumnID> group_columns = {1, 3};
  ColumnID aggregate_column = 2;
  AggregateOperator::Function aggregate_function =
      AggregateOperator::Function::SUM;
  // create aggregate operator..
  AggregateOperator aggregate =
      AggregateOperator(group_columns, aggregate_function, aggregate_column);
  aggregate.input_schemas_.push_back(SchemaRef(schema));
  aggregate.ComputeOutputSchema();

  // Records to be fed
  std::vector<Record> records;
  records.emplace_back(SchemaRef(schema), true, 1L,
                       std::make_unique<std::string>("CS"), 9L, 2L);
  records.emplace_back(SchemaRef(schema), true, 2L,
                       std::make_unique<std::string>("CS"), 7L, 2L);
  records.emplace_back(SchemaRef(schema), true, 3L,
                       std::make_unique<std::string>("Mechanical"), 5L, 6L);
  records.emplace_back(SchemaRef(schema), true, 4L,
                       std::make_unique<std::string>("Mechanical"), 7L, 6L);

  // expected output
  std::vector<Record> expected_records;
  expected_records.emplace_back(aggregate.output_schema_, true, std::make_unique<std::string>("CS"), 2L, 16L);
  expected_records.emplace_back(aggregate.output_schema_, true, std::make_unique<std::string>("Mechanical"), 6L, 12L);

  // Feed records and test
  std::vector<Record> outputs;
  EXPECT_TRUE(aggregate.Process(UNDEFINED_NODE_INDEX, records, &outputs));
  compareRecordStreams(expected_records, outputs);
}

TEST(AggregateOperatorTest, CountPositiveNegative) {
  SchemaOwner schema = CreateSchema();
  std::vector<ColumnID> group_columns = {1};
  ColumnID aggregate_column = -1;
  AggregateOperator::Function aggregate_function =
      AggregateOperator::Function::COUNT;
  // create aggregate operator..
  AggregateOperator aggregate =
      AggregateOperator(group_columns, aggregate_function, aggregate_column);
  aggregate.input_schemas_.push_back(SchemaRef(schema));
  aggregate.ComputeOutputSchema();

  // Description: The test consists of two stages:
  // STAGE1: records are fed for the first time, expect all output records to
  // be positive; STAGE2: a mix of positive and negative records are fed,
  // output records can contain either positive or negative records.

  // Records to be fed
  std::vector<Record> records1;
  records1.emplace_back(SchemaRef(schema), true, 1L, 2L, 9L);
  records1.emplace_back(SchemaRef(schema), true, 2L, 2L, 7L);
  records1.emplace_back(SchemaRef(schema), true, 3L, 5L, 5L);
  records1.emplace_back(SchemaRef(schema), true, 4L, 5L, 6L);

  // STAGE1
  std::vector<Record> outputs1;
  EXPECT_TRUE(aggregate.Process(UNDEFINED_NODE_INDEX, records1, &outputs1));

  // Records to be fed
  std::vector<Record> records2;
  records2.emplace_back(SchemaRef(schema), false, 5L, 2L, 6L);
  records2.emplace_back(SchemaRef(schema), true, 6L, 5L, 7L);
  records2.emplace_back(SchemaRef(schema), true, 7L, 7L, 7L);
  records2.emplace_back(SchemaRef(schema), false, 5L, 2L, 6L);

  // expected output
  std::vector<Record> expected_records;
  expected_records.emplace_back(aggregate.output_schema_, false, 2L, (uint64_t)2ULL);
  expected_records.emplace_back(aggregate.output_schema_, true, 2L, (uint64_t)0ULL);
  expected_records.emplace_back(aggregate.output_schema_, false, 5L, (uint64_t)2ULL);
  expected_records.emplace_back(aggregate.output_schema_, true, 5L, (uint64_t)3ULL);
  expected_records.emplace_back(aggregate.output_schema_, true, 7L, (uint64_t)1ULL);

  // STAGE2
  std::vector<Record> outputs2;
  EXPECT_TRUE(aggregate.Process(UNDEFINED_NODE_INDEX, records2, &outputs2));
  compareRecordStreams(expected_records, outputs2);
}

TEST(AggregateOperatorTest, SumPositiveNegative) {
  SchemaOwner schema = CreateSchema();
  std::vector<ColumnID> group_columns = {1};
  ColumnID aggregate_column = 2;
  AggregateOperator::Function aggregate_function =
      AggregateOperator::Function::SUM;
  // create aggregate operator..
  AggregateOperator aggregate =
      AggregateOperator(group_columns, aggregate_function, aggregate_column);
  aggregate.input_schemas_.push_back(SchemaRef(schema));
  aggregate.ComputeOutputSchema();

  // Description: The test consists of two stages:
  // STAGE1: records are fed for the first time, expect all output records to
  // be positive; STAGE2: a mix of positive and negative records are fed,
  // output records can contain either positive or negative records.

  // Records to be fed
  std::vector<Record> records1;
  records1.emplace_back(SchemaRef(schema), true, 1L, 2L, 9L);
  records1.emplace_back(SchemaRef(schema), true, 2L, 2L, 7L);
  records1.emplace_back(SchemaRef(schema), true, 3L, 5L, 5L);
  records1.emplace_back(SchemaRef(schema), true, 4L, 5L, 6L);

  // STAGE1
  std::vector<Record> outputs1;
  EXPECT_TRUE(aggregate.Process(UNDEFINED_NODE_INDEX, records1, &outputs1));

  // Records to be fed
  std::vector<Record> records2;
  records2.emplace_back(SchemaRef(schema), false, 5L, 2L, 6L);
  records2.emplace_back(SchemaRef(schema), true, 6L, 7L, 7L);

  // expected output
  std::vector<Record> expected_records;
  expected_records.emplace_back(aggregate.output_schema_, false, 2L, 16L);
  expected_records.emplace_back(aggregate.output_schema_, true, 2L, 10L);
  expected_records.emplace_back(aggregate.output_schema_, true, 7L, 7L);

  // STAGE2
  std::vector<Record> outputs2;
  EXPECT_TRUE(aggregate.Process(UNDEFINED_NODE_INDEX, records2, &outputs2));
  compareRecordStreams(expected_records, outputs2);
}

TEST(AggregateOperatorTest, CountPositiveNegativeSingleBatch) {
  SchemaOwner schema = CreateSchema();
  std::vector<ColumnID> group_columns = {1};
  ColumnID aggregate_column = -1;
  AggregateOperator::Function aggregate_function =
      AggregateOperator::Function::COUNT;
  // create aggregate operator..
  AggregateOperator aggregate =
      AggregateOperator(group_columns, aggregate_function, aggregate_column);
  aggregate.input_schemas_.push_back(SchemaRef(schema));
  aggregate.ComputeOutputSchema();

  // Records to be fed
  std::vector<Record> records;
  records.emplace_back(SchemaRef(schema), true, 1L, 2L, 9L);
  records.emplace_back(SchemaRef(schema), true, 2L, 2L, 7L);
  records.emplace_back(SchemaRef(schema), true, 3L, 5L, 5L);
  records.emplace_back(SchemaRef(schema), true, 4L, 5L, 6L);
  records.emplace_back(SchemaRef(schema), true, 5L, 7L, 7L);
  records.emplace_back(SchemaRef(schema), false, 6L, 7L, 7L);

  // expected output
  std::vector<Record> expected_records;
  expected_records.emplace_back(aggregate.output_schema_, true, 2L, (uint64_t)2ULL);
  expected_records.emplace_back(aggregate.output_schema_, true, 5L, (uint64_t)2ULL);

  std::vector<Record> outputs;
  EXPECT_TRUE(aggregate.Process(UNDEFINED_NODE_INDEX, records, &outputs));
  compareRecordStreams(expected_records, outputs);
}

TEST(AggregateOperatorTest, SumPositiveNegativeSingleBatch) {
  SchemaOwner schema = CreateSchema();
  std::vector<ColumnID> group_columns = {1};
  ColumnID aggregate_column = 2;
  AggregateOperator::Function aggregate_function =
      AggregateOperator::Function::SUM;
  // create aggregate operator..
  AggregateOperator aggregate =
      AggregateOperator(group_columns, aggregate_function, aggregate_column);
  aggregate.input_schemas_.push_back(SchemaRef(schema));
  aggregate.ComputeOutputSchema();

  // Records to be fed
  std::vector<Record> records;
  records.emplace_back(SchemaRef(schema), true, 1L, 2L, 9L);
  records.emplace_back(SchemaRef(schema), true, 2L, 2L, 7L);
  records.emplace_back(SchemaRef(schema), true, 3L, 5L, 5L);
  records.emplace_back(SchemaRef(schema), true, 4L, 5L, 6L);
  records.emplace_back(SchemaRef(schema), true, 5L, 7L, 7L);
  records.emplace_back(SchemaRef(schema), false, 6L, 7L, 7L);

  // expected output
  std::vector<Record> expected_records;
  expected_records.emplace_back(aggregate.output_schema_, true, 2L, 16L);
  expected_records.emplace_back(aggregate.output_schema_, true, 5L, 11L);

  std::vector<Record> outputs;
  EXPECT_TRUE(aggregate.Process(UNDEFINED_NODE_INDEX, records, &outputs));
  compareRecordStreams(expected_records, outputs);
}

}  // namespace dataflow
}  // namespace pelton
