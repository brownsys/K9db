#ifndef PELTON_DATAFLOW_OPS_PROJECT_H_
#define PELTON_DATAFLOW_OPS_PROJECT_H_

#include <cstdint>
#include <string>
#include <tuple>
// NOLINTNEXTLINE
#include <variant>
#include <vector>

#include "gtest/gtest_prod.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/project_enum.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class ProjectOperator : public Operator {
 public:
  using Operation = ProjectOperationEnum;
  using Metadata = ProjectMetadataEnum;

  ProjectOperator()
      : Operator(Operator::Type::PROJECT),
        has_keys_(false),
        keys_(),
        projections_() {}

  explicit ProjectOperator(const std::vector<ColumnID> &keys)
      : Operator(Operator::Type::PROJECT),
        has_keys_(true),
        keys_(keys),
        projections_() {}

  void AddProjection(const std::string &column_name, ColumnID cid) {
    this->projections_.push_back(std::make_tuple(
        column_name, cid, Operation::NONE, uint64_t(-1), Metadata::COLUMN));
  }
  // For projecting literal, user needs to explicitly specify Metadata::LITERAL.
  // This is done in order to make the APIs more usable, else explicit casting
  // would be required
  //  in order to distinguish between LITERAL and COLUMN.
  void AddProjection(const std::string &column_name,
                     Record::DataVariant literal, Metadata metadata) {
    this->projections_.push_back(
        std::make_tuple(column_name, 99, Operation::NONE, literal, metadata));
  }
  void AddProjection(std::string column_name, ColumnID left_operand,
                     Operation operation, int64_t right_operand,
                     Metadata metadata) {
    this->projections_.push_back(std::make_tuple(
        column_name, left_operand, operation, right_operand, metadata));
  }
  void AddProjection(std::string column_name, ColumnID left_operand,
                     Operation operation, uint64_t right_operand,
                     Metadata metadata) {
    this->projections_.push_back(std::make_tuple(
        column_name, left_operand, operation, right_operand, metadata));
  }

 protected:
  bool Process(NodeIndex source, const std::vector<Record> &records,
               std::vector<Record> *output) override;
  void ComputeOutputSchema() override;

 private:
  bool has_keys_;
  std::vector<ColumnID> keys_;

  std::vector<std::tuple<std::string, ColumnID, Operation, Record::DataVariant,
                         Metadata>>
      projections_;
  bool EnclosedKeyCols(const std::vector<ColumnID> &input_keycols,
                       const std::vector<ColumnID> &cids) const;
  // Allow tests to use .Process(...) directly.
  FRIEND_TEST(ProjectOperatorTest, BatchTestColumn);
  FRIEND_TEST(ProjectOperatorTest, BatchTestLiteral);
  FRIEND_TEST(ProjectOperatorTest, BatchTestOperationRightColumn);
  FRIEND_TEST(ProjectOperatorTest, BatchTestOperationRightLiteral);
  FRIEND_TEST(ProjectOperatorTest, OutputSchemaPrimaryKeyTest);
  FRIEND_TEST(ProjectOperatorTest, OutputSchemaCompositeKeyTest);
  FRIEND_TEST(ProjectOperatorTest, NullValueTest);
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_PROJECT_H_
