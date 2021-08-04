#ifndef PELTON_DATAFLOW_OPS_PROJECT_H_
#define PELTON_DATAFLOW_OPS_PROJECT_H_

#include <cstdint>
#include <string>
#include <memory>
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

  ProjectOperator() : Operator(Operator::Type::PROJECT), projections_() {}

  void AddColumnProjection(const std::string &column_name, ColumnID cid) {
    this->projections_.emplace_back(column_name, cid);
  }

  void AddLiteralProjection(const std::string &column_name,
                            Record::DataVariant literal) {
    this->projections_.emplace_back(column_name, literal, true);
  }

  void AddArithmeticColumnsProjection(const std::string &column_name,
                                      ColumnID left_col, ColumnID right_col,
                                      Operation op) {
    this->projections_.emplace_back(column_name, op, left_col, right_col);
  }
  void AddArithmeticLeftProjection(std::string column_name, ColumnID left_col,
                                   Record::DataVariant literal, Operation op) {
    if (literal.index() == 2 && this->input_schemas_.size() > 0 &&
        this->input_schemas_.at(0).TypeOf(left_col) ==
            sqlast::ColumnDefinition::Type::UINT) {
      int64_t v = std::get<2>(literal);
      CHECK_GE(v, 0);
      this->AddArithmeticLeftProjection(column_name, left_col,
                                        static_cast<uint64_t>(v), op);
    } else {
      this->projections_.emplace_back(column_name, left_col, op, literal);
    }
  }
  void AddArithmeticRightProjection(std::string column_name,
                                    Record::DataVariant literal,
                                    ColumnID right_col, Operation op) {
    if (literal.index() == 2 && this->input_schemas_.size() > 0 &&
        this->input_schemas_.at(0).TypeOf(right_col) ==
            sqlast::ColumnDefinition::Type::UINT) {
      int64_t v = std::get<2>(literal);
      CHECK_GE(v, 0);
      this->AddArithmeticRightProjection(column_name, static_cast<uint64_t>(v),
                                         right_col, op);
    } else {
      this->projections_.emplace_back(column_name, literal, right_col, op);
    }
  }

  std::shared_ptr<Operator> Clone() const override;

 protected:
  std::optional<std::vector<Record>> Process(
      NodeIndex, const std::vector<Record> &records) override;

  void ComputeOutputSchema() override;

 private:
  class ProjectionOperation {
   public:
    // Project column.
    ProjectionOperation(const std::string &name, ColumnID cid)
        : name_(name),
          left_(static_cast<uint64_t>(cid)),
          right_(NullValue()),
          meta_(Metadata::COLUMN),
          op_(Operation::NONE) {}

    // Project literal (bool _ is for distinguishing from other constructors).
    ProjectionOperation(const std::string &name, Record::DataVariant literal,
                        bool _)
        : name_(name),
          left_(literal),
          right_(NullValue()),
          meta_(Metadata::LITERAL),
          op_(Operation::NONE) {}

    // Project arithmetic operation
    ProjectionOperation(const std::string &name, Operation op, ColumnID left,
                        ColumnID right)
        : name_(name),
          left_(static_cast<uint64_t>(left)),
          right_(static_cast<uint64_t>(right)),
          meta_(Metadata::ARITHMETIC_WITH_COLUMN),
          op_(op) {
      CHECK_NE(op, Operation::NONE);
    }
    ProjectionOperation(const std::string &name, ColumnID left, Operation op,
                        Record::DataVariant literal)
        : name_(name),
          left_(static_cast<uint64_t>(left)),
          right_(literal),
          meta_(Metadata::ARITHMETIC_WITH_LITERAL_LEFT),
          op_(op) {
      CHECK_NE(op, Operation::NONE);
    }
    ProjectionOperation(const std::string &name, Record::DataVariant literal,
                        ColumnID right, Operation op)
        : name_(name),
          left_(literal),
          right_(static_cast<uint64_t>(right)),
          meta_(Metadata::ARITHMETIC_WITH_LITERAL_RIGHT),
          op_(op) {
      CHECK_NE(op, Operation::NONE);
    }

    // Checks.
    bool column() const { return this->meta_ == Metadata::COLUMN; }
    bool literal() const { return this->meta_ == Metadata::LITERAL; }
    bool arithemtic() const { return this->op_ != Operation::NONE; }
    bool left_column() const {
      return this->meta_ == Metadata::ARITHMETIC_WITH_COLUMN ||
             this->meta_ == Metadata::ARITHMETIC_WITH_LITERAL_LEFT;
    }
    bool right_column() const {
      return this->meta_ == Metadata::ARITHMETIC_WITH_COLUMN ||
             this->meta_ == Metadata::ARITHMETIC_WITH_LITERAL_RIGHT;
    }

    // Accessors.
    const std::string &getName() const { return this->name_; }
    ColumnID getColumn() const {
      uint64_t col = std::get<uint64_t>(this->left_);
      return static_cast<ColumnID>(col);
    }
    const Record::DataVariant &getLiteral() const { return this->left_; }
    ColumnID getLeftColumn() const { return this->getColumn(); }
    const Record::DataVariant &getLeftLiteral() const {
      return this->getLiteral();
    }
    ColumnID getRightColumn() const {
      uint64_t col = std::get<uint64_t>(this->right_);
      return static_cast<ColumnID>(col);
    }
    const Record::DataVariant &getRightLiteral() const { return this->right_; }
    Operation getOperation() const { return this->op_; }

   private:
    // The name of the target projected column.
    std::string name_;
    // The left (or only if unary) operand in the projection.
    // May be a literal or column.
    Record::DataVariant left_;
    // The right operand (only exists when binary projection).
    // May be a literal or a column.
    Record::DataVariant right_;
    // Specifes the type of projection (e.g. column, literal, or arithmetic)
    // this including whether left_ and right_ are literals or columns.
    Metadata meta_;
    // Arithmetic operation (+ or -) if applicable or NONE if no arithmetic
    // operation is required.
    Operation op_;
  };

  std::vector<ProjectionOperation> projections_;

  // Allow tests to use .Process(...) directly.
  FRIEND_TEST(ProjectOperatorTest, BatchTestColumn);
  FRIEND_TEST(ProjectOperatorTest, BatchTestLiteral);
  FRIEND_TEST(ProjectOperatorTest, BatchTestOperationRightColumn);
  FRIEND_TEST(ProjectOperatorTest, BatchTestOperationRightLiteral);
  FRIEND_TEST(ProjectOperatorTest, OutputSchemaPrimaryKeyTest);
  FRIEND_TEST(ProjectOperatorTest, OutputSchemaCompositeKeyTest);
  FRIEND_TEST(ProjectOperatorTest, NullValueTest);
  FRIEND_TEST(ProjectOperatorTest, ArithmeticAndNullValueTest);
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_PROJECT_H_
