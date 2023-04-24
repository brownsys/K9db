#ifndef K9DB_DATAFLOW_OPS_PROJECT_H_
#define K9DB_DATAFLOW_OPS_PROJECT_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
// NOLINTNEXTLINE
#include <variant>
#include <vector>

#include "gtest/gtest_prod.h"
#include "k9db/dataflow/operator.h"
#include "k9db/dataflow/ops/project_enum.h"
#include "k9db/dataflow/record.h"
#include "k9db/dataflow/types.h"
#include "k9db/sqlast/ast.h"

namespace k9db {
namespace dataflow {

class ProjectOperator : public Operator {
 public:
  using Operation = ProjectOperationEnum;
  using Metadata = ProjectMetadataEnum;
  // Cannot copy an operator.
  ProjectOperator(const ProjectOperator &other) = delete;
  ProjectOperator &operator=(const ProjectOperator &other) = delete;

  ProjectOperator() : Operator(Operator::Type::PROJECT), projections_() {}

  // Projecting a column.
  void AddColumnProjection(const std::string &name, ColumnID cid) {
    this->projections_.emplace_back(name, cid);
  }

  // Projecting a literal.
  void AddLiteralProjection(const std::string &name, uint64_t literal) {
    this->projections_.emplace_back(name, sqlast::Value(literal));
  }
  void AddLiteralProjection(const std::string &name, int64_t literal) {
    this->projections_.emplace_back(name, sqlast::Value(literal));
  }
  void AddLiteralProjection(const std::string &name, const std::string &lit) {
    this->projections_.emplace_back(name, sqlast::Value(lit));
  }

  // Projecting an arithmetic operation with two columns.
  void AddArithmeticProjectionColumns(const std::string &name, Operation op,
                                      ColumnID l, ColumnID r) {
    this->projections_.emplace_back(name, op, l, r);
  }

  // Projecting an arithmetic operation with a literal (on the left) and a col.
  void AddArithmeticProjectionLiteralLeft(const std::string &name, Operation op,
                                          uint64_t l, ColumnID r) {
    this->projections_.emplace_back(name, op, sqlast::Value(l), r);
  }
  void AddArithmeticProjectionLiteralLeft(const std::string &name, Operation op,
                                          int64_t l, ColumnID r) {
    this->projections_.emplace_back(name, op, sqlast::Value(l), r);
  }

  // Projecting an arithmetic operation with a col and a literal (on the right).
  void AddArithmeticProjectionLiteralRight(const std::string &name, Operation o,
                                           ColumnID l, uint64_t r) {
    this->projections_.emplace_back(name, o, l, sqlast::Value(r));
  }
  void AddArithmeticProjectionLiteralRight(const std::string &name, Operation o,
                                           ColumnID l, int64_t r) {
    this->projections_.emplace_back(name, o, l, sqlast::Value(r));
  }

  std::optional<ColumnID> ProjectColumn(ColumnID col) const;
  std::optional<ColumnID> UnprojectColumn(ColumnID col) const;

 protected:
  std::vector<Record> Process(NodeIndex source, std::vector<Record> &&records,
                              const Promise &promise) override;

  void ComputeOutputSchema() override;

  std::unique_ptr<Operator> Clone() const override;

 private:
  class ProjectionOperation {
   public:
    // Project column.
    ProjectionOperation(const std::string &name, ColumnID cid)
        : name_(name),
          left_(static_cast<uint64_t>(cid)),
          right_(),
          meta_(Metadata::COLUMN),
          op_(Operation::NONE) {}

    // Project literal.
    ProjectionOperation(const std::string &name, sqlast::Value &&literal)
        : name_(name),
          left_(std::move(literal)),
          right_(),
          meta_(Metadata::LITERAL),
          op_(Operation::NONE) {}

    // Project arithmetic operation over two columns.
    ProjectionOperation(const std::string &name, Operation op, ColumnID left,
                        ColumnID right)
        : name_(name),
          left_(static_cast<uint64_t>(left)),
          right_(static_cast<uint64_t>(right)),
          meta_(Metadata::ARITHMETIC_WITH_COLUMN),
          op_(op) {
      CHECK_NE(op, Operation::NONE);
    }
    // Project arithmetic operation over a literal and a column.
    ProjectionOperation(const std::string &name, Operation op,
                        sqlast::Value &&left, ColumnID right)
        : name_(name),
          left_(std::move(left)),
          right_(static_cast<uint64_t>(right)),
          meta_(Metadata::ARITHMETIC_WITH_LITERAL_LEFT),
          op_(op) {
      CHECK_NE(op, Operation::NONE);
    }
    ProjectionOperation(const std::string &name, Operation op, ColumnID left,
                        sqlast::Value &&right)
        : name_(name),
          left_(static_cast<uint64_t>(left)),
          right_(std::move(right)),
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
             this->meta_ == Metadata::ARITHMETIC_WITH_LITERAL_RIGHT;
    }
    bool right_column() const {
      return this->meta_ == Metadata::ARITHMETIC_WITH_COLUMN ||
             this->meta_ == Metadata::ARITHMETIC_WITH_LITERAL_LEFT;
    }

    // Accessors.
    const std::string &getName() const { return this->name_; }
    ColumnID getLeftColumn() const { return this->left_.GetUInt(); }
    const sqlast::Value &getLeftLiteral() const { return this->left_; }
    ColumnID getRightColumn() const { return this->right_->GetUInt(); }
    const sqlast::Value &getRightLiteral() const { return *this->right_; }
    Operation getOperation() const { return this->op_; }

   private:
    // The name of the target projected column.
    std::string name_;
    // The left (or only, if unary) operand in the projection.
    // May be a literal or column.
    sqlast::Value left_;
    // The right operand (only exists when binary projection).
    // May be a literal or a column.
    std::optional<sqlast::Value> right_;
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
}  // namespace k9db

#endif  // K9DB_DATAFLOW_OPS_PROJECT_H_
