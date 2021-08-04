#ifndef PELTON_DATAFLOW_OPS_IDENTITY_H_
#define PELTON_DATAFLOW_OPS_IDENTITY_H_

#include <vector>
#include <memory>

#include "gtest/gtest_prod.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class IdentityOperator : public Operator {
 public:
  IdentityOperator() : Operator(Operator::Type::IDENTITY) {}

  std::optional<std::vector<Record>> Process(
      NodeIndex source, const std::vector<Record> &records) override;
  std::shared_ptr<Operator> Clone() const override;
 protected:
  void ComputeOutputSchema() override;

  FRIEND_TEST(EquiJoinOperatorTest, BasicJoinTest);
  FRIEND_TEST(EquiJoinOperatorTest, BasicUnjoinableTest);
  FRIEND_TEST(EquiJoinOperatorTest, FullJoinTest);
  FRIEND_TEST(EquiJoinOperatorTest, BasicLeftJoinTest);
  FRIEND_TEST(EquiJoinOperatorTest, BasicRightJoinTest);
  FRIEND_TEST(EquiJoinOperatorTest, LeftJoinTest);
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_IDENTITY_H_
