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
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class ProjectOperator : public Operator {
 public:
  explicit ProjectOperator(std::vector<ColumnID> &cids)
      : Operator(Operator::Type::PROJECT),
        cids_(cids),
        owned_output_schema_(SchemaOwner()) {}

 protected:
  bool Process(NodeIndex source, const std::vector<Record> &records,
               std::vector<Record> *output) override;
  void ComputeOutputSchema() override;

 private:
  const std::vector<ColumnID> cids_;
  SchemaOwner owned_output_schema_;
  // Allow tests to use .Process(...) directly.
  FRIEND_TEST(ProjectOperatorTest, BatchTest);
  FRIEND_TEST(ProjectOperatorTest, OutputSchemaPrimaryKeyTest);
  FRIEND_TEST(ProjectOperatorTest, OutputSchemaCompositeKeyTest);
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_PROJECT_H_
