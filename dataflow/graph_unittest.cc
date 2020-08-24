#include "dataflow/graph.h"
#include "dataflow/operator.h"
#include "dataflow/ops/identity.h"
#include "dataflow/ops/input.h"
#include "dataflow/ops/matview.h"
#include "dataflow/record.h"

#include <memory>

#include "gtest/gtest.h"

namespace dataflow {

TEST(DataFlowGraphTest, Basic) {
  // TODO
  DataFlowGraph g;

  g.AddNode(OperatorType::INPUT, std::make_shared<InputOperator>());
  g.AddNode(OperatorType::IDENTITY, std::make_shared<IdentityOperator>());
  std::vector<ColumnID> keycol = {0};
  g.AddNode(OperatorType::MAT_VIEW, std::make_shared<MatViewOperator>(keycol));
}

}  // namespace dataflow
