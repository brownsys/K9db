#include "dataflow/graph.h"
#include "dataflow/operator.h"
#include "dataflow/ops/identity.h"

#include <memory>

#include "gtest/gtest.h"

namespace dataflow {

TEST(DataFlowGraphTest, Basic) {
  // TODO
  DataFlowGraph g;

  g.AddNode(OperatorType::IDENTITY, std::make_shared<IdentityOperator>());
}

}  // namespace dataflow
