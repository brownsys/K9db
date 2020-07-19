#include "dataflow/graph.h"
#include "dataflow/operator.h"
#include "dataflow/ops/identity.h"

#include <cstdint>

#include "gtest/gtest.h"

namespace dataflow {

TEST(DataFlowGraphTest, Basic) {
  // TODO
  DataFlowGraph g;

  IdentityOperator id_node;

  // g.AddNode(OperatorType::IDENTITY, &id_node);
}

}  // namespace dataflow
