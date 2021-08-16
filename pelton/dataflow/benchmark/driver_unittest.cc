#include "pelton/dataflow/benchmark/driver.h"

#include <memory>

#include "gtest/gtest.h"
#include "pelton/dataflow/benchmark/utils.h"

namespace pelton {
namespace dataflow {

TEST(DriverTest, BasicTest) {
  Driver driver(utils::GraphType::FILTER_GRAPH, 300000, 1, 3, 3);
  // Driver driver(utils::GraphType::EQUIJOIN_GRAPH_WITH_EXCHANGE, 300000, 1, 3,
  // 3);
  // Driver driver(utils::GraphType::EQUIJOIN_GRAPH_WITHOUT_EXCHANGE,
  // 300000, 1, 3, 3);
  driver.Execute();
}

}  // namespace dataflow
}  // namespace pelton
