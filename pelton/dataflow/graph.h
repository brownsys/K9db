#ifndef PELTON_DATAFLOW_GRAPH_H_
#define PELTON_DATAFLOW_GRAPH_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "gtest/gtest_prod.h"
#include "pelton/dataflow/graph_partition.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

// A graph is made out of many partitions.
class DataFlowGraph {
 public:
  DataFlowGraph() = default;

  void Initialize(std::unique_ptr<DataFlowGraphPartition> &&partition,
                  PartitionIndex partitions);

  void Process(const std::string &input_name, std::vector<Record> &&records);

  const std::vector<std::string> &Inputs() const { return this->inputs_; }

 private:
  std::vector<std::string> inputs_;
  std::vector<std::unique_ptr<DataFlowGraphPartition>> partitions_;
  // Maps each input name to the keys that partition records fed to that input.
  std::unordered_map<std::string, PartitionKey> inkeys_;
  // Maps each matview to the keys that partition records read from that view.
  std::vector<PartitionKey> outkeys_;

  // Unit tests need to look inside partitions_, inkeys_, and outkeys_.
  FRIEND_TEST(DataFlowGraphTest, JoinAggregateFunctionality);
  FRIEND_TEST(DataFlowGraphTest, JoinAggregateExchangeFunctionality);
  FRIEND_TEST(DataFlowGraphTest, TrivialGraphNoKey);
  FRIEND_TEST(DataFlowGraphTest, TrivialGraphWithKey);
  FRIEND_TEST(DataFlowGraphTest, TrivialFilterGraph);
  FRIEND_TEST(DataFlowGraphTest, TrivialUnionGraphWithKey);
  FRIEND_TEST(DataFlowGraphTest, TrivialUnionGraphWithNoKey);
  FRIEND_TEST(DataFlowGraphTest, AggregateGraphWithKey);
  FRIEND_TEST(DataFlowGraphTest, AggregateGraphSameKey);
  FRIEND_TEST(DataFlowGraphTest, AggregateGraphNoKey);
  FRIEND_TEST(DataFlowGraphTest, JoinGraphWithKey);
  FRIEND_TEST(DataFlowGraphTest, JoinGraphSameKey);
  FRIEND_TEST(DataFlowGraphTest, JoinGraphNoKey);
  FRIEND_TEST(DataFlowGraphTest, ReorderingProjectionWithKey);
  FRIEND_TEST(DataFlowGraphTest, ReorderingProjectionNoKey);
  FRIEND_TEST(DataFlowGraphTest, ChangingProjectionWithKey);
  FRIEND_TEST(DataFlowGraphTest, ChangingProjectionNoKey);
  FRIEND_TEST(DataFlowGraphTest, JoinReorderProjectWithKey);
  FRIEND_TEST(DataFlowGraphTest, JoinReorderProjectNoKey);
  FRIEND_TEST(DataFlowGraphTest, JoinProjectDroppedKeyWithKey);
  FRIEND_TEST(DataFlowGraphTest, JoinProjectDroppedKeyNoKey);
  FRIEND_TEST(DataFlowGraphTest, JoinAggregateKey);
  FRIEND_TEST(DataFlowGraphTest, JoinAggregateUnion);
  FRIEND_TEST(DataFlowGraphTest, UnionJoinAggregateUnionReorderProject);
  FRIEND_TEST(DataFlowGraphTest, UnionJoinAggregateUnionDroppingProject);
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_GRAPH_H_
