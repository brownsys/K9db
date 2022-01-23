#ifndef PELTON_DATAFLOW_GRAPH_H_
#define PELTON_DATAFLOW_GRAPH_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "gtest/gtest_prod.h"
#include "pelton/dataflow/channel.h"
#include "pelton/dataflow/graph_partition.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/ops/matview.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

// A graph is made out of many partitions.
class DataFlowGraph {
 public:
  DataFlowGraph(const std::string &flow_name, PartitionIndex partitions)
      : flow_name_(flow_name), size_(partitions) {}

  void Initialize(std::unique_ptr<DataFlowGraphPartition> &&partition,
                  const std::vector<Channel *> &channels);

  // Not copyable or movable.
  DataFlowGraph(const DataFlowGraph &) = delete;
  DataFlowGraph(DataFlowGraph &&) = delete;
  DataFlowGraph &operator=(const DataFlowGraph &) = delete;
  DataFlowGraph &operator=(DataFlowGraph &&) = delete;

  // Accessors.
  const std::string &flow_name() const { return this->flow_name_; }
  const std::vector<std::string> &Inputs() const { return this->inputs_; }
  const SchemaRef &output_schema() const { return this->output_schema_; }
  const std::vector<ColumnID> &matview_keys() const {
    return this->matview_keys_;
  }
  const std::vector<std::string> &matview_key_names() const {
    return this->matviews_.at(0)->key_names();
  }
  const std::vector<sqlast::ColumnDefinition::Type> &matview_key_types() const {
    return this->matviews_.at(0)->key_types();
  }

  // Access partitioning keys.
  const PartitionKey &input_partition_key(const std::string &table) const {
    return this->inkeys_.at(table);
  }
  const PartitionKey &out_partition_key() const { return this->outkey_; }

  // Partitioning.
  DataFlowGraphPartition *GetPartition(PartitionIndex partition) const {
    return this->partitions_.at(partition).get();
  }
  std::unordered_map<PartitionIndex, std::vector<Record>> PartitionInputs(
      const std::string &input_name, std::vector<Record> &&records);

  // Read outputs.
  std::vector<Record> All(int limit, size_t offset) const;
  std::vector<Record> Lookup(const Key &key, int limit, size_t offset) const;
  std::vector<Record> Lookup(const std::vector<Key> &keys, int limit,
                             size_t offset) const;
  std::vector<Record> AllRecordGreater(const Record &cmp, int limit,
                                       size_t offset) const;
  std::vector<Record> LookupRecordGreater(const Key &key, const Record &cmp,
                                          int limit, size_t offset) const;
  std::vector<Record> LookupRecordGreater(const std::vector<Key> &keys,
                                          const Record &cmp, int limit,
                                          size_t offset) const;
  std::vector<Record> LookupKeyGreater(const Key &key, int limit,
                                       size_t offset) const;

  // Debugging information.
  uint64_t SizeInMemory(std::vector<Record> *output) const;
  std::vector<Record> DebugRecords() const;

 private:
  std::string flow_name_;
  PartitionIndex size_;
  // partitions and their inputs and outputs.
  std::vector<std::unique_ptr<DataFlowGraphPartition>> partitions_;
  std::vector<std::string> inputs_;
  std::vector<MatViewOperator *> matviews_;  // one per partition.
  // Maps each input name to the keys that partition records fed to that input.
  std::unordered_map<std::string, PartitionKey> inkeys_;
  // The partition key of the matview.
  PartitionKey outkey_;
  // The output schema.
  SchemaRef output_schema_;
  // The key that the underlying matviews use for lookup.
  // If the matviews have keys, this is guaranteed to equal outkey_, and
  // matview_partition_match_ will be true.
  // Otherwise, outkey_ will be equal to some non empty key, and this
  // will be empty.
  std::vector<ColumnID> matview_keys_;
  bool matview_partition_match_;

  // Unit tests need to look inside partitions_, inkeys_, and outkey_.
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
