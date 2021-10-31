#ifndef PELTON_DATAFLOW_GRAPH_H_
#define PELTON_DATAFLOW_GRAPH_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

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
  std::unordered_map<std::string, std::vector<ColumnID>> partition_keys_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_GRAPH_H_
