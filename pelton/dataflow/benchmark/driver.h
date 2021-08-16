#ifndef PELTON_DATAFLOW_BENCHMARK_DRIVER_H_
#define PELTON_DATAFLOW_BENCHMARK_DRIVER_H_

#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "pelton/dataflow/benchmark/utils.h"
#include "pelton/dataflow/engine.h"

namespace pelton {
namespace dataflow {

class Driver {
 public:
  Driver(utils::GraphType graph_type, uint64_t num_batches, uint64_t batch_size,
         uint64_t num_workers, PartitionID num_partitions)
      : graph_type_(graph_type),
        num_batches_(num_batches),
        batch_size_(batch_size),
        num_workers_(num_workers),
        num_partitions_(num_partitions) {
    LOG(INFO) << "driver constructor";
    this->InitializeEngine();
  }
  void Execute();
  void InitializeEngine();
  ~Driver();

 private:
  // The engine is expected to be initialized with a single supported flow
  // (as specified in utils::GraphType)
  std::shared_ptr<DataFlowEngine> dataflow_engine_;
  utils::GraphType graph_type_;
  std::vector<TableName> input_names_;
  uint64_t num_batches_;
  uint64_t batch_size_;
  uint64_t num_workers_;
  uint64_t num_partitions_;
  // Just an object to store worker threads
  std::vector<std::thread> threads_;

  absl::flat_hash_map<uint64_t, std::vector<std::vector<Record>>>
  PrepareWorkerBatches(std::vector<std::vector<Record>> &&all_batches);
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_BENCHMARK_DRIVER_H_
