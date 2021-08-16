#ifndef PELTON_DATAFLOW_BENCHMARK_WORKER_H_
#define PELTON_DATAFLOW_BENCHMARK_WORKER_H_

#include <memory>
#include <random>
#include <string>
#include <vector>

#include "pelton/dataflow/benchmark/utils.h"
#include "pelton/dataflow/engine.h"

namespace pelton {
namespace dataflow {

class Worker {
 public:
  Worker(uint64_t index, std::shared_ptr<DataFlowEngine> dataflow_engine,
         utils::GraphType graph_type, std::vector<TableName> input_names)
      : index_(index),
        dataflow_engine_(dataflow_engine),
        graph_type_(graph_type),
        /*batches_(std::move(batches)),*/ input_names_(input_names) {}
  // Entry point for the thread that this worker is launched in.
  void Start(std::vector<std::vector<Record>> &&batches);
  void Start(std::vector<std::vector<Record>> &&left_batches,
             std::vector<std::vector<Record>> &&right_batches);

 private:
  uint64_t index_;
  std::shared_ptr<DataFlowEngine> dataflow_engine_;
  utils::GraphType graph_type_;
  std::vector<TableName> input_names_;

  // Helper methods used to dispatch records that are specific to graph_type_.
  // void DispatchFilterRecords(std::vector<std::vector<Record>> &&batches);
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_BENCHMARK_WORKER_H_
