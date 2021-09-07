#ifndef PELTON_DATAFLOW_WORKER_H_
#define PELTON_DATAFLOW_WORKER_H_

#include <condition_variable>
#include <memory>
#include <mutex>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "pelton/dataflow/channel.h"

namespace pelton {
namespace dataflow {

// Forward declarations
class DataFlowGraph;

class Worker {
 public:
  // Cannot copy a worker.
  Worker(const Worker &other) = delete;
  Worker &operator=(const Worker &other) = delete;

  explicit Worker(PartitionID partition_id,
                  std::shared_ptr<std::condition_variable> condition_variable)
      : partition_id_(partition_id), condition_variable_(condition_variable) {}

  bool MonitorChannel(std::shared_ptr<Channel> channel);
  bool AddPartitionedGraph(std::shared_ptr<DataFlowGraph> graph);
  void NotifyWorker();

  // Entry point for the thread that this worker is launched in.
  void Start();

  // Accessors
  const std::shared_ptr<std::condition_variable> condition_variable() {
    return this->condition_variable_;
  }

 private:
  // A worker is responsible for partition @partition_id_ of all flows
  // installed in the system.
  PartitionID partition_id_;
  // A condition variable that is notified when any of the channels this worker
  // is responsible for listening on, has input to read from.
  std::shared_ptr<std::condition_variable> condition_variable_;
  std::vector<std::shared_ptr<Channel>> chans_to_monitor_;
  // Contains partitioned graph for @partition_id_.
  absl::flat_hash_map<GraphIndex, std::shared_ptr<DataFlowGraph>> graphs_;
  // Keeps track of how many flows have been marked as stopped.
  // uint32_t stop_count_;
  // A mutex that allows the dataflow engine to safely modify worker's state.
  mutable std::mutex mtx_;
  // Variable that is meant to track of notifications even if the current worker
  // is busy in reading from channels. (protected with a fine grained mutex)
  bool notified_ = false;
  mutable std::mutex notification_mtx_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_WORKER_H_
