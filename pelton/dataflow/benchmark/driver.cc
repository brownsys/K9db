#include "pelton/dataflow/benchmark/driver.h"

#include <math.h>

#include <algorithm>
#include <chrono>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "pelton/dataflow/benchmark/worker.h"

namespace pelton {
namespace dataflow {

// Evenly distributes all batches across the workers.
absl::flat_hash_map<uint64_t, std::vector<std::vector<Record>>>
Driver::PrepareWorkerBatches(std::vector<std::vector<Record>> &&all_batches) {
  // Shuffle batches
  std::random_shuffle(all_batches.begin(), all_batches.end());
  // Evenly distribute batches across the workers
  absl::flat_hash_map<uint64_t, std::vector<std::vector<Record>>>
      worker_batches;
  uint64_t batches_per_worker =
      (uint64_t)floor(this->num_batches_ / this->num_workers_);
  uint64_t read_index = 0;
  for (uint64_t i = 0; i < this->num_workers_; i++) {
    std::vector<std::vector<Record>> temp;
    temp.insert(temp.end(),
                std::make_move_iterator(all_batches.begin() + read_index),
                std::make_move_iterator(all_batches.begin() + read_index +
                                        batches_per_worker));
    worker_batches.emplace(i, std::move(temp));
    read_index += batches_per_worker;
  }
  return worker_batches;
}

void Driver::Execute() {
  switch (this->graph_type_) {
    case utils::GraphType::FILTER_GRAPH: {
      std::vector<std::vector<Record>> batches =
          utils::MakeFilterBatches(this->num_batches_, this->batch_size_);
      auto worker_batches = this->PrepareWorkerBatches(std::move(batches));
      // Select appropriate overloaded function
      void (Worker::*memfunc)(std::vector<std::vector<Record>> &&) =
          &Worker::Start;
      auto time_point = std::chrono::system_clock::now();
      std::cout << "Start: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                       time_point.time_since_epoch())
                       .count()
                << std::endl;
      for (uint64_t i = 0; i < this->num_workers_; i++) {
        Worker worker(i, this->dataflow_engine_, this->graph_type_,
                      this->input_names_);
        this->threads_.emplace_back(
            std::thread(memfunc, worker, std::move(worker_batches.at(i))));
      }
    } break;
    case utils::GraphType::EQUIJOIN_GRAPH_WITH_EXCHANGE:
    case utils::GraphType::EQUIJOIN_GRAPH_WITHOUT_EXCHANGE: {
      std::vector<std::vector<Record>> left_batches =
          utils::MakeEquiJoinLeftBatches(this->num_batches_, this->batch_size_);
      std::vector<std::vector<Record>> right_batches =
          utils::MakeEquiJoinRightBatches(this->num_batches_,
                                          this->batch_size_);
      auto worker_batches_left =
          this->PrepareWorkerBatches(std::move(left_batches));
      auto worker_batches_right =
          this->PrepareWorkerBatches(std::move(right_batches));
      // Select appropriate overloaded function
      void (Worker::*memfunc)(std::vector<std::vector<Record>> &&,
                              std::vector<std::vector<Record>> &&) =
          &Worker::Start;
      auto time_point = std::chrono::system_clock::now();
      std::cout << "Start: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                       time_point.time_since_epoch())
                       .count()
                << std::endl;
      for (uint64_t i = 0; i < this->num_workers_; i++) {
        Worker worker(i, this->dataflow_engine_, this->graph_type_,
                      this->input_names_);
        this->threads_.emplace_back(
            std::thread(memfunc, worker, std::move(worker_batches_left.at(i)),
                        std::move(worker_batches_right.at(i))));
      }
    } break;
    default:
      LOG(FATAL) << "Unsupported graph type";
  }
}

void Driver::InitializeEngine() {
  switch (this->graph_type_) {
    case utils::GraphType::FILTER_GRAPH:
      LOG(INFO) << "User supplied filter grap";
      {
        this->dataflow_engine_ =
            std::make_shared<DataFlowEngine>(this->num_partitions_);
        // Make schema
        SchemaRef schema = utils::MakeFilterSchema();
        this->dataflow_engine_->AddTableSchema("test-table", schema);
        this->input_names_ = std::vector<TableName>{"test-table"};
        // Make graph
        auto graph = utils::MakeFilterGraph(0, schema);
        std::string flow_name = "flowX";
        this->dataflow_engine_->AddFlow(flow_name, graph);
        // Set markers in all matviews
        // For the filter graph, records are constructed such that, all records
        // will be distributed evenly across all partitions.
        uint64_t records_per_partition = (uint64_t)floor(
            (this->num_batches_ * this->batch_size_) / this->num_partitions_);
        for (auto matview :
             this->dataflow_engine_->GetPartitionedMatViews(flow_name)) {
          matview->SetMarker(records_per_partition);
        }
      }
      LOG(INFO) << "Initialized engine";
      break;
    case utils::GraphType::EQUIJOIN_GRAPH_WITH_EXCHANGE:
    case utils::GraphType::EQUIJOIN_GRAPH_WITHOUT_EXCHANGE: {
      this->dataflow_engine_ =
          std::make_shared<DataFlowEngine>(this->num_partitions_);
      SchemaRef left_schema = utils::MakeEquiJoinLeftSchema();
      SchemaRef right_schema = utils::MakeEquiJoinRightSchema();
      this->dataflow_engine_->AddTableSchema("test-table1", left_schema);
      this->dataflow_engine_->AddTableSchema("test-table2", right_schema);
      this->input_names_ = std::vector<TableName>{"test-table1", "test-table2"};
      // Make graph
      std::shared_ptr<DataFlowGraph> graph;
      if (this->graph_type_ == utils::GraphType::EQUIJOIN_GRAPH_WITH_EXCHANGE) {
        graph = utils::MakeEquiJoinGraph(3, 1, 1, left_schema, right_schema);
      } else {
        graph = utils::MakeEquiJoinGraph(1, 1, 1, left_schema, right_schema);
      }
      std::string flow_name = "flowX";
      this->dataflow_engine_->AddFlow(flow_name, graph);
      // Set markers in all matviews
      // For the filter graph, records are constructed such that, all records
      // will be distributed evenly across all partitions.
      uint64_t records_per_partition = (uint64_t)floor(
          (this->num_batches_ * this->batch_size_) / this->num_partitions_);
      for (auto matview :
           this->dataflow_engine_->GetPartitionedMatViews(flow_name)) {
        matview->SetMarker(records_per_partition);
      }
    } break;
    default:
      LOG(FATAL) << "Unsupported graph type in dataflow benchmark";
  }
}

Driver::~Driver() {
  // Wait for all threads to terminate
  for (auto &thread : this->threads_) {
    thread.join();
  }
}

}  // namespace dataflow
}  // namespace pelton
