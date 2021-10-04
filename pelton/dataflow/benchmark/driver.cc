#include "pelton/dataflow/benchmark/driver.h"

#include <math.h>

#include <algorithm>
#include <chrono>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "pelton/dataflow/benchmark/client.h"

namespace pelton {
namespace dataflow {

// Evenly distributes all batches across the clients.
absl::flat_hash_map<uint64_t, std::vector<std::vector<Record>>>
Driver::PrepareClientBatches(std::vector<std::vector<Record>> &&all_batches) {
  // Shuffle batches
  std::random_shuffle(all_batches.begin(), all_batches.end());
  // Evenly distribute batches across the clients
  absl::flat_hash_map<uint64_t, std::vector<std::vector<Record>>>
      client_batches;
  uint64_t batches_per_client =
      (uint64_t)floor(this->num_batches_ / this->num_clients_);
  uint64_t read_index = 0;
  for (uint64_t i = 0; i < this->num_clients_; i++) {
    std::vector<std::vector<Record>> temp;
    temp.insert(temp.end(),
                std::make_move_iterator(all_batches.begin() + read_index),
                std::make_move_iterator(all_batches.begin() + read_index +
                                        batches_per_client));
    client_batches.emplace(i, std::move(temp));
    read_index += batches_per_client;
  }
  // Move any remaining batches as well. Batches may remain because
  // #batches % #clients may not be zero.
  client_batches.at(this->num_clients_ - 1)
      .insert(client_batches.at(this->num_clients_ - 1).end(),
              std::make_move_iterator(all_batches.begin() + read_index),
              std::make_move_iterator(all_batches.end()));

  return client_batches;
}

void Driver::Execute() {
  if (this->input_names_.size() == 2) {
    std::vector<std::vector<Record>> left_batches;
    std::vector<std::vector<Record>> right_batches;
    switch (this->graph_type_) {
      case utils::GraphType::EQUIJOIN_GRAPH_WITHOUT_EXCHANGE:
      case utils::GraphType::EQUIJOIN_GRAPH_WITH_EXCHANGE:
        left_batches = utils::MakeEquiJoinLeftBatches(this->num_batches_,
                                                      this->batch_size_);
        right_batches = utils::MakeEquiJoinRightBatches(this->num_batches_,
                                                        this->batch_size_);
        break;
      default:
        LOG(FATAL) << "Unsupported graph type";
    }
    auto client_batches_right =
        this->PrepareClientBatches(std::move(right_batches));
    // Prime the partitions with left batches
    for (size_t i = 0; i < left_batches.size(); i++) {
      this->dataflow_engine_->PrimeDataFlow(this->input_names_.at(0),
                                            std::move(left_batches.at(i)));
    }
    // Select appropriate overloaded function
    void (Client::*memfunc)(std::vector<std::vector<Record>> &&) =
        &Client::Start;
    auto time_point = std::chrono::system_clock::now();
    std::cout << "Start: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     time_point.time_since_epoch())
                     .count()
              << std::endl;
    for (uint64_t i = 0; i < this->num_clients_; i++) {
      Client client(i, this->dataflow_engine_, this->graph_type_,
                    this->input_names_);
      this->threads_.emplace_back(
          std::thread(memfunc, client, std::move(client_batches_right.at(i))));
    }
  } else {
    std::vector<std::vector<Record>> batches;
    switch (this->graph_type_) {
      case utils::GraphType::FILTER_GRAPH:
        batches =
            utils::MakeFilterBatches(this->num_batches_, this->batch_size_);
        break;
      case utils::GraphType::AGGREGATE_GRAPH_WITHOUT_EXCHANGE:
        batches =
            utils::MakeAggregateBatches(this->num_batches_, this->batch_size_);
        break;
      case utils::GraphType::IDENTITY_GRAPH:
        batches =
            utils::MakeIdentityBatches(this->num_batches_, this->batch_size_);
        break;
      default:
        LOG(FATAL) << "Unsupported graph type";
    }
    auto client_batches = this->PrepareClientBatches(std::move(batches));
    // Select appropriate overloaded function
    void (Client::*memfunc)(std::vector<std::vector<Record>> &&) =
        &Client::Start;
    auto time_point = std::chrono::system_clock::now();
    std::cout << "Start: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     time_point.time_since_epoch())
                     .count()
              << std::endl;
    for (uint64_t i = 0; i < this->num_clients_; i++) {
      Client client(i, this->dataflow_engine_, this->graph_type_,
                    this->input_names_);
      this->threads_.emplace_back(
          std::thread(memfunc, client, std::move(client_batches.at(i))));
    }
  }
}

void Driver::InitializeEngine() {
  this->dataflow_engine_ =
      std::make_shared<DataFlowEngine>(this->num_partitions_);
  std::shared_ptr<DataFlowGraph> graph;
  switch (this->graph_type_) {
    case utils::GraphType::FILTER_GRAPH: {
      // Make schema
      SchemaRef schema = utils::MakeFilterSchema();
      this->dataflow_engine_->AddTableSchema("test-table", schema);
      this->input_names_ = std::vector<TableName>{"test-table"};
      // Make graph
      graph = utils::MakeFilterGraph(0, schema);
    } break;
    case utils::GraphType::EQUIJOIN_GRAPH_WITH_EXCHANGE:
    case utils::GraphType::EQUIJOIN_GRAPH_WITHOUT_EXCHANGE: {
      SchemaRef left_schema = utils::MakeEquiJoinLeftSchema();
      SchemaRef right_schema = utils::MakeEquiJoinRightSchema();
      this->dataflow_engine_->AddTableSchema("test-table1", left_schema);
      this->dataflow_engine_->AddTableSchema("test-table2", right_schema);
      this->input_names_ = std::vector<TableName>{"test-table1", "test-table2"};
      // Make graph
      if (this->graph_type_ == utils::GraphType::EQUIJOIN_GRAPH_WITH_EXCHANGE) {
        graph = utils::MakeEquiJoinGraph(3, 1, 1, left_schema, right_schema);
      } else {
        graph = utils::MakeEquiJoinGraph(1, 1, 1, left_schema, right_schema);
      }
    } break;
    case utils::GraphType::AGGREGATE_GRAPH_WITHOUT_EXCHANGE: {
      // Make schema
      SchemaRef schema = utils::MakeAggregateSchema();
      this->dataflow_engine_->AddTableSchema("test-table", schema);
      this->input_names_ = std::vector<TableName>{"test-table"};
      // Make graph
      graph = utils::MakeAggregateGraph(0, schema, std::vector<ColumnID>{1});
    } break;
    case utils::GraphType::IDENTITY_GRAPH: {
      // Make schema
      SchemaRef schema = utils::MakeIdentitySchema();
      this->dataflow_engine_->AddTableSchema("test-table", schema);
      this->input_names_ = std::vector<TableName>{"test-table"};
      // Make graph
      graph = utils::MakeIdentityGraph(0, schema);
    } break;
    default:
      LOG(FATAL) << "Unsupported graph type in dataflow benchmark";
  }
  // Add flow
  this->dataflow_engine_->AddFlow(this->flow_name, graph);
  // Set markers in all matviews
  // All graphs and input records are constructed such that at the end, all
  // records are distributed evenly across all partitions.
  // NOTE: For equijoin graphs, the dataflow is primed on the left batches
  // and is benchmarked on the right batches.
  uint64_t records_per_partition = (uint64_t)floor(
      (this->num_batches_ * this->batch_size_) / this->num_partitions_);
  for (auto matview :
       this->dataflow_engine_->GetPartitionedMatViews(this->flow_name)) {
    matview->SetMarker(records_per_partition);
  }
}

Driver::~Driver() {
  // Wait for all client threads to terminate
  for (auto &thread : this->threads_) {
    thread.join();
  }
}

}  // namespace dataflow
}  // namespace pelton
