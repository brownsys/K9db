#include "pelton/dataflow/benchmark/worker.h"

#include <utility>

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/ints.h"

namespace pelton {
namespace dataflow {

void Worker::Start(std::vector<std::vector<Record>> &&batches) {
  switch (this->graph_type_) {
    case utils::GraphType::FILTER_GRAPH:
      CHECK_EQ(this->input_names_.size(), (size_t)1);
      for (size_t i = 0; i < batches.size(); i++) {
        this->dataflow_engine_->ProcessRecords(this->input_names_.at(0),
                                               std::move(batches.at(i)));
      }
      break;
    default:
      LOG(FATAL) << "Graph not supported by worker";
  }
}

void Worker::Start(std::vector<std::vector<Record>> &&left_batches,
                   std::vector<std::vector<Record>> &&right_batches) {
  switch (this->graph_type_) {
    case utils::GraphType::EQUIJOIN_GRAPH_WITH_EXCHANGE:
    case utils::GraphType::EQUIJOIN_GRAPH_WITHOUT_EXCHANGE: {
      CHECK_EQ(this->input_names_.size(), (size_t)2);
      // The following check is specific to this graph and the way records
      // are being constructed by the benchmark.
      CHECK_EQ(left_batches.size(), right_batches.size());
      for (size_t i = 0; i < left_batches.size(); i++) {
        this->dataflow_engine_->ProcessRecords(this->input_names_.at(0),
                                               std::move(left_batches.at(i)));
        this->dataflow_engine_->ProcessRecords(this->input_names_.at(1),
                                               std::move(right_batches.at(i)));
      }
    } break;
    default:
      LOG(FATAL) << "Graph not supported by worker";
  }
}

}  // namespace dataflow
}  // namespace pelton
