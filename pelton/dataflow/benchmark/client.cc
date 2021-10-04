#include "pelton/dataflow/benchmark/client.h"

#include <utility>

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/ints.h"

namespace pelton {
namespace dataflow {

void Client::Start(std::vector<std::vector<Record>> &&batches) {
  switch (this->graph_type_) {
    case utils::GraphType::FILTER_GRAPH:
    case utils::GraphType::AGGREGATE_GRAPH_WITHOUT_EXCHANGE:
    case utils::GraphType::IDENTITY_GRAPH:
      CHECK_EQ(this->input_names_.size(), (size_t)1);
      for (size_t i = 0; i < batches.size(); i++) {
        this->dataflow_engine_->ProcessRecords(this->input_names_.at(0),
                                               std::move(batches.at(i)));
      }
      break;
    case utils::GraphType::EQUIJOIN_GRAPH_WITH_EXCHANGE:
    case utils::GraphType::EQUIJOIN_GRAPH_WITHOUT_EXCHANGE: {
      CHECK_EQ(this->input_names_.size(), (size_t)2);
      for (size_t i = 0; i < batches.size(); i++) {
        // The dataflow is already primed with the left batches by the driver,
        // the supplied batches are meant for the right input operator.
        this->dataflow_engine_->ProcessRecords(this->input_names_.at(1),
                                               std::move(batches.at(i)));
      }
    } break;
    default:
      LOG(FATAL) << "Graph not supported by benchmark client";
  }
}

void Client::Start(std::vector<std::vector<Record>> &&left_batches,
                   std::vector<std::vector<Record>> &&right_batches) {
  switch (this->graph_type_) {
    // case utils::GraphType::EQUIJOIN_GRAPH_WITH_EXCHANGE:
    // case utils::GraphType::EQUIJOIN_GRAPH_WITHOUT_EXCHANGE: {
    //   CHECK_EQ(this->input_names_.size(), (size_t)2);
    //   // The following check is specific to this graph and the way records
    //   // are being constructed by the benchmark.
    //   CHECK_EQ(left_batches.size(), right_batches.size());
    //   for (size_t i = 0; i < left_batches.size(); i++) {
    //     this->dataflow_engine_->ProcessRecords(this->input_names_.at(0),
    //                                            std::move(left_batches.at(i)));
    //     this->dataflow_engine_->ProcessRecords(this->input_names_.at(1),
    //                                            std::move(right_batches.at(i)));
    //   }
    // } break;
    default:
      LOG(FATAL) << "Graph not supported by client";
  }
}

}  // namespace dataflow
}  // namespace pelton
