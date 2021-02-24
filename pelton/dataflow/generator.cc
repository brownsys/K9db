#include "pelton/dataflow/generator.h"

#include <memory>

#include "glog/logging.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/schema.h"

namespace pelton {
namespace dataflow {

DataFlowGraphGenerator::DataFlowGraphGenerator() {
  this->graph_ = new DataFlowGraph();
}

DataFlowGraphGenerator::~DataFlowGraphGenerator() {
  delete static_cast<DataFlowGraph *>(this->graph_);
  this->graph_ = nullptr;
}

int DataFlowGraphGenerator::AddInputNode(const std::string &table_name) {
  SchemaOwner schema = SchemaOwner({}, {}, {});
  std::shared_ptr<InputOperator> input =
      std::make_shared<InputOperator>(table_name, SchemaRef(schema));
  CHECK(static_cast<DataFlowGraph *>(this->graph_)->AddInputNode(input));
  LOG(INFO) << "Add input to table " << table_name;
  return input->index();
}

}  // namespace dataflow
}  // namespace pelton
