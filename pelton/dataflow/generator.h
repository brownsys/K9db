// A graph generator is used to build a DataFlowGraph incrementally.
// This is used by the planner (Java with calcite) to build a DataFlowGraph
// using our C++ class structure directly without going through intermediates.
#ifndef PELTON_DATAFLOW_GENERATOR_H_
#define PELTON_DATAFLOW_GENERATOR_H_

#include <string>

namespace pelton {
namespace dataflow {

class DataFlowGraphGenerator {
 public:
  DataFlowGraphGenerator();
  ~DataFlowGraphGenerator();

  int AddInputNode(const std::string &name);

 private:
  void* graph_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_GENERATOR_H_
