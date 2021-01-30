#ifndef PELTON_DATAFLOW_OPERATOR_H_
#define PELTON_DATAFLOW_OPERATOR_H_

#include <memory>

#include "dataflow/edge.h"
#include "dataflow/record.h"
#include "dataflow/types.h"

namespace dataflow {

class Edge;
class DataFlowGraph;

enum OperatorType { INPUT, IDENTITY, MAT_VIEW, FILTER, UNION, EQUIJOIN };

class Operator {
 public:
  Operator() : graph_(nullptr) {}
  Operator(const Operator& other) = delete;
  Operator& operator=(const Operator& other) = delete;

  virtual ~Operator() { graph_ = nullptr; }

  void AddParent(std::shared_ptr<Operator> parent, std::shared_ptr<Edge> edge);

  virtual bool process(NodeIndex src_op_idx, std::vector<Record>& rs,
                       std::vector<Record>& out_rs) {
    return process(rs, out_rs);
  }
  virtual bool process(std::vector<Record>& rs,
                       std::vector<Record>& out_rs) = 0;
  bool ProcessAndForward(NodeIndex src_op_idx, std::vector<Record>& rs);

  virtual OperatorType type() const = 0;
  NodeIndex index() const { return index_; }
  void set_index(NodeIndex index) { index_ = index; }
  DataFlowGraph* graph() const { return graph_; }
  void set_graph(DataFlowGraph* graph) { graph_ = graph; }

  std::vector<std::shared_ptr<Operator>> parents() const;

 private:
  NodeIndex index_;
  std::vector<std::weak_ptr<Edge>> children_;
  std::vector<std::shared_ptr<Edge>> parents_;
  DataFlowGraph* graph_;  // to which graph the operator belongs to.
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_OPERATOR_H_
