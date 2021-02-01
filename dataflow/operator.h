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

  /*!
   * push a batch of records through the dataflow graph. Order within this batch
   * does not matter. i.e. an operator can reorder individual records to
   * optimize processing within a single operator node. The dataflow assumes all
   * records of this batch arrive at the same time. Records will NOT be pushed
   * to children.
   * @param src_op_idx index of the node to which the input batch rs belongs to.
   * @param rs an input target vector which gets consumed (i.e. all elements
   * removed) ??? ???
   * @param out_rs target vector where to write outputs to.
   * @return true when processing succeeded, false when an error occurred ???
   * ???
   */
  virtual bool process(NodeIndex src_op_idx, std::vector<Record>& rs,
                       std::vector<Record>& out_rs) {
    return process(rs, out_rs);
  }

  virtual bool process(std::vector<Record>& rs,
                       std::vector<Record>& out_rs) = 0;

  /*!
   * Take a batch of records belonging to operator src_op_idx and push it
   * through the dataflow graph. I.e., the batch will be processed through this
   * node and then pushed to all children nodes for further processing.
   * Internally calls process(...) function.
   * @param src_op_idx
   * @param rs
   * @return
   */
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
