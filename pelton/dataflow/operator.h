#ifndef PELTON_DATAFLOW_OPERATOR_H_
#define PELTON_DATAFLOW_OPERATOR_H_

#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "pelton/dataflow/edge.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class Edge;
class DataFlowGraph;

#ifdef PELTON_BENCHMARK  // shuts up compiler warnings
static void JoinOneToOne(benchmark::State &state);
#endif

class Operator {
 public:
  enum class Type {
    INPUT,
    IDENTITY,
    MAT_VIEW,
    FILTER,
    UNION,
    EQUIJOIN,
    PROJECT,
    AGGREGATE,
  };

  // Cannot copy an operator.
  Operator(const Operator &other) = delete;
  Operator &operator=(const Operator &other) = delete;

  virtual ~Operator() { graph_ = nullptr; }

  /*!
   * Take a batch of records belonging to operator source and push it
   * through the dataflow graph. I.e., the batch will be processed through this
   * node and then pushed to all children nodes for further processing.
   * Internally calls process(...) function.
   * @param source
   * @param records
   * @return
   */
  virtual bool ProcessAndForward(NodeIndex source,
                                 const std::vector<Record> &records);
  // TODO(babman): we can have an optimized version for the case where this
  // operator is a single child of another operator. The records can then be
  // moved into ProcessAndForward, modified in place, and then moved/passed
  // by const ref to its child/children.

  Type type() const { return this->type_; }
  NodeIndex index() const { return this->index_; }
  DataFlowGraph *graph() const { return graph_; }
  const SchemaRef &output_schema() const { return this->output_schema_; }

  // Constructs a vector of parent operators from parents_ edge vector.
  std::vector<std::shared_ptr<Operator>> GetParents() const;

 protected:
  explicit Operator(Type type)
      : type_(type), index_(UNDEFINED_NODE_INDEX), graph_(nullptr) {}

  void SetGraph(DataFlowGraph *graph) { this->graph_ = graph; }
  void SetIndex(NodeIndex index) { this->index_ = index; }
  void AddParent(std::shared_ptr<Operator> parent, std::shared_ptr<Edge> edge);

  /*!
   * Push a batch of records through the dataflow graph. Order within this batch
   * does not matter. i.e. an operator can reorder individual records to
   * optimize processing within a single operator node. The dataflow assumes all
   * records of this batch arrive at the same time. Records will NOT be pushed
   * to children.
   * @param source the index of the source operator.
   * @param records an input target vector.
   * @param output target vector where to write outputs to.
   * @return true when processing succeeded, false when an error occurred.
   */
  virtual bool Process(NodeIndex source, const std::vector<Record> &records,
                       std::vector<Record> *output) = 0;

  /*!
   * Compute the output_schema of the operator.
   * If this is called prior to all input_schemas_ (i.e. parents) being provided
   * then nothing happens. Otherwise, output_schema_ is set to the computed
   * schema.
   */
  virtual void ComputeOutputSchema() = 0;

  // Edges to children and parents.
  std::vector<std::weak_ptr<Edge>> children_;
  std::vector<std::shared_ptr<Edge>> parents_;

  // Input and output schemas.
  std::vector<SchemaRef> input_schemas_;
  SchemaRef output_schema_;

 private:
  Type type_;
  NodeIndex index_;
  DataFlowGraph *graph_;  // The graph the operator belongs to.

  // Allow DataFlowGraph to use SetGraph, SetIndex, and AddParent functions.
  friend class DataFlowGraph;
#ifdef PELTON_BENCHMARK  // shuts up compiler warnings
  friend void JoinOneToOne(benchmark::State &state);
#endif
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPERATOR_H_
