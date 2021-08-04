#ifndef PELTON_DATAFLOW_OPERATOR_H_
#define PELTON_DATAFLOW_OPERATOR_H_

#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include "benchmark/benchmark.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class DataFlowGraph;

#ifdef PELTON_BENCHMARK  // shuts up compiler warnings
// NOLINTNEXTLINE
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
    EXCHANGE,
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
  virtual void ProcessAndForward(NodeIndex source,
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

  // Meant to generate a clone with same operator specific information, edges,
  // and input/output schemas.
  virtual std::shared_ptr<Operator> Clone() const = 0;

  // For debugging.
  virtual std::string DebugString() const;

 protected:
  explicit Operator(Type type)
      : index_(UNDEFINED_NODE_INDEX), type_(type), graph_(nullptr) {}

  void SetGraph(DataFlowGraph *graph) { this->graph_ = graph; }
  void SetIndex(NodeIndex index) { this->index_ = index; }
  void AddParent(std::shared_ptr<Operator> parent,
                 std::tuple<NodeIndex, NodeIndex> edge);

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
  virtual std::optional<std::vector<Record>> Process(
      NodeIndex source, const std::vector<Record> &records) = 0;

  /*!
   * Compute the output_schema of the operator.
   * If this is called prior to all input_schemas_ (i.e. parents) being provided
   * then nothing happens. Otherwise, output_schema_ is set to the computed
   * schema.
   */
  virtual void ComputeOutputSchema() = 0;

  // Return the size of any stored state in memory.
  virtual uint64_t SizeInMemory() const { return 0; }

  // Edges to children and parents.
  std::vector<NodeIndex> children_;
  std::vector<NodeIndex> parents_;

  // Input and output schemas.
  std::vector<SchemaRef> input_schemas_;
  SchemaRef output_schema_;
  NodeIndex index_;

 private:
  Type type_;
  DataFlowGraph *graph_;  // The graph the operator belongs to.
  void BroadcastToChildren(const std::vector<Record> &records);

  // Allow DataFlowGraph to use SetGraph, SetIndex, and AddParent functions.
  friend class DataFlowGraph;
  // friend class MatViewOperator;
  template <typename T>
  friend class MatViewOperatorT;

#ifdef PELTON_BENCHMARK  // shuts up compiler warnings
  // NOLINTNEXTLINE
  friend void JoinOneToOne(benchmark::State &state);
#endif
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPERATOR_H_
