#ifndef K9DB_DATAFLOW_OPERATOR_H_
#define K9DB_DATAFLOW_OPERATOR_H_

#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "gtest/gtest_prod.h"
#include "k9db/dataflow/future.h"
#include "k9db/dataflow/record.h"
#include "k9db/dataflow/schema.h"
#include "k9db/dataflow/types.h"

#ifdef K9DB_BENCHMARK  // shuts up compiler warnings
#include "k9db/dataflow/ops/benchmark_utils.h"
#endif

namespace k9db {
namespace dataflow {

class DataFlowGraphPartition;

class Operator {
 public:
  enum class Type {
    INPUT,
    IDENTITY,
    MAT_VIEW,
    FORWARD_VIEW,
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

  virtual ~Operator() {}

  /*!
   * Take a batch of records belonging to operator source and push it
   * through the dataflow graph. I.e., the batch will be processed through this
   * node and then pushed to all children nodes for further processing.
   * Internally calls process(...) function.
   * @param source
   * @param records
   * @return
   */
  void ProcessAndForward(NodeIndex source, std::vector<Record> &&records,
                         Promise &&promise);

  // Accessors (read only).
  NodeIndex index() const { return this->index_; }
  Type type() const { return this->type_; }
  PartitionIndex partition() const { return this->partition_; }

  const SchemaRef &output_schema() const { return this->output_schema_; }
  const std::vector<Operator *> &children() const { return this->children_; }
  const std::vector<Operator *> &parents() const { return this->parents_; }

  // For debugging.
  virtual std::string DebugString() const;
  virtual Record DebugRecord() const;

  uint64_t SizeInMemory(const std::string &flow_name,
                        std::vector<Record> *output) const;

 protected:
  explicit Operator(Type type) : index_(UNDEFINED_NODE_INDEX), type_(type) {}

  /*!
   * Push a batch of records through the dataflow graph. Order within this batch
   * does not matter. i.e. an operator can reorder individual records to
   * optimize processing within a single operator node. The dataflow assumes all
   * records of this batch arrive at the same time. Records will NOT be pushed
   * to children.
   * @param source the index of the source operator.
   * @param records the input vector of records.
   * @param promise this can be derived from by operators that need sub-promises
   *                but should not be resolved by the operator.
   * @return the output vector to broadcast to children operators.
   */
  virtual std::vector<Record> Process(NodeIndex source,
                                      std::vector<Record> &&records,
                                      const Promise &promise) = 0;

  /*!
   * Compute the output_schema of the operator.
   * If this is called prior to all input_schemas_ (i.e. parents) being provided
   * then nothing happens. Otherwise, output_schema_ is set to the computed
   * schema.
   */
  virtual void ComputeOutputSchema() = 0;

  // Meant to generate a clone of the same operator type and information.
  // The generated clone does not have its parents or children set, since those
  // likely need to be cloned as well. The cloned operator must be added to
  // a partition with the appropriate parent(s) for it to become fully
  // functional.
  // This should only be used inside DtaFlowGraphPartition's Clone() function.
  virtual std::unique_ptr<Operator> Clone() const = 0;

  // Return the size of any stored state in memory.
  virtual uint64_t SizeInMemory() const { return 0; }

  // Edges to children and parents.
  std::vector<Operator *> children_;
  std::vector<Operator *> parents_;

  // Input and output schemas.
  std::vector<SchemaRef> input_schemas_;
  SchemaRef output_schema_;

 private:
  // Called by DataFlowGraphPartition.
  friend class DataFlowGraphPartition;
  void SetIndex(NodeIndex index) { this->index_ = index; }
  void SetPartition(PartitionIndex partition) { this->partition_ = partition; }
  void AddParent(Operator *parent);
  void AddParentAt(Operator *parent, size_t parent_index);
  size_t RemoveParent(Operator *parent);

  // Pass the given batch to the children operators (if any) for processing.
  void BroadcastToChildren(std::vector<Record> &&records, Promise &&promise);

  NodeIndex index_;
  Type type_;
  PartitionIndex partition_;

  // These tests need to use AddParent().
  FRIEND_TEST(AggregateOperatorTest, SumGoesAwayWithFilter);
  FRIEND_TEST(AggregateOperatorTest, CountGoesAwayOnDelete);
  FRIEND_TEST(AggregateOperatorTest, SimpleAverage);

#ifdef K9DB_BENCHMARK  // shuts up compiler warnings
  friend void ProcessBenchmark(Operator *op, NodeIndex src, RecordGenFunc gen);
#endif
};

}  // namespace dataflow
}  // namespace k9db

#endif  // K9DB_DATAFLOW_OPERATOR_H_
