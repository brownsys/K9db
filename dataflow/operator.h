#ifndef PELTON_DATAFLOW_OPERATOR_H_
#define PELTON_DATAFLOW_OPERATOR_H_

#include <memory>

#include "dataflow/edge.h"
#include "dataflow/record.h"
#include "dataflow/types.h"

namespace dataflow {

typedef uint32_t EdgeIndex;
typedef uint32_t NodeIndex;

class Edge;

enum OperatorType {
  INPUT,
  IDENTITY,
  MAT_VIEW,
  FILTER,
};

class Operator {
 public:
  virtual ~Operator(){};

  void AddParent(std::shared_ptr<Operator> parent, std::shared_ptr<Edge> edge);
  virtual bool process(std::vector<Record>& rs,
                       std::vector<Record>& out_rs) = 0;
  bool ProcessAndForward(std::vector<Record>& rs);

  virtual OperatorType type() = 0;
  NodeIndex index() const { return index_; }
  void set_index(NodeIndex index) { index_ = index; }

 private:
  NodeIndex index_;
  std::vector<std::shared_ptr<Edge>> parents_;
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_OPERATOR_H_
