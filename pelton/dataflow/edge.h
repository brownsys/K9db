#ifndef PELTON_DATAFLOW_EDGE_H_
#define PELTON_DATAFLOW_EDGE_H_

#include <memory>

#include "pelton/dataflow/operator.h"

namespace pelton {
namespace dataflow {

class Operator;

class Edge {
 public:
  Edge(std::shared_ptr<Operator> from, std::shared_ptr<Operator> to)
      : from_(from), to_(to) {}
  ~Edge() {}

  std::shared_ptr<Operator> from() { return from_; }
  std::weak_ptr<Operator> to() { return to_; }

 private:
  std::shared_ptr<Operator> from_;
  std::weak_ptr<Operator> to_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_EDGE_H_
