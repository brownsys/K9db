#ifndef PELTON_DATAFLOW_EDGE_H_
#define PELTON_DATAFLOW_EDGE_H_

#include "dataflow/operator.h"

namespace dataflow {

class Edge {
 public:
  Edge(std::shared_ptr<Operator> from, std::shared_ptr<Operator> to)
      : from_(from), to_(to){};
  virtual ~Edge(){};

 private:
  std::shared_ptr<Operator> from_;
  std::shared_ptr<Operator> to_;
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_EDGE_H_
