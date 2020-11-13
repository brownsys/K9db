#ifndef PELTON_DATAFLOW_TYPES_H_
#define PELTON_DATAFLOW_TYPES_H_

#include <limits>

namespace dataflow {

typedef uint32_t EdgeIndex;
typedef uint32_t NodeIndex;

typedef uint32_t ColumnID;

static const EdgeIndex UNDEFINED_EDGE_INDEX = std::numeric_limits<EdgeIndex>::max();
static const NodeIndex UNDEFINED_NODE_INDEX = std::numeric_limits<NodeIndex>::max();

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_TYPES_H_
