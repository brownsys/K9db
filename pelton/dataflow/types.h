#ifndef PELTON_DATAFLOW_TYPES_H_
#define PELTON_DATAFLOW_TYPES_H_

#include <limits>

namespace pelton {
namespace dataflow {

typedef uint32_t NodeIndex;

typedef uint32_t GraphIndex;

typedef uint32_t ColumnID;

typedef uint16_t PartitionID;

static const NodeIndex UNDEFINED_NODE_INDEX =
    std::numeric_limits<NodeIndex>::max();

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_TYPES_H_
