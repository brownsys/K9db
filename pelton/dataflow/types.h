#ifndef PELTON_DATAFLOW_TYPES_H_
#define PELTON_DATAFLOW_TYPES_H_

#include <limits>
#include <vector>

namespace pelton {
namespace dataflow {

typedef uint32_t NodeIndex;
typedef uint32_t PartitionIndex;
typedef uint32_t ColumnID;
typedef std::vector<ColumnID> PartitionKey;

static const NodeIndex UNDEFINED_NODE_INDEX =
    std::numeric_limits<NodeIndex>::max();

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_TYPES_H_
