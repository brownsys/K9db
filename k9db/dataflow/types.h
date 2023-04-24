#ifndef K9DB_DATAFLOW_TYPES_H_
#define K9DB_DATAFLOW_TYPES_H_

#include <limits>
#include <vector>

namespace k9db {
namespace dataflow {

typedef uint32_t NodeIndex;
typedef uint32_t PartitionIndex;
typedef uint32_t ColumnID;
typedef std::vector<ColumnID> PartitionKey;

static const NodeIndex UNDEFINED_NODE_INDEX =
    std::numeric_limits<NodeIndex>::max();

}  // namespace dataflow
}  // namespace k9db

#endif  // K9DB_DATAFLOW_TYPES_H_
