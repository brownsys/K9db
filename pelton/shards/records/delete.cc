// Create dataflow records expressing the effects of DELETE statements.

#include "pelton/shards/records/delete.h"

#include "absl/status/status.h"
#include "pelton/dataflow/schema.h"

namespace pelton {
namespace shards {
namespace records {
namespace delete_ {

absl::StatusOr<std::vector<dataflow::Record>> MakeRecords(
    const sqlast::Delete &stmt, SharderState *state) {
  return absl::UnimplementedError("Flows not integrated for deletes yet");
}

}  // namespace delete_
}  // namespace records
}  // namespace shards
}  // namespace pelton
