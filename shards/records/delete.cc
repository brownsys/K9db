// Create dataflow records expressing the effects of DELETE statements.

#include "shards/records/delete.h"

#include "absl/status/status.h"
#include "dataflow/schema.h"

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
