// Create dataflow records expressing the effects of UPDATE statements.

#include "shards/records/update.h"

#include "absl/status/status.h"
#include "dataflow/schema.h"

namespace shards {
namespace records {
namespace update {

absl::StatusOr<std::vector<dataflow::Record>> MakeRecords(
    const sqlast::Update &stmt, SharderState *state) {
  return absl::UnimplementedError("Flows not integrated for updates yet");
}

}  // namespace update
}  // namespace records
}  // namespace shards
