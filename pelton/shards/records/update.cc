// Create dataflow records expressing the effects of UPDATE statements.

#include "pelton/shards/records/update.h"

#include "absl/status/status.h"
#include "pelton/dataflow/schema.h"

namespace pelton {
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
}  // namespace pelton
