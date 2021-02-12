// Create dataflow records expressing the effects of INSERT statements.

#include "pelton/shards/records/insert.h"

#include <algorithm>
#include <string>

#include "absl/status/status.h"
#include "pelton/dataflow/schema.h"

namespace pelton {
namespace shards {
namespace records {
namespace insert {

absl::StatusOr<std::vector<dataflow::Record>> MakeRecords(
    const sqlast::Insert &stmt, SharderState *state) {
  // Get schema of target table.
  const dataflow::Schema &schema = state->LogicalSchemaOf(stmt.table_name());

  // Build records.
  std::vector<dataflow::Record> output;
  output.emplace_back(schema, true);

  // There is only one record, the order of values either match that of the
  // schema, or has to be resolved via the explicit column names provided in
  // the insert statement.
  const std::vector<std::string> &cols = stmt.GetColumns();
  const std::vector<std::string> &values = stmt.GetValues();

  if (values.size() != schema.num_columns()) {
    return absl::InvalidArgumentError("Insert does not match table schema");
  }

  for (size_t i = 0; i < schema.num_columns(); i++) {
    // Find value for column i using order or explicit column names if provided.
    std::string v = values.at(i);
    if (stmt.HasColumns()) {
      auto it = std::find(cols.cbegin(), cols.cend(), schema.NameOf(i));
      if (it == cols.cend()) {
        return absl::InvalidArgumentError("Insert does not match table schema");
      }
      v = values.at(std::distance(cols.cbegin(), it));
    }

    // Transform value to appropriate type and insert into record.
    switch (schema.TypeOf(i)) {
      case dataflow::DataType::kInt: {
        output.at(0).set_int(i, std::stoll(v));
        break;
      }
      case dataflow::DataType::kUInt: {
        output.at(0).set_int(i, std::stoull(v));
        break;
      }
      case dataflow::DataType::kText: {
        std::string *ptr = new std::string(v);
        output.at(0).set_string(i, ptr);
        break;
      }
      default:
        return absl::InvalidArgumentError("Unsupported datatype");
    }
  }

  return output;
}

}  // namespace insert
}  // namespace records
}  // namespace shards
}  // namespace pelton
