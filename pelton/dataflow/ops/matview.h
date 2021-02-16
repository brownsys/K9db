#ifndef PELTON_DATAFLOW_OPS_MATVIEW_H_
#define PELTON_DATAFLOW_OPS_MATVIEW_H_

#include <vector>

#include "absl/container/flat_hash_map.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/grouped_data.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class MatViewOperator : public Operator {
 public:
  explicit MatViewOperator(const std::vector<ColumnID> &key_cols)
      : Operator(Operator::Type::MAT_VIEW), key_cols_(key_cols) {}

  explicit MatViewOperator(std::vector<ColumnID> &&key_cols)
      : Operator(Operator::Type::MAT_VIEW), key_cols_(key_cols) {}

  bool Process(NodeIndex source, const std::vector<Record> &records,
               std::vector<Record> *output) override;

  const std::vector<Record> &Lookup(const Key &key) const;

  GroupedData::const_iterator cbegin() const;
  GroupedData::const_iterator cend() const;

 private:
  GroupedData contents_;
  std::vector<ColumnID> key_cols_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_MATVIEW_H_
