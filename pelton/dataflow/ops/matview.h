#ifndef PELTON_DATAFLOW_OPS_MATVIEW_H_
#define PELTON_DATAFLOW_OPS_MATVIEW_H_

#include <vector>

#include "absl/container/flat_hash_map.h"
#include "pelton/dataflow/grouped_data.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/record.h"

namespace pelton {
namespace dataflow {

class MatViewOperator : public Operator {
 public:
  OperatorType type() const override { return OperatorType::MAT_VIEW; }
  bool process(std::vector<Record>& rs, std::vector<Record>& out_rs) override;

  explicit MatViewOperator(std::vector<ColumnID> key_cols)
      : key_cols_(key_cols) {
    // nothing to do
  }

  std::vector<Record> lookup(const Key& key) const;
  std::vector<Record> contents() const;

 private:
  GroupedData contents_;
  std::vector<ColumnID> key_cols_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_MATVIEW_H_
