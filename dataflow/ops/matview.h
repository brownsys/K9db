#ifndef PELTON_DATAFLOW_OPS_MATVIEW_H_
#define PELTON_DATAFLOW_OPS_MATVIEW_H_

#include <vector>

#include "absl/container/flat_hash_map.h"
#include "dataflow/key.h"
#include "dataflow/operator.h"
#include "dataflow/record.h"

namespace dataflow {

class MatViewOperator : public Operator {
 public:
  OperatorType type() override { return OperatorType::MAT_VIEW; }
  bool process(std::vector<Record>& rs, std::vector<Record>& out_rs) override;

  explicit MatViewOperator(std::vector<ColumnID> key_cols, const Schema& schema)
      : key_cols_(key_cols), schema_(&schema) {
    // nothing to do
  }

  std::vector<Record> lookup(const Key& key) const;
  std::vector<Record> multi_lookup(const std::vector<Key>& keys);
  const Schema& schema() { return *schema_; }

 private:
  absl::flat_hash_map<Key, std::vector<Record>> contents_;
  std::vector<ColumnID> key_cols_;
  const Schema* schema_;
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_OPS_MATVIEW_H_
