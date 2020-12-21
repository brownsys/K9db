#ifndef PELTON_DATAFLOW_OPS_MATVIEW_H_
#define PELTON_DATAFLOW_OPS_MATVIEW_H_

#include <vector>

#include "absl/container/flat_hash_map.h"

#include "dataflow/operator.h"
#include "dataflow/record.h"

namespace dataflow {

class MatViewOperator : public Operator {
 public:
  OperatorType type() const override { return OperatorType::MAT_VIEW; }
  bool process(std::vector<Record>& rs, std::vector<Record>& out_rs) override;

  explicit MatViewOperator(std::vector<ColumnID> key_cols)
      : key_cols_(key_cols) {
    // nothing to do
  }

  std::vector<Record> lookup(const RecordData& key) const;
  std::vector<Record> multi_lookup(std::vector<RecordData>& keys);

 private:
  // TODO(malte): should use Key type
  absl::flat_hash_map<RecordData, std::vector<Record>> contents_;
  std::vector<ColumnID> key_cols_;

  const RecordData get_key(const Record& r) {
    assert(key_cols_.size() == 1);
    ColumnID cid = key_cols_[0];

    return r.raw_at(cid);
  }
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_OPS_MATVIEW_H_
