#ifndef PELTON_DATAFLOW_OPS_MATVIEW_H_
#define PELTON_DATAFLOW_OPS_MATVIEW_H_

#include <vector>

#include "absl/container/flat_hash_map.h"

#include "dataflow/key.h"
#include "dataflow/operator.h"
#include "dataflow/record.h"
#include "dataflow/grouped_data.h"

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
  std::vector<Record> multi_lookup(const std::vector<Key>& keys);

 private:
  GroupedData contents_;
  std::vector<ColumnID> key_cols_;

  const std::pair<Key, bool> get_key(const Record& r) {
    assert(key_cols_.size() == 1);
    ColumnID cid = key_cols_[0];

    const void* key =
        reinterpret_cast<const void*>(*static_cast<const uintptr_t*>(r[cid]));
    return std::make_pair(Key(key, r.schema().TypeOf(cid)),
                          Schema::is_inlineable(r.schema().TypeOf(cid)));
  }
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_OPS_MATVIEW_H_
