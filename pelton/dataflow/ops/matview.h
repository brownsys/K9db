#ifndef PELTON_DATAFLOW_OPS_MATVIEW_H_
#define PELTON_DATAFLOW_OPS_MATVIEW_H_

#include <utility>
#include <vector>

#include "pelton/dataflow/key.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/grouped_data.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class MatViewOperator : public Operator {
 public:
  // Explicitly specified keys for this Materialized view may differ from
  // PrimaryKey.
  explicit MatViewOperator(const std::vector<ColumnID> &key_cols);
  explicit MatViewOperator(std::vector<ColumnID> &&key_cols);

  size_t count() const;
  bool Contains(const Key &key) const;
  const std::vector<Record> &Lookup(const Key &key) const;

  GroupedData::const_iterator begin() const;
  GroupedData::const_iterator end() const;

 protected:
  bool Process(NodeIndex source, const std::vector<Record> &records,
               std::vector<Record> *output) override;

  void ComputeOutputSchema() override;

 private:
  GroupedData contents_;
  std::vector<ColumnID> key_cols_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_MATVIEW_H_
