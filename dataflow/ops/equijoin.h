//
// Created by Leonhard Spiegelberg on 11/6/20.
//

#ifndef PELTON_EQUIJOIN_H
#define PELTON_EQUIJOIN_H

#include <vector>
#include "dataflow/operator.h"
#include "dataflow/record.h"

namespace dataflow {

class EquiJoin : public Operator {
 public:
  EquiJoin() = delete;
  explicit EquiJoin(ColumnID left_id, ColumnID right_id);

  bool process(std::vector<Record>& rs, std::vector<Record>& out_rs) override {
    throw std::runtime_error(
        "do not call for EquiJoin, needs info from where data is coming.");
  }

  /*!
   * processes a batch of input rows and writes output to out_rs. Output schema
   * of join (i.e. records written to out_rs) is defined as concatenated left
   * schema and right schema w. key column dropped.
   * @param src_op_idx
   * @param rs
   * @param out_rs
   * @return
   */
  bool process(NodeIndex src_op_idx, std::vector<Record>& rs,
               std::vector<Record>& out_rs) override;

  OperatorType type() const override { return OperatorType::EQUIJOIN; }

  std::shared_ptr<Operator> left() const {
    assert(parents().size() == 2);
    return parents()[0];
  }
  std::shared_ptr<Operator> right() const {
    assert(parents().size() == 2);
    return parents()[1];
  }

 private:
  ColumnID left_id_;
  ColumnID right_id_;

  // hash tables for each operator side
  GroupedData left_table_;
  GroupedData right_table_;

  // output schema, must live here because records only own reference to it
  Schema left_schema_;
  Schema right_schema_;
  Schema joined_schema_;

  inline void emitRow(std::vector<Record>& out_rs, const Record& r_left,
                      const Record& r_right) {

    // TODO: mapping ColumnID to index. For now assume they're the same
    unsigned right_key_idx = right_id_;

    // set schemas here lazily, note this can get optimized.
    // create a concatenated record, dropping key column from left side
    if(joined_schema_.is_undefined()) {
      left_schema_ = r_left.schema();
      right_schema_ = r_right.schema();

      std::vector<DataType> st;
      for (unsigned i = 0; i < left_schema_.num_columns(); ++i) {
        st.push_back(left_schema_.TypeOf(i));
      }
      for (unsigned i = 0; i < right_schema_.num_columns(); ++i) {
        if (i != right_key_idx) st.push_back(right_schema_.TypeOf(i));
      }

      joined_schema_ = Schema(st);
    }

    Record r(joined_schema_);

    // @TODO: refactor this??
    for (unsigned i = 0; i < left_schema_.num_columns(); ++i)
      switch (left_schema_.TypeOf(i)) {
        case kUInt: {
          r.set_uint(i, r_left.as_uint(i));
          break;
        }
        case kInt: {
          r.set_int(i, r_left.as_int(i));
          break;
        }
        case kText: {
          auto str_copy = new std::string(*r_left.as_string(i));
          r.set_string(i, str_copy);
          break;
        }

    // @TODO: refactor this??
    unsigned i = 0;
    for (unsigned j = 0; j < right_schema_.num_columns(); ++j) {
      if (j == right_key_idx) continue;  // skip key column...
      switch (right_schema_.TypeOf(i)) {
        case kUInt: {
          r.set_uint(i, r_left.as_uint(i));
          break;
        }
        case kInt: {
          r.set_int(i, r_left.as_int(i));
          break;
        }
        case kText: {
          auto str_copy = new std::string(*r_left.as_string(i));
          r.set_string(i, str_copy);
          break;
        }
        case kDatetime: {
          throw std::runtime_error("unsupported yet in record...");
          break;
        }
#ifndef NDEBUG
        default:
          throw std::runtime_error("fatal internal error in emitRow");
#endif
      }
      i++;
    }

    // add result record to output
    out_rs.push_back(r);
  }

}  // namespace dataflow

#endif //PELTON_EQUIJOIN_H
