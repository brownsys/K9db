#include "dataflow/ops/aggregate.h"

#include <vector>

#include "dataflow/record.h"

namespace dataflow {

// This class is used to track state changes when processing given batch of
// input records
class StateChange {
 public:
  StateChange(bool is_insert, Record old_val)
      : is_insert_(is_insert), old_val_(old_val) {}
  bool is_insert_;
  Record old_val_;
};

bool AggregateOperator::process(std::vector<Record>& rs,
                                std::vector<Record>& out_rs) {
  absl::flat_hash_map<std::vector<Key>, StateChange> first_delta;

  for (Record& r : rs) {
    std::vector<Key> key = get_key(r);

    switch (agg_func_) {
      case FuncCount:
        if (!r.positive()) {
          if (!state_.contains(key)) {
            LOG(FATAL)
                << "State does not exist for corresponding negative record";
          }
          if (!first_delta.contains(key)) {  // track first state change of key
            first_delta.emplace(key, StateChange(false, state_.at(key)));
          }
          state_.at(key).set_uint(0, state_.at(key).as_uint(0) - 1);
        } else {  // handle positive record
          if (!state_.contains(key)) {
            Record dummy_r(agg_schema_);
            // track first state change of key
            first_delta.emplace(key, StateChange(true, dummy_r));
            // new Record
            Record agg_r(agg_schema_);
            agg_r.set_uint(0, 1ULL);
            state_.emplace(key, agg_r);
          } else {
            if (!first_delta.contains(key)) {  // track first state change of key
              first_delta.emplace(key, StateChange(false, state_.at(key)));
            }
            state_.at(key).set_uint(0, state_.at(key).as_uint(0) + 1);
          }
        }
        break;
      case FuncSum:
        if (!r.positive()) {
          if (!state_.contains(key)) {
            LOG(FATAL)
                << "State does not exist for corresponding negative record";
          }
          if (!first_delta.contains(key)) {  // track first state change of key
            first_delta.emplace(key, StateChange(false, state_.at(key)));
          }
          switch (agg_schema_.TypeOf(0)) {
            case DataType::kUInt:
              state_.at(key).set_uint(
                  0, state_.at(key).as_uint(0) - r.as_uint(agg_col_));
              break;
            case DataType::kInt:
              state_.at(key).set_int(
                  0, state_.at(key).as_int(0) - r.as_int(agg_col_));
              break;
            default:
              LOG(FATAL) << "Unexpected type when computing SUM aggregate";
          }
        } else {  // handle positive record
          if (!state_.contains(key)) {
            Record dummy_r(agg_schema_);
            // track first state change of key
            first_delta.emplace(key, StateChange(true, dummy_r));
            // new Record
            Record agg_r(agg_schema_);
            switch (agg_schema_.TypeOf(0)) {
              case DataType::kUInt:
                agg_r.set_uint(0, r.as_uint(agg_col_));
                break;
              case DataType::kInt:
                agg_r.set_int(0, r.as_int(agg_col_));
                break;
              default:
                // TODO(Ishan): support for float?
                LOG(FATAL) << "Unexpected type when computing SUM aggregate";
            }
            state_.emplace(key, agg_r);
          } else {
            if (!first_delta.contains(key)) {  // track first state change of key
              first_delta.emplace(key, StateChange(false, state_.at(key)));
            }
            switch (agg_schema_.TypeOf(0)) {
              case DataType::kUInt:
                state_.at(key).set_uint(
                    0, state_.at(key).as_uint(0) + r.as_uint(agg_col_));
                break;
              case DataType::kInt:
                state_.at(key).set_int(
                    0, state_.at(key).as_int(0) + r.as_int(agg_col_));
                break;
              default:
                LOG(FATAL) << "Unexpected type when computing SUM aggregate";
            }
          }
        }
        break;
    }
  }

  // emit records only for tracked changes
  for (auto const& item : first_delta) {
    // avoid an edge case where a key is inserted via a positive record
    // and whose effect is negated by the subsequent negative record(s)
    // in the same batch
    if (item.second.is_insert_) {
      bool flag = false;
      // in the following switch block, comparison of latest state update
      // with 0 is semantically correct for both FuncSum and FuncCout
      switch (agg_schema_.TypeOf(0)) {
        case DataType::kUInt:
          if (state_.at(item.first).as_uint(0) == 0) {
            flag = true;
          }
          break;
        case DataType::kInt:
          if (state_.at(item.first).as_int(0) == 0) {
            flag = true;
          }
          break;
        default:
          LOG(FATAL) << "Unexpected type when computing SUM aggregate";
      }

      if (flag) {
        // don't emit any records since the negative records have cancelled
        // out the effect of positive records within a single batch.
        continue;
      }
    }

    Record pos_record = gen_out_record(item.first, state_.at(item.first), true);
    if (item.second.is_insert_) {
      out_rs.push_back(pos_record);
    } else {
      Record neg_record =
          gen_out_record(item.first, item.second.old_val_, false);
      // for a negative update push negative record followed by positvie record
      out_rs.push_back(neg_record);
      out_rs.push_back(pos_record);
    }
  }
  return true;
}

}  // namespace dataflow
