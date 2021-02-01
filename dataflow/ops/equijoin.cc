//
// Created by Leonhard Spiegelberg on 11/6/20.
//

#include "equijoin.h"

namespace dataflow {

EquiJoin::EquiJoin(ColumnID left_id, ColumnID right_id)
    : Operator::Operator(), left_id_(left_id), right_id_(right_id), left_schema_(nullptr), right_schema_(nullptr), joined_schema_(nullptr) {}

bool EquiJoin::process(NodeIndex src_op_idx, std::vector<Record>& rs,
                       std::vector<Record>& out_rs) {

  for (const auto& r : rs) {
    // in comparison to a normal HashJoin in a database, here
    // record flow from one operator in.

    // append all input records to corresponding hashtable
    if (src_op_idx == left()->index()) {
      // match each record with right table
      // coming from left operator
      // => match with right operator's table!
      const auto record_left_key_ptr = reinterpret_cast<const void*>(
          *static_cast<const uintptr_t*>(r[left_id_]));
      auto left_key = Key(record_left_key_ptr, r.schema().TypeOf(left_id_));

      for (auto it = right_table_.beginGroup(left_key);
           it != right_table_.endGroup(left_key); ++it) {
        emitRow(out_rs, r, *it);
      }

      // save record hashed to left table
      left_table_.insert(left_key, r);

    } else if (src_op_idx == right()->index()) {
      // match each record with left table
      // coming from right operator
      // => match with left operator's table!
      const auto record_right_key_ptr = reinterpret_cast<const void*>(
          *static_cast<const uintptr_t*>(r[right_id_]));
      auto right_key = Key(record_right_key_ptr, r.schema().TypeOf(right_id_));

                // TODO: emit final records if match occured!
                for(auto it = left_table_.beginGroup(right_key); it != left_table_.endGroup(right_key); ++it) {
                    emitRow(out_rs, *it, r);
                }

                // save record hashed to right table
                right_table_.insert(right_key, r);
            } else {
#ifndef NDEBUG
          throw std::runtime_error("internal error. Received data input"
           "from operator " + std::to_string(src_op_idx)
           + " but join has only IDs " + std::to_tring(left()->index())
           + " and " + std::to_string(right()->index()) + " registered to it.");
#endif
        }
    }
    }
  }
  return true;
}

}
