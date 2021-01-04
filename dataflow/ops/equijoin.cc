//
// Created by Leonhard Spiegelberg on 11/6/20.
//

#include "equijoin.h"

namespace dataflow {
    EquiJoin::EquiJoin(ColumnID left_id, ColumnID right_id) : Operator::Operator(), left_id_(left_id), right_id_(right_id) {
    }

    // Note: use https://github.com/brownsys/pelton/blob/better-records-redux/dataflow/key.h
    // for hashing.

    bool process(NodeIndex src_op_idx, std::vector<Record>& rs,
                         std::vector<Record>& out_rs) {
        // append all input records to corresponding hashtable
        if(src_op_idx == left()->index()) {
          // match each record with right table

        } else if(src_op_idx == right()->index()) {
          // match each record with left table
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
