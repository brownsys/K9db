//
// Created by Leonhard Spiegelberg on 11/6/20.
//

#include "equijoin.h"

namespace dataflow {
    EquiJoin::EquiJoin(Operator* left, Operator* right,
                       ColumnID left_id, ColumnID right_id) : left_(left), right_(right), left_id_(left_id), right_id_(right_id) {
    }


}
