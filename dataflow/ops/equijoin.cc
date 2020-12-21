//
// Created by Leonhard Spiegelberg on 11/6/20.
//

#include "equijoin.h"

namespace dataflow {
    EquiJoin::EquiJoin(ColumnID left_id, ColumnID right_id) : Operator::Operator(), left_id_(left_id), right_id_(right_id) {
    }
}
