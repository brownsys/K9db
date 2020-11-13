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

        explicit EquiJoin(Operator* left, Operator* right, ColumnID left_id, ColumnID right_id);
    private:
        ColumnID left_id_;
        ColumnID right_id_;

        Operator* left_;
        Operator* right_;
    };
}

#endif //PELTON_EQUIJOIN_H
