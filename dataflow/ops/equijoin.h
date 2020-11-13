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

        explicit EquiJoin(ColumnID left_id, ColumnID right_id);
    private:
        ColumnID left_id_;
        ColumnID right_id_;

        std::shared_ptr<Operator> left() { assert(parents().size() == 2); return parents()[0]; }
        std::shared_ptr<Operator> right() { assert(parents().size() == 2); return parents()[1]; }
    };
}

#endif //PELTON_EQUIJOIN_H
