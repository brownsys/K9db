//
// Created by Leonhard Spiegelberg on 11/6/20.
//

#ifndef PELTON_EQUIJOIN_H
#define PELTON_EQUIJOIN_H

#include <vector>

#include "absl/container/flat_hash_map.h"

#include "dataflow/operator.h"
#include "dataflow/record.h"
#include "dataflow/key.h"
#include "dataflow/grouped_data.h"

namespace dataflow {
    class EquiJoin : public Operator {
    public:
        EquiJoin() = delete;
        explicit EquiJoin(ColumnID left_id, ColumnID right_id);

        bool process(std::vector<Record>& rs,
                             std::vector<Record>& out_rs) override {
                              throw std::runtime_error("do not call for EquiJoin, needs info from where data is coming.");
        }

        bool process(NodeIndex src_op_idx, std::vector<Record>& rs,
                             std::vector<Record>& out_rs) override;

        OperatorType type() const override { return OperatorType::EQUIJOIN; }
    private:
        ColumnID left_id_;
        ColumnID right_id_;

        // hash tables for each operator side
        GroupedData left_table_;
        GroupedData right_table_;

        std::shared_ptr<Operator> left() const { assert(parents().size() == 2); return parents()[0]; }
        std::shared_ptr<Operator> right() const { assert(parents().size() == 2); return parents()[1]; }
    };
}

#endif //PELTON_EQUIJOIN_H
