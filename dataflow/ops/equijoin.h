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

        bool process(std::vector<Record>& rs,
                             std::vector<Record>& out_rs) override {
                              throw std::runtime_error("do not call for EquiJoin, needs info from where data is coming.");
        }

        bool process(NodeIndex src_op_idx, std::vector<Record>& rs,
                             std::vector<Record>& out_rs) override;

        OperatorType type() const { return OperatorType::EQUIJOIN; }
    private:
        ColumnID left_id_;
        ColumnID right_id_;

        // hash tables for each operator side
        inline void emitRow(std::vector<Record>& out_rs, const Record& r_left, const Record& r_right) {
            // create a concatenated record, dropping key column from left side
            auto left_schema = r_left.schema();
            auto right_schema = r_right.schema();
            // TODO: mapping ColumnID to index. For now assume they're the same
            unsigned right_key_idx = right_id_;

            std::vector<DataType> st;
            for(unsigned i = 0; i < left_schema.num_columns(); ++i) {
                st.push_back(left_schema.TypeOf(i));
            }
            for(unsigned i = 0; i < right_schema.num_columns(); ++i) {
                if(i != right_key_idx)
                    st.push_back(right_schema.TypeOf(i));
            }

            Schema s(st);
            Record r(s);

            // @TODO: refactor this??
            for(unsigned i = 0; i < left_schema.num_columns(); ++i)
                switch(left_schema.TypeOf(i)) {
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

            // @TODO: refactor this??
            unsigned i = 0;
            for(unsigned j = 0; j < left_schema.num_columns(); ++j) {
                if(j == right_key_idx)
                    continue; // skip key column...
                switch(left_schema.TypeOf(i)) {
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

        std::shared_ptr<Operator> left() const { assert(parents().size() == 2); return parents()[0]; }
        std::shared_ptr<Operator> right() const { assert(parents().size() == 2); return parents()[1]; }
    };
}

#endif //PELTON_EQUIJOIN_H
