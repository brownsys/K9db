//
// Created by Leonhard Spiegelberg on 11/6/20.
//

#ifndef PELTON_DATAFLOW_OPS_EQUIJOIN_H_
#define PELTON_DATAFLOW_OPS_EQUIJOIN_H_

#include <memory>
#include <vector>

#include "gtest/gtest_prod.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/grouped_data.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class EquiJoinOperator : public Operator {
 public:
  EquiJoinOperator() = delete;

  EquiJoinOperator(ColumnID left_id, ColumnID right_id)
      : Operator(Operator::Type::EQUIJOIN),
        left_id_(left_id),
        right_id_(right_id),
        joined_schema_() {}

  std::shared_ptr<Operator> left() const {
    return this->parents_.at(0)->from();
  }
  std::shared_ptr<Operator> right() const {
    return this->parents_.at(1)->from();
  }

 protected:
  /*!
   * processes a batch of input rows and writes output to output.
   * Output schema of join (i.e. records written to output) is defined as
   * concatenated left schema and right schema with key column from the right
   * schema dropped.
   * @param source
   * @param records
   * @param output
   * @return
   */
  bool Process(NodeIndex source, const std::vector<Record> &records,
               std::vector<Record> *output) override;

  // Computes joined_schema_.
  void ComputeOutputSchema() override;

 private:
  // Columns on which join is computed.
  ColumnID left_id_;
  ColumnID right_id_;
  // Schema of the output.
  SchemaOwner joined_schema_;
  UnorderedGroupedData left_table_;
  UnorderedGroupedData right_table_;

  // Join left and right and store it in output.
  void EmitRow(const Record &left, const Record &right,
               std::vector<Record> *output);

  FRIEND_TEST(EquiJoinOperatorTest, JoinedSchemaTest);
  FRIEND_TEST(EquiJoinOperatorTest, BasicJoinTest);
  FRIEND_TEST(EquiJoinOperatorTest, BasicUnjoinableTest);
  FRIEND_TEST(EquiJoinOperatorTest, FullJoinTest);
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_EQUIJOIN_H_
