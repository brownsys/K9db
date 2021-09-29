#ifndef PELTON_DATAFLOW_OPS_EQUIJOIN_H_
#define PELTON_DATAFLOW_OPS_EQUIJOIN_H_

#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "gtest/gtest_prod.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/grouped_data.h"
#include "pelton/dataflow/ops/join_enum.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

// protect against underflow bugs
#define MAX_JOIN_COLUMNS (((uint32_t)-1) - 1000)

class EquiJoinOperator : public Operator {
 public:
  using Mode = JoinModeEnum;
  EquiJoinOperator() = delete;
  // Cannot copy an operator.
  EquiJoinOperator(const EquiJoinOperator &other) = delete;
  EquiJoinOperator &operator=(const EquiJoinOperator &other) = delete;

  EquiJoinOperator(ColumnID left_id, ColumnID right_id, Mode mode = Mode::INNER)
      : Operator(Operator::Type::EQUIJOIN),
        left_id_(left_id),
        right_id_(right_id),
        mode_(mode) {
    if (left_id > MAX_JOIN_COLUMNS) {
      LOG(FATAL) << "implausible column ID " << left_id << " in join operator!";
    }
    if (right_id > MAX_JOIN_COLUMNS) {
      LOG(FATAL) << "implausible column ID " << right_id
                 << " in join operator!";
    }
  }

  std::shared_ptr<Operator> left() const;
  std::shared_ptr<Operator> right() const;

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
  std::optional<std::vector<Record>> Process(
      NodeIndex source, const std::vector<Record> &records) override;

  // Computes joined_schema_.
  void ComputeOutputSchema() override;

  uint64_t SizeInMemory() const override {
    return this->left_table_.SizeInMemory() +
           this->right_table_.SizeInMemory() +
           this->emitted_nulls_.SizeInMemory();
  }

  std::shared_ptr<Operator> Clone() const override;

 private:
  // Columns on which join is computed.
  ColumnID left_id_;
  ColumnID right_id_;
  Mode mode_;
  UnorderedGroupedData left_table_;
  UnorderedGroupedData right_table_;
  // Keeps track of (left records + NULL) or (NULL + right records) columns that
  // have been emitted. This is later used to generate corresponding
  // negative records
  UnorderedGroupedData emitted_nulls_;
  // Records consisting entirely of NULL values. at index 0 is meant to be for
  // left schema and at index 1 is meant to be for right schema.
  std::vector<Record> null_records_;

  // Join left and right and store it in output.
  void EmitRow(const Record &left, const Record &right,
               std::vector<Record> *output, bool positive);

  FRIEND_TEST(EquiJoinOperatorTest, JoinedSchemaTest);
  FRIEND_TEST(EquiJoinOperatorTest, BasicJoinTest);
  FRIEND_TEST(EquiJoinOperatorTest, BasicUnjoinableTest);
  FRIEND_TEST(EquiJoinOperatorTest, FullJoinTest);
  FRIEND_TEST(EquiJoinOperatorTest, BasicLeftJoinTest);
  FRIEND_TEST(EquiJoinOperatorTest, BasicRightJoinTest);
  FRIEND_TEST(EquiJoinOperatorTest, LeftJoinTest);

#ifdef PELTON_BENCHMARK  // shuts up compiler warnings
  // NOLINTNEXTLINE
  friend void JoinOneToOne(benchmark::State &state);
#endif
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_EQUIJOIN_H_
