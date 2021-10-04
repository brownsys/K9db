#include "pelton/dataflow/benchmark/utils.h"

#include <string>
#include <utility>

namespace pelton {
namespace dataflow {
namespace utils {

SchemaRef MakeFilterSchema() {
  std::vector<std::string> names = {"ID", "Category"};
  std::vector<CType> types = {CType::UINT, CType::UINT};
  std::vector<ColumnID> keys = {0};
  return SchemaFactory::Create(names, types, keys);
}

SchemaRef MakeEquiJoinLeftSchema() {
  std::vector<std::string> names = {"ID", "Category"};
  std::vector<CType> types = {CType::UINT, CType::UINT};
  std::vector<ColumnID> keys = {0};
  return SchemaFactory::Create(names, types, keys);
}

SchemaRef MakeEquiJoinRightSchema() {
  std::vector<std::string> names = {"ID", "Category", "Count"};
  std::vector<CType> types = {CType::UINT, CType::UINT, CType::UINT};
  std::vector<ColumnID> keys = {0};
  return SchemaFactory::Create(names, types, keys);
}

SchemaRef MakeAggregateSchema() {
  std::vector<std::string> names = {"ID", "Category", "Count"};
  std::vector<CType> types = {CType::UINT, CType::UINT, CType::UINT};
  std::vector<ColumnID> keys = {0};
  return SchemaFactory::Create(names, types, keys);
}

SchemaRef MakeIdentitySchema() { return MakeFilterSchema(); }

std::vector<std::vector<Record>> MakeFilterBatches(uint64_t num_batches,
                                                   uint64_t batch_size) {
  uint64_t record_id = 0;
  SchemaRef schema = utils::MakeFilterSchema();
  std::vector<std::vector<Record>> batches;
  for (uint64_t i = 0; i < num_batches; i++) {
    std::vector<Record> batch;
    for (uint64_t j = 0; j < batch_size; j++) {
      // Since record_id is sequentially generated, if the matview is keyed
      // on the same column then the records will be equally distributed among
      // all the partitions since partition::HashPartition computes a modulo
      // on the column and then decides which partition the record belongs to.
      batch.emplace_back(schema, true, record_id, 0_u);
      record_id += 1;
    }
    batches.push_back(std::move(batch));
  }
  return batches;
}

std::vector<std::vector<Record>> MakeEquiJoinLeftBatches(uint64_t num_batches,
                                                         uint64_t batch_size) {
  uint64_t record_id = 0;
  SchemaRef schema = utils::MakeEquiJoinLeftSchema();
  std::vector<std::vector<Record>> batches;
  for (uint64_t i = 0; i < num_batches; i++) {
    std::vector<Record> batch;
    for (uint64_t j = 0; j < batch_size; j++) {
      // The join will be performed on the second column.
      batch.emplace_back(schema, true, record_id * 2, record_id);
      record_id += 1;
    }
    batches.push_back(std::move(batch));
  }
  return batches;
}

std::vector<std::vector<Record>> MakeEquiJoinRightBatches(uint64_t num_batches,
                                                          uint64_t batch_size) {
  uint64_t record_id = 0;
  SchemaRef schema = utils::MakeEquiJoinRightSchema();
  std::vector<std::vector<Record>> batches;
  for (uint64_t i = 0; i < num_batches; i++) {
    std::vector<Record> batch;
    for (uint64_t j = 0; j < batch_size; j++) {
      // Reserve two columns (the second and third column w.r.t this schema)
      // so that the matview can be keyed on either one based on whether the
      // graph is of type EQUIJOIN_GRAPH_WITH_EXCHANGE(keyed on third column) or
      // EQUIJOIN_GRAPH_WITHOUT_EXCHANGE(keyed on third column).
      batch.emplace_back(schema, true, record_id * 3, record_id, record_id);
      record_id += 1;
    }
    batches.push_back(std::move(batch));
  }
  return batches;
}

std::vector<std::vector<Record>> MakeAggregateBatches(uint64_t num_batches,
                                                      uint64_t batch_size) {
  uint64_t record_id = 0;
  SchemaRef schema = utils::MakeAggregateSchema();
  std::vector<std::vector<Record>> batches;
  for (uint64_t i = 0; i < num_batches; i++) {
    std::vector<Record> batch;
    for (uint64_t j = 0; j < batch_size; j++) {
      // Since record_id is sequentially generated, if the matview is keyed
      // on the same column then the records will be equally distributed among
      // all the partitions since partition::HashPartition computes a modulo
      // on the column and then decides which partition the record belongs to.
      batch.emplace_back(schema, true, record_id, record_id, 1_u);
      record_id += 1;
    }
    batches.push_back(std::move(batch));
  }
  return batches;
}

std::vector<std::vector<Record>> MakeIdentityBatches(uint64_t num_batches,
                                                     uint64_t batch_size) {
  return MakeFilterBatches(num_batches, batch_size);
}

std::shared_ptr<DataFlowGraph> MakeFilterGraph(ColumnID keycol,
                                               const SchemaRef &schema) {
  std::vector<ColumnID> keys = {keycol};
  auto g = std::make_shared<DataFlowGraph>();

  auto in = std::make_shared<InputOperator>("test-table", schema);
  auto filter = std::make_shared<FilterOperator>();
  filter->AddOperation(0UL, 0,
                       FilterOperator::Operation::GREATER_THAN_OR_EQUAL);
  auto matview = std::make_shared<KeyOrderedMatViewOperator>(keys);

  g->AddInputNode(in);
  g->AddNode(filter, in);
  g->AddOutputOperator(matview, filter);
  return g;
}

std::shared_ptr<DataFlowGraph> MakeEquiJoinGraph(ColumnID ok, ColumnID lk,
                                                 ColumnID rk,
                                                 const SchemaRef &lschema,
                                                 const SchemaRef &rschema) {
  std::vector<ColumnID> keys = {ok};
  auto g = std::make_shared<DataFlowGraph>();

  auto in1 = std::make_shared<InputOperator>("test-table1", lschema);
  auto in2 = std::make_shared<InputOperator>("test-table2", rschema);
  auto join = std::make_shared<EquiJoinOperator>(lk, rk);
  auto matview = std::make_shared<UnorderedMatViewOperator>(keys);

  g->AddInputNode(in1);
  g->AddInputNode(in2);
  g->AddNode(join, {in1, in2});
  g->AddOutputOperator(matview, join);
  return g;
}

std::shared_ptr<DataFlowGraph> MakeAggregateGraph(
    ColumnID keycol, const SchemaRef &schema,
    std::vector<ColumnID> group_cols) {
  std::vector<ColumnID> keys = {keycol};
  auto g = std::make_shared<DataFlowGraph>();

  auto in = std::make_shared<InputOperator>("test-table", schema);
  auto aggregate = std::make_shared<AggregateOperator>(
      group_cols, AggregateOperator::Function::COUNT, -1);
  auto matview = std::make_shared<UnorderedMatViewOperator>(keys);

  g->AddInputNode(in);
  g->AddNode(aggregate, in);
  g->AddOutputOperator(matview, aggregate);
  return g;
}

std::shared_ptr<DataFlowGraph> MakeIdentityGraph(ColumnID keycol,
                                                 const SchemaRef &schema) {
  std::vector<ColumnID> keys = {keycol};
  auto g = std::make_shared<DataFlowGraph>();

  auto in = std::make_shared<InputOperator>("test-table", schema);
  auto identity = std::make_shared<IdentityOperator>();
  auto matview = std::make_shared<KeyOrderedMatViewOperator>(keys);

  g->AddInputNode(in);
  g->AddNode(identity, in);
  g->AddOutputOperator(matview, identity);
  return g;
}

}  // namespace utils
}  // namespace dataflow
}  // namespace pelton
