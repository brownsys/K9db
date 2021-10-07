#include "pelton/benchmark/client.h"

// NOLINTNEXTLINE
#include <chrono>
#include <memory>
#include <utility>

#include "glog/logging.h"
#include "pelton/sqlast/ast_schema.h"

#define TLOG LOG(ERROR) << "Client " << this->index_ << " "

namespace pelton {
namespace benchmark {

// Batch Generation.
std::vector<dataflow::Record> Client::GenerateBatch(dataflow::SchemaRef schema,
                                                    size_t batch_size) {
  std::vector<dataflow::Record> records;
  records.reserve(batch_size);
  for (size_t i = 0; i < batch_size; i++) {
    records.emplace_back(schema, true);
    for (size_t j = 0; j < schema.size(); j++) {
      switch (schema.TypeOf(j)) {
        case pelton::sqlast::ColumnDefinition::Type::UINT:
          records.back().SetUInt(0, j);
          break;
        case pelton::sqlast::ColumnDefinition::Type::INT:
          records.back().SetInt(0, j);
          break;
        case pelton::sqlast::ColumnDefinition::Type::TEXT:
          records.back().SetString(std::make_unique<std::string>("test"), j);
          break;
        case pelton::sqlast::ColumnDefinition::Type::DATETIME:
        default:
          LOG(FATAL) << "Unsupported column type";
      }
    }
  }
  return records;
}

// Batch computation.
uint64_t Client::BenchmarkBatch(const std::string &table,
                                std::vector<dataflow::Record> &&records) {
  // Process the records.
  auto start = std::chrono::high_resolution_clock::now();
  this->state_->ProcessRecords(table, std::move(records));

  // Measure time.
  auto end = std::chrono::high_resolution_clock::now();
  auto diff = end - start;
  return std::chrono::duration_cast<std::chrono::nanoseconds>(diff).count();
}

// Entry Point.
void Client::Benchmark(size_t batch_size, size_t batch_count) {
  TLOG << "Starting...";
  auto start = std::chrono::high_resolution_clock::now();
  std::vector<std::string> tables = this->state_->GetTables();

  // Prime all inputs except one.
  for (size_t i = 0; i < tables.size() - 1; i++) {
    const std::string &table = tables.at(i);
    dataflow::SchemaRef schema = this->state_->GetTableSchema(table);
    TLOG << "Priming " << table << "...";
    for (size_t j = 0; j < batch_count; j++) {
      auto batch = this->GenerateBatch(schema, batch_size);
      this->state_->ProcessRecords(table, std::move(batch));
    }
  }
  TLOG << "Done Priming!";

  // Benchmark with the last remaining flow input.
  uint64_t total_time = 0;
  const std::string &table = tables.back();
  dataflow::SchemaRef schema = this->state_->GetTableSchema(table);
  TLOG << "Benchmarking " << table << "...";
  for (size_t i = 0; i < batch_count; i++) {
    auto batch = this->GenerateBatch(schema, batch_size);
    total_time += this->BenchmarkBatch(table, std::move(batch));
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto diff = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
  TLOG << "Total time: " << (diff.count() / 1000.0 / 1000 / 1000);

  double throughput = total_time / 1000.0 / 1000 / 1000;
  throughput = (batch_size * batch_count) / throughput;
  TLOG << "Measured time: " << total_time << "ns";
  TLOG << "Throughput: " << throughput << " records per second";
}

}  // namespace benchmark
}  // namespace pelton
