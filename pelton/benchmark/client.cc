#include "pelton/benchmark/client.h"

#include <cstdlib>
// NOLINTNEXTLINE
#include <chrono>
#include <memory>
#include <utility>

#include "glog/logging.h"
#include "pelton/sqlast/ast_schema.h"

#define TLOG LOG(ERROR) << "Client " << this->index_ << " "

namespace pelton {
namespace benchmark {

namespace {

std::string RandomString() {
  static const char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  // Randomly select length, then fill in with random alphanums.
  size_t size = std::rand() % 10;
  std::string str = "";
  str.reserve(size);
  for (size_t i = 0; i < size; i++) {
    str.push_back(alphanum[std::rand() % (sizeof(alphanum) - 1)]);
  }
  return str;
}

}  // namespace

// Batch Generation.
std::vector<dataflow::Record> Client::GenerateBatch(const std::string &table,
                                                    dataflow::SchemaRef schema,
                                                    size_t batch_size) {
  std::vector<dataflow::Record> records;
  records.reserve(batch_size);
  for (size_t i = 0; i < batch_size; i++) {
    // Generate random records repeatedly until one hashes to the right
    // partition.
    while (true) {
      dataflow::Record record{schema, true};
      for (size_t j = 0; j < schema.size(); j++) {
        switch (schema.TypeOf(j)) {
          case pelton::sqlast::ColumnDefinition::Type::UINT:
            record.SetUInt(std::rand(), j);
            break;
          case pelton::sqlast::ColumnDefinition::Type::INT:
            record.SetInt(std::rand(), j);
            break;
          case pelton::sqlast::ColumnDefinition::Type::TEXT:
            record.SetString(std::make_unique<std::string>(RandomString()), j);
            break;
          case pelton::sqlast::ColumnDefinition::Type::DATETIME:
          default:
            LOG(FATAL) << "Unsupported column type";
        }
      }
      // See if record hash to the right thing.
      const auto &graph = this->state_->GetFlow("bench_flow");
      const auto &inkey = graph.input_partition_key(table);
      if (record.Hash(inkey) % this->state_->workers() == this->index_) {
        records.push_back(std::move(record));
        break;
      }
    }
  }
  return records;
}

// Entry Point.
std::chrono::time_point<std::chrono::system_clock> Client::Benchmark(
    size_t batch_size, size_t batch_count) {
  TLOG << "Starting..." << batch_size << " " << batch_count;
  std::vector<std::string> tables = this->state_->GetTables();

  // Prime all inputs except one.
  for (size_t i = 0; i < tables.size() - 1; i++) {
    const std::string &table = tables.at(i);
    dataflow::SchemaRef schema = this->state_->GetTableSchema(table);
    TLOG << "Priming " << table << "...";
    for (size_t j = 0; j < batch_count; j++) {
      auto batch = this->GenerateBatch(table, schema, batch_size);
      this->state_->ProcessRecords(table, std::move(batch));
    }
  }
  TLOG << "Done Priming!";

  // Generate all batches in one go.
  TLOG << "Creating benchmark batches...";
  const std::string &table = tables.back();
  dataflow::SchemaRef schema = this->state_->GetTableSchema(table);
  std::vector<std::vector<dataflow::Record>> batches;
  batches.reserve(batch_count);
  for (size_t i = 0; i < batch_count; i++) {
    batches.push_back(this->GenerateBatch(table, schema, batch_size));
  }

  // Benchmark!
  TLOG << "Benchmarking " << table << "...";
  auto start = std::chrono::high_resolution_clock::now();
  for (std::vector<dataflow::Record> &batch : batches) {
    this->state_->ProcessRecords(table, std::move(batch));
  }
  return start;
}

}  // namespace benchmark
}  // namespace pelton
