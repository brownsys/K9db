#ifndef PELTON_BENCHMARK_CLIENT_H_
#define PELTON_BENCHMARK_CLIENT_H_

#include <string>
#include <vector>

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/state.h"

namespace pelton {
namespace benchmark {

class Client {
 public:
  Client(uint64_t index, dataflow::DataFlowState *state)
      : index_(index), state_(state) {}

  // Batch Generation.
  std::vector<dataflow::Record> GenerateBatch(dataflow::SchemaRef schema,
                                              size_t batch_size);

  // Batch computation.
  uint64_t BenchmarkBatch(const std::string &table,
                          std::vector<dataflow::Record> &&records);

  // Entry Point.
  void Benchmark(size_t batch_size, size_t batch_count);

 private:
  size_t index_;
  dataflow::DataFlowState *state_;
};

}  // namespace benchmark
}  // namespace pelton

#endif  // PELTON_BENCHMARK_CLIENT_H_
