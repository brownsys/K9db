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
  std::vector<dataflow::Record> GenerateBatch(const std::string &table,
                                              dataflow::SchemaRef schema,
                                              size_t batch_size);

  // Entry Point.
  std::chrono::time_point<std::chrono::system_clock> Benchmark(
      size_t batch_size, size_t batch_count);

 private:
  size_t index_;
  dataflow::DataFlowState *state_;
};

}  // namespace benchmark
}  // namespace pelton

#endif  // PELTON_BENCHMARK_CLIENT_H_
