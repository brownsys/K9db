#include "pelton/dataflow/benchmark/driver.h"
#include "pelton/dataflow/benchmark/utils.h"

// NOTE: Ensure that #batches % #clients = 0
DEFINE_uint64(graph_type, 0, "GraphType as defined in benchmark_utils.h");
DEFINE_uint64(num_batches, 300000, "Number of batches");
DEFINE_uint64(batch_size, 1, "Number of records per batch");
DEFINE_uint64(num_clients, 3, "Number of clients");
DEFINE_uint64(num_partitions, 3,
              "Number of partitions the dataflow is sharded into");

int main(int argc, char **argv) {
  // Command line arugments and help message.
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Initialize Google’s logging library.
  google::InitGoogleLogging("benchmark");

  // Read MySql configurations.
  const uint64_t graph_type = FLAGS_graph_type;
  const uint64_t num_batches = FLAGS_num_batches;
  const uint64_t batch_size = FLAGS_batch_size;
  const uint64_t num_clients = FLAGS_num_clients;
  const uint64_t num_partitions = FLAGS_num_partitions;

  pelton::dataflow::utils::GraphType type =
      (pelton::dataflow::utils::GraphType)graph_type;
  pelton::dataflow::Driver driver(type, num_batches, batch_size, num_clients,
                                  num_partitions);
  driver.Execute();
}
