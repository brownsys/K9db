#ifndef PELTON_DATAFLOW_CHANNEL_H_
#define PELTON_DATAFLOW_CHANNEL_H_

#include <deque>
// NOLINTNEXTLINE
#include <mutex>
// NOLINTNEXTLINE
#include <semaphore>
// NOLINTNEXTLINE
#include <shared_mutex>
#include <string>
#include <vector>

#include "pelton/dataflow/future.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class Channel {
 public:
  // A batch of records with source and target operators.
  struct Message {
    std::string flow_name;
    std::vector<Record> records;
    NodeIndex source;
    NodeIndex target;
    Promise promise;
  };

  // Constructor.
  explicit Channel(size_t workers);

  // Called by consumer.
  std::vector<Message> Read();

  // Called by (worker) producers.
  void Write(PartitionIndex producer, Message &&msg);

  // Called by (client) producers.
  void WriteInput(Message &&msg);

  void Shutdown();

 private:
  // A queue for each producer (e.g. partition).
  std::vector<std::deque<Message>> queues_;
  std::deque<Message> input_queue_;
  // Synchronizes the many producers (shared) with the consumer (unique).
  // When a consumer is reading data from queue, this is acquired uniquely
  // and no produced can operate.
  std::shared_mutex mtx_;
  std::mutex input_mtx_;  // input_queue_ is shared between many producers.
  // A better notification variable that is lightweight and guarantees
  // notifications will never be lost in our design.
  std::binary_semaphore semaphore_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_CHANNEL_H_
