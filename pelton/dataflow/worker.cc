#include "pelton/dataflow/worker.h"

#include "pelton/dataflow/batch_message.h"
#include "pelton/dataflow/graph.h"

namespace pelton {
namespace dataflow {

bool Worker::MonitorChannel(std::shared_ptr<Channel> channel) {
  std::unique_lock<std::mutex> lock(this->mtx_);
  this->chans_to_monitor_.push_back(channel);
  return true;
}

bool Worker::AddPartitionedGraph(std::shared_ptr<DataFlowGraph> graph) {
  CHECK_EQ(graph->partition_id(), this->partition_id_);
  std::unique_lock<std::mutex> lock(this->mtx_);
  this->graphs_.emplace(graph->index(), graph);
  return true;
}

void Worker::NotifyWorker() {
  std::unique_lock<std::mutex> lock(this->notification_mtx_);
  if (!this->notified_) {
    this->notified_ = true;
  }
}

void Worker::Start() {
  // Continuously listen on channels.
  while (true) {
    std::unique_lock<std::mutex> lock(this->mtx_);
    // Before waiting on cv again check if this worker was notified while it was
    // busy reading from channels.
    this->notification_mtx_.lock();
    if (this->notified_) {
      // Reset
      this->notified_ = false;
      this->notification_mtx_.unlock();
    } else {
      this->notification_mtx_.unlock();
      this->condition_variable_->wait(lock);
    }

    for (auto channel : this->chans_to_monitor_) {
      std::optional<std::vector<std::shared_ptr<Message>>> messages =
          channel->Recv();
      if (messages.has_value()) {
        for (auto msg : messages.value()) {
          switch (msg->type()) {
            case Message::Type::BATCH: {
              auto batch_msg = std::dynamic_pointer_cast<BatchMessage>(msg);
              this->graphs_.at(channel->graph_index())
                  ->Process(batch_msg->destination_index(),
                            batch_msg->source_index(),
                            batch_msg->ConsumeRecords());
            } break;
            case Message::Type::STOP: {
              // Terminate this worker thread if a stop message is received
              // by any flow.
              return;
            } break;
            default:
              LOG(FATAL) << "Unsupported message type";
          }
        }
      }
    }
  }
}

}  // namespace dataflow
}  // namespace pelton
