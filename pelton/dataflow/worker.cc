#include "pelton/dataflow/worker.h"

#include "pelton/dataflow/batch_message.h"
#include "pelton/dataflow/graph.h"

namespace pelton {
namespace dataflow {

bool Worker::MonitorStopChannel(std::shared_ptr<Channel> channel) {
  std::unique_lock<std::mutex> lock(this->mtx_);
  this->stop_chan_ = channel;
  return true;
}

// The DataFlowEngine inserts channels such that the channels present towards
// the end of chans_to_monitor_ vector are for the exchange operators and
// channels present at the front are for the inputs. This is intended by design
// so that by the time worker is done consuming channels for the inputs, It
// will be more likely that channels reserved for the exchange ops will contain
// messages.
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

    // Check and process messages on channels.
    for (auto channel : this->chans_to_monitor_) {
      std::optional<std::vector<std::shared_ptr<Message>>> messages =
          channel->Recv();
      if (messages.has_value()) {
        for (auto msg : messages.value()) {
          switch (msg->type()) {
            case Message::Type::BATCH: {
              auto batch_msg = std::dynamic_pointer_cast<BatchMessage>(msg);
              this->graphs_.at(channel->graph_index())
                  ->Process(channel->destination_index(),
                            channel->source_index(),
                            batch_msg->ConsumeRecords());
            } break;
            case Message::Type::STOP:
              LOG(FATAL)
                  << "Stop is message not expected to be sent on this channel";
            default:
              LOG(FATAL) << "Unsupported message type";
          }
        }
      }
    }

    // Consume messages from the stop channel at the end so that in case if a
    // stop message is present, then messages from other channels have the
    // opportunity to get consumed before the worker thread terminates.
    std::optional<std::vector<std::shared_ptr<Message>>> messages =
        this->stop_chan_->Recv();
    if (messages.has_value()) {
      CHECK_EQ(messages.value().size(), static_cast<size_t>(1));
      auto stop_message = messages.value().at(0);
      assert(stop_message->type() == Message::Type::STOP);
      return;
    }
  }
}

}  // namespace dataflow
}  // namespace pelton
