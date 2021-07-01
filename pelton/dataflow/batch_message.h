#ifndef PELTON_DATAFLOW_BATCH_MESSAGE_H_
#define PELTON_DATAFLOW_BATCH_MESSAGE_H_

#include <string>
#include <vector>

#include "pelton/dataflow/types.h"
#include "pelton/dataflow/record.h"

namespace pelton{
namespace dataflow{

class BatchMessage {
 public:
  BatchMessage(std::string input_name, NodeIndex entry_index,
               NodeIndex source_index, std::vector<Record> records)
      : input_name(input_name),
        entry_index(entry_index),
        source_index(source_index),
        records(std::move(records)) {}
        
  std::string input_name;
  NodeIndex entry_index;
  NodeIndex source_index;
  std::vector<Record> records;
};

} // namespace datalfow
} // namespace pelton

#endif  // PELTON_DATAFLOW_BATCH_MESSAGE_H_
