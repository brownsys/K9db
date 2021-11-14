#include "pelton/dataflow/graph.h"

#include <algorithm>
#include <iterator>
#include <list>
#include <tuple>
#include <utility>
// NOLINTNEXTLINE
#include <variant>

#include "glog/logging.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/aggregate.h"
#include "pelton/dataflow/ops/equijoin.h"
#include "pelton/dataflow/ops/exchange.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/ops/project.h"
#include "pelton/dataflow/schema.h"
#include "pelton/util/merge_sort.h"

namespace pelton {
namespace dataflow {

// To avoid clutter, everything related to identifying what operator is
// partitioned by what key is in this file.
#include "pelton/dataflow/graph_traversal.inc"

// shortcut for std::make_move_iterator
#define MOVEIT(it) std::make_move_iterator(it)

// DataFlowGraph.
DataFlowGraph::DataFlowGraph(
    std::unique_ptr<DataFlowGraphPartition> &&partition,
    PartitionIndex partitions) {
  // Store the input names for this graph (i.e. for any partition).
  this->output_schema_ = partition->outputs().at(0)->output_schema();
  this->matview_keys_ = partition->outputs().at(0)->key_cols();
  for (const auto &[input_name, _] : partition->inputs()) {
    this->inputs_.push_back(input_name);
  }

  // Traverse the graph, adding the needed exchange operators, and deducing
  // the partitioning keys for the inputs.
  ProducerMap p;
  RequiredMap r;
  ExternalMap e;
  for (const auto &[_, input_operator] : partition->inputs()) {
    DFS(*input_operator, UNDEFINED_NODE_INDEX, Any::UNIT, &p, &r, e);
  }

  // Check to see that every MatView was assigned a partition key.
  // If some were not, assign them one externally.
  this->matview_partition_match_ = true;
  if (ExternallyAssignKey(partition->outputs(), p, &e)) {
    this->matview_partition_match_ = false;
    // Run again with the new external constraints
    p.clear();
    r.clear();
    for (const auto &[_, input_operator] : partition->inputs()) {
      DFS(*input_operator, UNDEFINED_NODE_INDEX, Any::UNIT, &p, &r, e);
    }
  }

  // Check to see that every input operator was assigned a partition key.
  // If some were not, assign them one externally.
  std::vector<InputOperator *> inputs(partition->inputs().size());
  std::transform(partition->inputs().begin(), partition->inputs().end(),
                 inputs.begin(), [](auto pair) { return pair.second; });
  if (ExternallyAssignKey(inputs, p, &e)) {
    p.clear();
    r.clear();
    for (const auto &[_, input_operator] : partition->inputs()) {
      DFS(*input_operator, UNDEFINED_NODE_INDEX, Any::UNIT, &p, &r, e);
    }
  }

  // Fill in partition key maps.
  for (const auto &[name, op] : partition->inputs()) {
    this->inkeys_.emplace(name, std::get<PartitionKey>(p.at(op->index())));
  }
  for (auto op : partition->outputs()) {
    this->outkey_ = std::get<PartitionKey>(p.at(op->index()));
  }

  // Add in any needed exchanges.
  std::vector<std::tuple<PartitionKey, Operator *, Operator *>> exchanges;
  for (NodeIndex i = 0; i < partition->Size(); i++) {
    Operator *op = partition->GetNode(i);
    for (Operator *child : op->children()) {
      auto result = ExchangeKey(op, child, p, r);
      if (result) {
        exchanges.emplace_back(std::move(result.value()), op, child);
      }
    }
  }
  for (const auto &[key, parent, child] : exchanges) {
    partition->InsertNode(
        std::make_unique<ExchangeOperator>(&this->partitions_, key), parent,
        child);
  }

  // Print debugging information.
  LOG(INFO) << "Planned exchanges and partitioning of flow";
  LOG(INFO) << partition->DebugString();

  // Make the specified number of partitions.
  for (PartitionIndex i = 0; i < partitions; i++) {
    this->partitions_.push_back(partition->Clone(i));
    this->matviews_.push_back(this->partitions_.back()->outputs().at(0));
  }
}

// Processing records: records are hash-partitioning and fed to the input
// operator of corresponding partion.
void DataFlowGraph::Process(const std::string &input_name,
                            std::vector<Record> &&records) {
  // Map each input record to its output partition.
  std::unordered_map<PartitionIndex, std::vector<Record>> partitioned;
  for (Record &record : records) {
    PartitionIndex partition =
        record.Hash(this->inkeys_.at(input_name)) % this->partitions_.size();
    partitioned[partition].push_back(std::move(record));
  }

  // Send records to each partition.
  for (auto &[partition, records] : partitioned) {
    this->partitions_.at(partition)->Process(input_name, std::move(records));
  }
}

// Reading records from views by different constraints or orders.
std::vector<Record> DataFlowGraph::All(int limit, size_t offset) const {
  int olimit = limit > -1 ? limit + offset : -1;
  auto matview = this->matviews_.front();
  if (matview->RecordOrdered()) {
    // Order actually matters.
    auto ordview = static_cast<RecordOrderedMatViewOperator *>(matview);
    std::list<Record> records;
    for (auto matview : this->matviews_) {
      util::MergeInto(&records, matview->All(olimit), ordview->comparator(),
                      olimit);
    }
    return util::ToVector(&records, limit, offset);
  } else {
    // Order does not matter.
    std::vector<Record> records;
    for (auto matview : this->matviews_) {
      if (records.size() == 0) {
        records = matview->All(olimit);
      } else {
        auto tmp = matview->All(olimit);
        records.insert(records.end(), MOVEIT(tmp.begin()), MOVEIT(tmp.end()));
      }
    }
    util::Trim(&records, limit, offset);
    return records;
  }
}
std::vector<Record> DataFlowGraph::Lookup(const Key &key, int limit,
                                          size_t offset) const {
  if (this->matview_partition_match_) {
    PartitionIndex partition = key.Hash() % this->partitions_.size();
    return this->matviews_.at(partition)->Lookup(key, limit, offset);
  } else {
    int olimit = limit > -1 ? limit + offset : -1;
    auto matview = this->matviews_.front();
    if (matview->RecordOrdered()) {
      // Order actually matters.
      auto ordview = static_cast<RecordOrderedMatViewOperator *>(matview);
      std::list<Record> records;
      for (auto matview : this->matviews_) {
        util::MergeInto(&records, matview->Lookup(key, olimit),
                        ordview->comparator(), olimit);
      }
      return util::ToVector(&records, limit, offset);
    } else {
      // Order does not matter.
      std::vector<Record> records;
      for (auto matview : this->matviews_) {
        if (records.size() == 0) {
          records = matview->Lookup(key, olimit);
        } else {
          auto tmp = matview->Lookup(key, olimit);
          records.insert(records.end(), MOVEIT(tmp.begin()), MOVEIT(tmp.end()));
        }
      }
      util::Trim(&records, limit, offset);
      return records;
    }
  }
}
std::vector<Record> DataFlowGraph::Lookup(const std::vector<Key> &keys,
                                          int limit, size_t offset) const {
  if (keys.size() == 1) {
    return this->Lookup(keys.front(), limit, offset);
  }
  // Iterate over keys.
  int olimit = limit > -1 ? limit + offset : -1;
  auto matview = this->matviews_.front();
  if (matview->RecordOrdered()) {
    // Order actually matters.
    auto ordview = static_cast<RecordOrderedMatViewOperator *>(matview);
    std::list<Record> records;
    for (const auto &key : keys) {
      util::MergeInto(&records, this->Lookup(key, olimit, 0),
                      ordview->comparator(), olimit);
    }
    return util::ToVector(&records, limit, offset);
  } else {
    // Order does not matter.
    std::vector<Record> records;
    for (const auto &key : keys) {
      if (records.size() == 0) {
        records = this->Lookup(key, olimit, 0);
      } else {
        auto tmp = this->Lookup(key, olimit, 0);
        records.insert(records.end(), MOVEIT(tmp.begin()), MOVEIT(tmp.end()));
      }
    }
    util::Trim(&records, limit, offset);
    return records;
  }
}
std::vector<Record> DataFlowGraph::AllRecordGreater(const Record &cmp,
                                                    int limit,
                                                    size_t offset) const {
  // Must be a Record Ordered Matview.
  auto matview = this->matviews_.front();
  if (!matview->RecordOrdered()) {
    LOG(FATAL) << "Attempting to query incompatible view by record order";
  }
  // Get the records from all partitions and merge them.
  int olimit = limit > -1 ? limit + offset : -1;
  auto ordview = static_cast<RecordOrderedMatViewOperator *>(matview);
  std::list<Record> records;
  for (auto matview : this->matviews_) {
    ordview = static_cast<RecordOrderedMatViewOperator *>(matview);
    util::MergeInto(&records, ordview->All(cmp, olimit), ordview->comparator(),
                    olimit);
  }
  return util::ToVector(&records, limit, offset);
}
std::vector<Record> DataFlowGraph::LookupRecordGreater(const Key &key,
                                                       const Record &cmp,
                                                       int limit,
                                                       size_t offset) const {
  // Must be a Record Ordered Matview.
  auto matview = this->matviews_.front();
  if (!matview->RecordOrdered()) {
    LOG(FATAL) << "Attempting to query incompatible view by record order";
  }
  // Easy if partition keys match matview keys.
  if (this->matview_partition_match_) {
    PartitionIndex partition = key.Hash() % this->partitions_.size();
    auto matview = this->matviews_.at(partition);
    auto ordview = static_cast<RecordOrderedMatViewOperator *>(matview);
    return ordview->LookupGreater(key, cmp, limit, offset);
  } else {
    // We do not know which partition, must try them all.
    int olimit = limit > -1 ? limit + offset : -1;
    std::list<Record> records;
    for (auto matview : this->matviews_) {
      auto ordview = static_cast<RecordOrderedMatViewOperator *>(matview);
      util::MergeInto(&records, ordview->LookupGreater(key, cmp, olimit),
                      ordview->comparator(), olimit);
    }
    return util::ToVector(&records, limit, offset);
  }
}
std::vector<Record> DataFlowGraph::LookupRecordGreater(
    const std::vector<Key> &keys, const Record &cmp, int limit,
    size_t offset) const {
  // Must be a Record Ordered Matview.
  auto matview = this->matviews_.front();
  if (!matview->RecordOrdered()) {
    LOG(FATAL) << "Attempting to query incompatible view by record order";
  }
  // Iterate over all keys.
  if (keys.size() == 1) {
    return this->Lookup(keys.front(), limit, offset);
  }
  // Iterate over keys.
  int olimit = limit > -1 ? limit + offset : -1;
  auto ordview = static_cast<RecordOrderedMatViewOperator *>(matview);
  std::list<Record> records;
  for (const auto &key : keys) {
    util::MergeInto(&records, this->LookupRecordGreater(key, cmp, olimit, 0),
                    ordview->comparator(), olimit);
  }
  return util::ToVector(&records, limit, offset);
}
std::vector<Record> DataFlowGraph::LookupKeyGreater(const Key &key, int limit,
                                                    size_t offset) const {
  // Must be a Key Ordered Matview.
  auto matview = this->matviews_.front();
  if (!matview->KeyOrdered()) {
    LOG(FATAL) << "Attempting to query incompatible view by record order";
  }
  // Get all the keys sorted.
  std::list<Key> keys;
  for (auto matview : this->matviews_) {
    auto ordview = static_cast<KeyOrderedMatViewOperator *>(matview);
    util::MergeInto(&keys, ordview->KeysGreater(key));
  }
  // Get all the records according to the order of the keys.
  int olimit = limit > -1 ? static_cast<size_t>(limit) + offset : -1;
  std::vector<Record> records;
  for (const auto &key : keys) {
    auto tmp = this->Lookup(key, olimit, 0);
    records.insert(records.end(), MOVEIT(tmp.begin()), MOVEIT(tmp.end()));
  }
  util::Trim(&records, limit, offset);
  return records;
}

/*
void test(Key key, const Record &cmp, int limit, size_t offset) {
  int olimit = limit > -1 ? limit + offset : -1;
  // Greater KEY
  //auto ord_view = static_cast<dataflow::KeyOrderedMatViewOperator *>(matview);
  //auto result = ord_view->LookupGreater(*condition.greater_key, olimit);
  //util::Trim(&result, limit, offset);
  // Greater RECORD
  // By ordered record and potentially key(s).
  if (matview->RecordOrdered()) {
    auto ord_view =
        static_cast<dataflow::RecordOrderedMatViewOperator *>(matview);
    if (!condition.greater_record) {
      if (!condition.equality_keys) {
        auto result = ord_view->All(olimit);
        util::Trim(&result, limit, offset);
        return result;
      } else if (condition.equality_keys->size() == 1) {
        return ord_view->Lookup(condition.equality_keys->at(0), limit, offset);
      } else {
        std::list<dataflow::Record> result;
        for (const auto &key : *condition.equality_keys) {
          util::MergeInto(&result, ord_view->Lookup(key, olimit),
                          ord_view->comparator(), olimit);
        }
        return util::ToVector(&result, limit, offset);
      }
    } else {
      if (!condition.equality_keys) {
        auto result = ord_view->All(*condition.greater_record, olimit);
        util::Trim(&result, limit, offset);
        return result;
      } else if (condition.equality_keys->size() == 1) {
        return ord_view->LookupGreater(condition.equality_keys->at(0),
                                       *condition.greater_record, limit,
                                       offset);
      } else {
        std::list<dataflow::Record> result;
        for (const auto &key : *condition.equality_keys) {
          util::MergeInto(
              &result,
              ord_view->LookupGreater(key, *condition.greater_record, olimit),
              ord_view->comparator(), olimit);
        }
        return util::ToVector(&result, limit, offset);
      }
    }
  }

  // By key(s).
  if (!condition.equality_keys) {
    auto result = matview->All(olimit);
    util::Trim(&result, limit, offset);
    return result;
  } else if (condition.equality_keys->size() == 1) {
    return matview->Lookup(condition.equality_keys->at(0), limit, offset);
  } else {
    std::vector<dataflow::Record> result;
    for (const auto &key : *condition.equality_keys) {
      auto vec = matview->Lookup(key, olimit);
      result.insert(result.end(), std::make_move_iterator(vec.begin()),
                    std::make_move_iterator(vec.end()));
    }
    util::Trim(&result, limit, offset);
    return result;
  }
}
*/

// Debugging information.
uint64_t DataFlowGraph::SizeInMemory() const {
  uint64_t total_size = 0;
  for (const auto &partition : this->partitions_) {
    total_size += partition->SizeInMemory();
  }
  return total_size;
}

}  // namespace dataflow
}  // namespace pelton
