#include "pelton/sql/result.h"

#include <string>

#include "glog/logging.h"

namespace pelton {
namespace sql {

// SqlDeleteSet.
size_t SqlDeleteSet::AddRecord(dataflow::Record &&record) {
  this->records_.push_back(std::move(record));
  return this->records_.size() - 1;
}
void SqlDeleteSet::AssignToShard(size_t idx, std::string &&shard) {
  auto [it, _] = this->shards_.emplace(std::move(shard), 0);
  it->second.push_back(idx);
  this->count_++;
}

util::Iterable<SqlDeleteSet::RecordsIt> SqlDeleteSet::IterateRecords() const {
  return util::Iterable<RecordsIt>(this->records_.cbegin(),
                                   this->records_.cend());
}
util::Iterable<SqlDeleteSet::ShardsIt> SqlDeleteSet::IterateShards() const {
  return util::Map(&this->shards_,
                   [](MapIt::reference ref) -> const util::ShardName & {
                     return ref.first;
                   });
}
util::Iterable<SqlDeleteSet::ShardRecordsIt> SqlDeleteSet::Iterate(
    const util::ShardName &shard) const {
  const std::vector<size_t> &vec = this->shards_.at(shard);
  return util::Map(&vec, [&](VecIt::reference ref) -> const dataflow::Record & {
    return this->records_.at(ref);
  });
}

// SqlUpdateSet.
size_t SqlUpdateSet::AddRecord(dataflow::Record &&old,
                               dataflow::Record &&updated) {
  this->records_.push_back(std::move(old));
  this->records_.push_back(std::move(updated));
  return this->records_.size() - 2;
}
void SqlUpdateSet::AssignToShard(size_t idx, std::string &&shard) {
  auto [it, _] = this->shards_.emplace(std::move(shard), 0);
  it->second.emplace_back(idx);
  this->count_++;
}
util::Iterable<SqlUpdateSet::RecordsIt> SqlUpdateSet::IterateRecords() const {
  return util::Iterable<RecordsIt>(this->records_.cbegin(),
                                   this->records_.cend());
}
util::Iterable<SqlUpdateSet::ShardsIt> SqlUpdateSet::IterateShards() const {
  return util::Map(&this->shards_,
                   [](MapIt::reference ref) -> const util::ShardName & {
                     return ref.first;
                   });
}
util::Iterable<SqlUpdateSet::ShardRecordsIt> SqlUpdateSet::Iterate(
    const util::ShardName &shard) const {
  const std::vector<size_t> &vec = this->shards_.at(shard);
  return util::Map(&vec, [&](VecIt::reference ref) -> RecordPair {
    return RecordPair(this->records_.at(ref), this->records_.at(ref + 1));
  });
}

// SqlResultSet.
void SqlResultSet::AppendDeduplicate(SqlResultSet &&other) {
  if (this->schema_ != other.schema_) {
    LOG(FATAL) << "Appending different schemas";
  }

  // Deduplicate using a hash set.
  std::unordered_set<std::string> duplicates;
  size_t pk_index = this->schema_.keys().front();

  // Store keys of existing records.
  if (this->lazy_keys_.size() == 0) {
    for (size_t i = 0; i < this->records_.size(); i++) {
      std::string key = this->records_.at(i).GetValue(pk_index).AsSQLString();
      duplicates.insert(std::move(key));
    }
  }
  for (dataflow::Record &record : other.records_) {
    std::string key = record.GetValue(pk_index).AsSQLString();
    if (duplicates.insert(std::move(key)).second) {
      this->records_.push_back(std::move(record));
    }
  }
}

void SqlResultSet::AppendMerge(SqlResultSet &&other) {
  if (this->schema_ != other.schema_) {
    LOG(FATAL) << "Appending different schemas";
  }

  // Deduplicate using a hash set.
  std::unordered_map<std::string, dataflow::Record> consolidated;
  size_t pk_index = this->schema_.keys().front();

  // Store keys of existing records.
  if (this->lazy_keys_.size() == 0) {
    for (dataflow::Record &record : this->records_) {
      std::string key = record.GetValue(pk_index).AsSQLString();
      consolidated.insert({key, std::move(record)});
    }
  }
  for (dataflow::Record &record : other.records_) {
    std::string key = record.GetValue(pk_index).AsSQLString();
    auto it = consolidated.find(key);
    if (it == consolidated.end()) {
      consolidated.insert({key, std::move(record)});
    } else {
      dataflow::Record &merging_into = (*it).second;
      for (const std::string &column_name :
           merging_into.schema().column_names()) {
        size_t column_idx = merging_into.schema().IndexOf(column_name);
        if (merging_into.GetValue(column_idx).IsNull()) {
          merging_into.SetValue(record.GetValue(column_idx), column_idx);
        }
      }
    }
  }
  this->records_.clear();
  for (auto &p : consolidated) {
    this->records_.push_back(std::move(p.second));
  }
}

std::ostream &operator<<(std::ostream &s, const SqlResult::Type &res) {
  switch (res) {
    case SqlResult::Type::STATEMENT:
      s << "Statement";
      break;
    case SqlResult::Type::UPDATE:
      s << "Update";
      break;
    case SqlResult::Type::QUERY:
      s << "Query";
      break;
    default:
      s << "Impossible! SqlResult::Type enum has invalid value.";
  }
  return s;
}

}  // namespace sql
}  // namespace pelton
