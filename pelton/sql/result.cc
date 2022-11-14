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
