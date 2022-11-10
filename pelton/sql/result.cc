#include "pelton/sql/result.h"

#include <string>
#include <unordered_set>

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
void SqlResultSet::Append(SqlResultSet &&other, bool deduplicate) {
  if (this->schema_ != other.schema_) {
    LOG(FATAL) << "Appending different schemas";
  }
  // Deduplicate using a hash set.
  std::unordered_set<std::string> duplicates;
  if (deduplicate) {
    // Store keys of existing records.
    for (size_t i = 0; i < this->records_.size(); i++) {
      std::string key = this->records_.at(i).GetValueString(this->schema_.keys().front());
      duplicates.insert(key);
    }
  }
  for (size_t i = 0; i < other.records_.size(); i++) {
    std::string key = other.records_.at(i).GetValueString(other.schema_.keys().front());
    if (!deduplicate || duplicates.count(key) == 0) {
      this->records_.push_back(std::move(other.records_.at(i)));
    }
  }
}

// SqlResult.
void SqlResult::Append(SqlResult &&other, bool deduplicate) {
  CHECK(this->type_ == other.type_) << "Append different types";
  switch (this->type_) {
    case Type::STATEMENT: {
      if (this->status_ != false) {
        this->status_ = other.status_;
      }
      break;
    }
    case Type::UPDATE: {
      if (other.status_ < 0) {
        this->status_ = other.status_;
      } else if (this->status_ >= 0) {
        this->status_ += other.status_;
      }
      if (this->lid_ != 0 && other.lid_ != 0) {
        LOG(FATAL) << "Appending results with different last insert id!";
      }
      if (this->lid_ == 0) {
        this->lid_ = other.lid_;
      }
      break;
    }
    case Type::QUERY: {
      if (this->sets_.size() != 1 || other.sets_.size() != 1) {
        LOG(FATAL) << "Appending bad ResultSet size";
      }
      this->sets_.front().Append(std::move(other.sets_.front()), deduplicate);
      break;
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
