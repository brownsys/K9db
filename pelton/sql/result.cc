#include "pelton/sql/result.h"

#include <string>
#include <unordered_set>

#include "glog/logging.h"

namespace pelton {
namespace sql {

// SqlResultSet.
void SqlResultSet::Append(SqlResultSet &&other, bool deduplicate) {
  if (this->schema_ != other.schema_) {
    LOG(FATAL) << "Appending different schemas";
  }
  if (deduplicate) {
    CHECK_EQ(this->keys_.size(), this->records_.size())
        << "Deduplicating keyless result set";
    CHECK_EQ(other.keys_.size(), other.records_.size())
        << "Deduplicating keyless result set";
  }
  // Deduplicate using a hash set.
  std::unordered_set<std::string> duplicates;
  if (deduplicate) {
    for (const std::string &key : this->keys_) {
      duplicates.insert(key);
    }
  }
  for (size_t i = 0; i < other.records_.size(); i++) {
    if (!deduplicate || duplicates.count(other.keys_.at(i)) == 0) {
      this->records_.push_back(std::move(other.records_.at(i)));
      this->keys_.push_back(std::move(other.keys_.at(i)));
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

}  // namespace sql
}  // namespace pelton
