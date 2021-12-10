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
  std::unordered_set<std::string> duplicates;
  if (deduplicate) {
    for (size_t i = 0; i < this->records_.keys.size(); i++) {
      duplicates.insert(this->records_.keys.at(i));
    }
  }
  for (size_t i = 0; i < other.records_.records.size(); i++) {
    if (deduplicate && duplicates.count(other.records_.keys.at(i)) == 0) {
      this->records_.records.push_back(std::move(other.records_.records.at(i)));
      this->records_.keys.push_back(std::move(other.records_.keys.at(i)));
    }
  }
}

// SqlResult.
void SqlResult::Append(SqlResult &&other, bool deduplicate) {
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
    } break;
  }
}

}  // namespace sql
}  // namespace pelton
