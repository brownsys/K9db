#include "pelton/sql/result.h"

#include <string>
#include <unordered_set>

#include "glog/logging.h"

namespace pelton {
namespace sql {

namespace {

// Turn record into a concatenated string.
std::string StringifyRecord(const dataflow::Record &record) {
  std::string delim("\0", 1);
  std::string str = "";
  for (size_t i = 0; i < record.schema().size(); i++) {
    str += record.GetValueString(i) + delim;
  }
  return str;
}

}  // namespace

// SqlResultSet.
void SqlResultSet::Append(SqlResultSet &&other, bool deduplicate) {
  if (this->schema_ != other.schema_) {
    LOG(FATAL) << "Appending different schemas";
  }
  std::unordered_set<std::string> duplicates;
  for (const dataflow::Record &r : this->records_) {
    duplicates.insert(StringifyRecord(r));
  }
  for (dataflow::Record &r : other.records_) {
    if (duplicates.count(StringifyRecord(r)) == 0) {
      this->records_.push_back(std::move(r));
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
