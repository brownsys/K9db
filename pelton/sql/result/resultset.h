#ifndef PELTON_SQL_RESULT_RESULTSET_H_
#define PELTON_SQL_RESULT_RESULTSET_H_

#include <memory>
#include <string>
#include <vector>

#include "mariadb/conncpp.hpp"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"

namespace pelton {
namespace sql {
namespace _result {

struct AugmentingInformation {
  size_t index;
  std::string value;
};

// ResultSet consists of potentially many (lazy) portions, each coming in from
// some shard.
struct LazyResultSet {
  std::string sql;  // Statement to execute to get this shard's result.
  std::vector<AugmentingInformation> augment_info;
};

class SqlResultSet {
 public:
  // Iterator that goes over all records in this result set.
  class Iterator {
   public:
    // Iterator traits.
    using difference_type = int64_t;
    using value_type = dataflow::Record;
    using iterator_category = std::input_iterator_tag;
    using pointer = dataflow::Record *;
    using reference = dataflow::Record &;

    // Construct the generic iterator by providing a concrete implemenation.
    Iterator(SqlResultSet *result_set, const dataflow::SchemaRef &schema)
        : result_set_(result_set), record_(schema) {
      ++(*this);
    }

    // All operations translate to operation on the underlying implementation.
    Iterator &operator++() {
      if (this->result_set_ && this->result_set_->HasNext()) {
        this->record_ = this->result_set_->FetchOne();
      } else {
        this->result_set_ = nullptr;
      }
      return *this;
    }

    // Check equality.
    bool operator==(const Iterator &o) const {
      return this->result_set_ == o.result_set_;
    }
    bool operator!=(const Iterator &o) const {
      return this->result_set_ != o.result_set_;
    }

    // Get current record.
    const dataflow::Record &operator*() const { return this->record_; }
    dataflow::Record &operator*() { return this->record_; }

   private:
    SqlResultSet *result_set_;
    dataflow::Record record_;
  };

  // Constructors.
  explicit SqlResultSet(const dataflow::SchemaRef &schema);

  // Adding additional results to this set.
  void AddShardResult(LazyResultSet &&lazy_result_set);
  void Append(SqlResultSet &&other, bool deduplicate);

  // Query API.
  dataflow::SchemaRef GetSchema() { return this->schema_; }
  bool HasNext();
  dataflow::Record FetchOne();

  // Iterator API.
  SqlResultSet::Iterator begin() {
    return SqlResultSet::Iterator{this, this->schema_};
  }
  SqlResultSet::Iterator end() {
    return SqlResultSet::Iterator{nullptr, this->schema_};
  }

 private:
  // index marks the current LazyResultSet.
  size_t index_;
  std::vector<LazyResultSet> lazy_data_;
  std::unique_ptr<::sql::ResultSet> current_result_;
  dataflow::SchemaRef schema_;

  // Actually execute SQL statement in LazyResultSet against the shards.
  void ExecuteLazy();
};

}  // namespace _result
}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_RESULT_RESULTSET_H_
