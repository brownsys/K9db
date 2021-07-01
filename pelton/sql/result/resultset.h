#ifndef PELTON_SQL_RESULT_RESULTSET_H_
#define PELTON_SQL_RESULT_RESULTSET_H_

#include <list>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "mariadb/conncpp.hpp"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sql/eager_executor.h"

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
  SqlResultSet(const dataflow::SchemaRef &schema,
               SqlEagerExecutor *eager_executor);

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
  std::list<LazyResultSet> lazy_data_;
  std::vector<AugmentingInformation> current_augment_info_;
  std::unique_ptr<::sql::ResultSet> current_result_;
  dataflow::Record current_record_;
  bool current_record_ready_;
  dataflow::SchemaRef schema_;
  // Online deduplication as rows are read in from underlying results.
  bool deduplicate_;
  std::unordered_set<std::string> duplicates_;
  // Allows us to actually execute SQL commands when their data is being read.
  SqlEagerExecutor *eager_executor_;

  // Get the next record, from current_result_ or from the next result(s)
  // found by executing the next LazyResultSet(s). Store record in
  // current_record_.
  // Returns true if a record was found, and false if the resultsets are totally
  // consumed. Perform deduplication when needed.
  bool GetNext();
  // Actually execute SQL statement in LazyResultSet against the shards.
  void Execute();
};

}  // namespace _result
}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_RESULT_RESULTSET_H_
