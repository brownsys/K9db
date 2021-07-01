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

// Abstract class.
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

  // Constructor.
  explicit SqlResultSet(const dataflow::SchemaRef &schema) : schema_(schema) {}
  virtual ~SqlResultSet() = default;

  // SqlResultSet API.
  virtual bool IsInline() const = 0;
  dataflow::SchemaRef GetSchema() { return this->schema_; }
  virtual bool HasNext() = 0;
  virtual dataflow::Record FetchOne() = 0;

  // Appending other SqlResultSets.
  virtual void Append(std::unique_ptr<SqlResultSet> &&other,
                      bool deduplicate) = 0;

  // Iterator API.
  SqlResultSet::Iterator begin() {
    return SqlResultSet::Iterator{this, this->schema_};
  }
  SqlResultSet::Iterator end() {
    return SqlResultSet::Iterator{nullptr, this->schema_};
  }

  // Consume the ResultSet returning all remaining records as a vector.
  virtual std::vector<dataflow::Record> Vectorize() = 0;

 protected:
  dataflow::SchemaRef schema_;
};

// SqlLazyResultSet consists of potentially many (lazy) portions, each coming
// in from some shard.
class SqlLazyResultSet : public SqlResultSet {
 public:
  struct LazyState {
    std::string sql;  // Statement to execute to get this shard's result.
    std::vector<AugmentingInformation> augment_info;
  };

  // Constructor.
  SqlLazyResultSet(const dataflow::SchemaRef &schema,
                   SqlEagerExecutor *eager_executor);

  // Adding additional results to this set.
  void AddShardResult(LazyState &&lazy_state);
  void Append(std::unique_ptr<SqlResultSet> &&other, bool deduplicate) override;

  // Query API.
  bool IsInline() const override { return false; }
  bool HasNext() override;
  dataflow::Record FetchOne() override;

  std::vector<dataflow::Record> Vectorize() override;

 private:
  std::list<LazyState> lazy_data_;
  std::vector<AugmentingInformation> current_augment_info_;
  std::unique_ptr<::sql::ResultSet> current_result_;
  dataflow::Record current_record_;
  bool current_record_ready_;
  // Online deduplication as rows are read in from underlying results.
  bool deduplicate_;
  std::unordered_set<std::string> duplicates_;
  // Allows us to actually execute SQL commands when their data is being read.
  SqlEagerExecutor *eager_executor_;

  // Get the next record, from current_result_ or from the next result(s)
  // found by executing the next LazyState(s). Store record in
  // current_record_.
  // Returns true if a record was found, and false if the resultsets are totally
  // consumed. Perform deduplication when needed.
  bool GetNext();
  // Actually execute SQL statement in LazyState against the shards.
  void Execute();
};

// SqlInlineResultSet consists of an inlined vector of records.
class SqlInlineResultSet : public SqlResultSet {
 public:
  // Constructor.
  explicit SqlInlineResultSet(const dataflow::SchemaRef &schema,
                              std::vector<dataflow::Record> &&records = {});

  // Adding additional results to this set.
  void Append(std::unique_ptr<SqlResultSet> &&other, bool deduplicate) override;

  // Query API.
  bool IsInline() const override { return true; }
  bool HasNext() override;
  dataflow::Record FetchOne() override;

  std::vector<dataflow::Record> Vectorize() override;

 private:
  size_t index_;
  std::vector<dataflow::Record> records_;
};

}  // namespace _result
}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_RESULT_RESULTSET_H_
