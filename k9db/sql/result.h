#ifndef K9DB_SQL_RESULT_H_
#define K9DB_SQL_RESULT_H_

#include <iterator>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "k9db/dataflow/record.h"
#include "k9db/dataflow/schema.h"
#include "k9db/util/iterator.h"
#include "k9db/util/shard_name.h"

namespace k9db {
namespace sql {

// A SqlDeleteSet represents the results of a delete operations.
// It contains the records that were deleted, in addition to the shard each
// was deleted from.
class SqlDeleteSet {
 public:
  SqlDeleteSet() = default;

  size_t AddRecord(dataflow::Record &&record);
  void AssignToShard(size_t idx, std::string &&shard);

  std::vector<dataflow::Record> &&Vec() { return std::move(this->records_); }
  const std::vector<dataflow::Record> &Rows() const { return this->records_; }
  const std::unordered_map<util::ShardName, std::vector<size_t>> &Map() const {
    return this->shards_;
  }

  size_t Count() const { return this->count_; }

 private:
  size_t count_ = 0;
  std::vector<dataflow::Record> records_;
  std::unordered_map<util::ShardName, std::vector<size_t>> shards_;

  using MapIt = decltype(shards_)::const_iterator;
  using VecIt = std::vector<size_t>::const_iterator;

 public:
  // Iterator interface.
  using RecordsIt = decltype(records_)::const_iterator;
  using ShardsIt = util::MapItT<MapIt, const util::ShardName &>;
  using ShardRecordsIt = util::MapItT<VecIt, const dataflow::Record &>;

 public:
  util::Iterable<RecordsIt> IterateRecords() const;
  util::Iterable<ShardsIt> IterateShards() const;
  util::Iterable<ShardRecordsIt> Iterate(const util::ShardName &shard) const;
};

// A SqlUpdateSet represents the results of an update operation.
// It contains the records as they were before deletion, in addition to their
// new state after update.
// It also tracks the shards that these records are in.
class SqlUpdateSet {
 public:
  SqlUpdateSet() = default;

  size_t AddRecord(dataflow::Record &&old, dataflow::Record &&updated);
  void AssignToShard(size_t idx, std::string &&shard);

  std::vector<dataflow::Record> &&Vec() { return std::move(this->records_); }
  const std::vector<dataflow::Record> &Rows() const { return this->records_; }
  const std::unordered_map<util::ShardName, std::vector<size_t>> &Map() const {
    return this->shards_;
  }

  size_t Count() const { return this->count_; }

 private:
  size_t count_ = 0;
  std::vector<dataflow::Record> records_;
  std::unordered_map<util::ShardName, std::vector<size_t>> shards_;

  using MapIt = decltype(shards_)::const_iterator;
  using VecIt = std::vector<size_t>::const_iterator;

 public:
  using RecordPair =
      std::pair<const dataflow::Record &, const dataflow::Record &>;

  // Iterator interface.
  using RecordsIt = decltype(records_)::const_iterator;
  using ShardsIt = util::MapItT<MapIt, const util::ShardName &>;
  using ShardRecordsIt = util::MapItT<VecIt, RecordPair>;

 public:
  util::Iterable<RecordsIt> IterateRecords() const;
  util::Iterable<ShardsIt> IterateShards() const;
  util::Iterable<ShardRecordsIt> Iterate(const util::ShardName &shard) const;
};

// An SqlResultSet represents the content of a single table.
// An SqlResult may include multiple results when it reads from multiple tables!
class SqlResultSet {
 public:
  // Constructors.
  explicit SqlResultSet(const dataflow::SchemaRef &schema) : schema_(schema) {}

  SqlResultSet(const dataflow::SchemaRef &schema,
               std::vector<dataflow::Record> &&records)
      : schema_(schema), records_(std::move(records)) {}

  // Adding additional results to this set.
  void AppendDeduplicate(SqlResultSet &&other);
  void AppendMerge(SqlResultSet &&other);

  // Query API.
  const dataflow::SchemaRef &schema() const { return this->schema_; }
  std::vector<dataflow::Record> &&Vec() { return std::move(this->records_); }
  const std::vector<dataflow::Record> &rows() const { return this->records_; }
  inline size_t size() const { return this->records_.size(); }

  inline bool empty() const { return this->records_.empty(); }

 private:
  dataflow::SchemaRef schema_;
  std::vector<dataflow::Record> records_;
  std::unordered_set<std::string> lazy_keys_;
};

// Our version of an SqlResult.
// Might store a boolean or int status, or several ResultSets
class SqlResult {
 private:
  enum class Type { STATEMENT, UPDATE, QUERY };

 public:
  // Constructors.
  // For results of DDL (e.g. CREATE TABLE).
  // Success is true if and only if the DDL statement succeeded.
  explicit SqlResult(bool success) : type_(Type::STATEMENT), status_(success) {}
  // For results of DML (e.g. INSERT/UPDATE/DELETE).
  // row_count specifies how many rows were affected (-1 for failures).
  explicit SqlResult(int rows) : type_(Type::UPDATE), status_(rows), lid_(0) {}
  explicit SqlResult(size_t rows) : SqlResult(static_cast<int>(rows)) {}
  SqlResult(int rows, uint64_t lid)
      : type_(Type::UPDATE), status_(rows), lid_(lid) {}
  SqlResult(size_t rows, uint64_t lid)
      : SqlResult(static_cast<int>(rows), lid) {}
  // For results of SELECT.
  explicit SqlResult(std::vector<SqlResultSet> &&v)
      : type_(Type::QUERY), sets_(std::move(v)) {}
  explicit SqlResult(SqlResultSet &&resultset) : type_(Type::QUERY) {
    this->sets_.push_back(std::move(resultset));
  }
  explicit SqlResult(const dataflow::SchemaRef &schema) : type_(Type::QUERY) {
    this->sets_.emplace_back(schema);
  }

  // API for accessing the result.
  inline bool IsStatement() const { return this->type_ == Type::STATEMENT; }
  inline bool IsUpdate() const { return this->type_ == Type::UPDATE; }
  inline bool IsQuery() const { return this->type_ == Type::QUERY; }

  // Only safe to use if IsStatement() returns true.
  inline bool Success() const {
    if (!this->IsStatement())
      LOG(FATAL) << "Success() called on non-statement result (" << this->type_
                 << ")";
    return this->status_;
  }

  // Only safe to use if IsUpdate() returns true.
  inline int UpdateCount() const {
    if (!this->IsUpdate()) {
      LOG(FATAL) << "UpdateCount() called on non-update result (" << this->type_
                 << ")";
    }
    return this->status_;
  }
  inline uint64_t LastInsertId() const {
    if (!this->IsUpdate()) {
      LOG(FATAL) << "LastInsertId() called on non-update result ("
                 << this->type_ << ")";
    }
    return this->lid_;
  }

  // Only safe to use if IsQuery() returns true.
  const std::vector<SqlResultSet> &ResultSets() const {
    if (!this->IsQuery()) {
      LOG(FATAL) << "ResultSets() called on non-query result";
    }
    return this->sets_;
  }
  std::vector<SqlResultSet> &ResultSets() {
    if (!this->IsQuery()) {
      LOG(FATAL) << "ResultSets() called on non-query result";
    }
    return this->sets_;
  }

  inline void AddResultSet(SqlResultSet &&resultset) {
    this->sets_.push_back(std::move(resultset));
  }

  friend std::ostream &operator<<(std::ostream &str,
                                  const SqlResult::Type &res);

 private:
  Type type_;
  int status_;
  uint64_t lid_;
  std::vector<SqlResultSet> sets_;
};

}  // namespace sql
}  // namespace k9db

#endif  // K9DB_SQL_RESULT_H_
