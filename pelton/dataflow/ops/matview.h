#ifndef PELTON_DATAFLOW_OPS_MATVIEW_H_
#define PELTON_DATAFLOW_OPS_MATVIEW_H_

#include <memory>
// NOLINTNEXTLINE
#include <mutex>
// NOLINTNEXTLINE
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest_prod.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/grouped_data.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

// Virtual parent class: this is what the rest of our system uses and interacts
// with. Allows reading and writing into the materialized view without knowing
// the underlying storage layout and order.
class MatViewOperator : public Operator {
 public:
  // Cannot copy an operator.
  MatViewOperator(const MatViewOperator &other) = delete;
  MatViewOperator &operator=(const MatViewOperator &other) = delete;

  // Generic API.
  virtual bool RecordOrdered() const = 0;
  virtual bool KeyOrdered() const = 0;
  const std::vector<ColumnID> &key_cols() const { return this->key_cols_; }

  // Key API.
  virtual std::vector<Key> Keys(int limit = -1) const = 0;
  virtual size_t Count(const Key &key) const = 0;
  virtual bool Contains(const Key &key) const = 0;

  // Record API.
  virtual size_t count() const = 0;
  virtual std::vector<Record> All(int limit = -1) const = 0;
  virtual std::vector<Record> Lookup(const Key &key, int limit = -1,
                                     size_t offset = 0) const = 0;

  int limit() const { return this->limit_; }
  size_t offset() const { return this->offset_; }

 protected:
  // We do not know if we are ordered or unordered, this type is revealed
  // to us by the derived class as an argument.
  MatViewOperator(const std::vector<ColumnID> &key_cols, int limit,
                  size_t offset)
      : Operator(Operator::Type::MAT_VIEW),
        key_cols_(key_cols),
        limit_(limit),
        offset_(offset) {}

  // Data members.
  std::vector<ColumnID> key_cols_;
  int limit_;
  size_t offset_;

  // Allow tests to set input_schemas_ directly.
  FRIEND_TEST(MatViewOperatorTest, EmptyMatView);
  FRIEND_TEST(MatViewOperatorTest, SingleMatView);
  FRIEND_TEST(MatViewOperatorTest, SingleMatViewDifferentKey);
  FRIEND_TEST(MatViewOperatorTest, ProcessBatchTest);
  FRIEND_TEST(MatViewOperatorTest, OrderedKeyTest);
  FRIEND_TEST(MatViewOperatorTest, OrderedRecordTest);
  FRIEND_TEST(MatViewOperatorTest, EmptyKeyTest);
  FRIEND_TEST(MatViewOperatorTest, LimitTest);
  FRIEND_TEST(MatViewOperatorTest, LookupGreater);
  FRIEND_TEST(MatViewOperatorTest, AllOnRecordOrdered);
  FRIEND_TEST(MatViewOperatorTest, NightmareScenarioNegativePositiveOutOfOrder);
};

// Actual implementation is generic over T: the underlying GroupedDataT
// storage layout.
// A concrete instance of this class is constructor by providing a vector of
// columns, which constitute the keys of this view.
// Furthermore, if T specifies an underlying storage that is ordered over
// records, the constructor also expects an instance of Record::Compare, which
// the underlying storage uses to determine the ordering of records.
// Explicitly specified keys for this Materialized view may differ from
// PrimaryKey.
// For a detailed description of usage and the various cases, check out the
// extended comment inside ./grouped_data.h
template <typename T>
class MatViewOperatorT : public MatViewOperator {
 public:
  // Uncopyable.
  MatViewOperatorT(const MatViewOperatorT &other) = delete;
  MatViewOperatorT &operator=(const MatViewOperatorT &other) = delete;

  // Constructors: must provide a record comparator if record ordered.
  template <typename X = void,
            typename std::enable_if<T::NoCompare::value, X>::type * = nullptr>
  explicit MatViewOperatorT(const std::vector<ColumnID> &key_cols,
                            int limit = -1, size_t offset = 0)
      : MatViewOperator(key_cols, limit, offset), contents_() {}

  template <typename X = void,
            typename std::enable_if<!T::NoCompare::value, X>::type * = nullptr>
  MatViewOperatorT(const std::vector<ColumnID> &key_cols,
                   const Record::Compare &compare, int limit = -1,
                   size_t offset = 0)
      : MatViewOperator(key_cols, limit, offset), contents_(compare) {}

  // Override MatviewOperator functions.
  bool RecordOrdered() const override {
    if constexpr (std::is_same<T, RecordOrderedGroupedData>::value) {
      return true;
    } else {
      return false;
    }
  }
  bool KeyOrdered() const override {
    if constexpr (std::is_same<T, KeyOrderedGroupedData>::value) {
      return true;
    } else {
      return false;
    }
  }

  // Key API.
  size_t Count(const Key &key) const override {
    std::shared_lock<std::shared_mutex> lock(this->mtx_);
    return this->contents_.Count(key);
  }
  bool Contains(const Key &key) const override {
    std::shared_lock<std::shared_mutex> lock(this->mtx_);
    return this->contents_.Contains(key);
  }
  std::vector<Key> Keys(int limit = -1) const override {
    std::shared_lock<std::shared_mutex> lock(this->mtx_);
    return this->contents_.Keys(limit);
  }
  // Ordering instantiation specific API.
  template <typename X = void,
            typename std::enable_if<T::KeyOrdered::value, X>::type * = nullptr>
  std::vector<Key> KeysGreater(const Key &key, int limit = -1) const {
    std::shared_lock<std::shared_mutex> lock(this->mtx_);
    return this->contents_.KeysGreater(key, limit);
  }

  // Record API.
  size_t count() const override {
    std::shared_lock<std::shared_mutex> lock(this->mtx_);
    return this->contents_.count();
  }
  std::vector<Record> All(int limit = -1) const override {
    std::shared_lock<std::shared_mutex> lock(this->mtx_);
    return this->contents_.All(limit);
  }
  std::vector<Record> Lookup(const Key &key, int limit = -1,
                             size_t offset = 0) const override {
    std::shared_lock<std::shared_mutex> lock(this->mtx_);
    return this->contents_.Lookup(key, limit, offset);
  }

  // Ordering instantiation specific API.
  template <typename X = void,
            typename std::enable_if<!T::NoCompare::value, X>::type * = nullptr>
  const Record::Compare &comparator() const {
    return this->contents_.compare();
  }
  template <typename X = void,
            typename std::enable_if<!T::NoCompare::value, X>::type * = nullptr>
  std::vector<Record> All(const Record &cmp, int limit = -1) const {
    std::shared_lock<std::shared_mutex> lock(this->mtx_);
    return this->contents_.All(cmp, limit);
  }

  template <typename X = void,
            typename std::enable_if<!T::NoCompare::value, X>::type * = nullptr>
  std::vector<Record> LookupGreater(const Key &key, const Record &cmp,
                                    int limit = -1, size_t offset = 0) const {
    std::shared_lock<std::shared_mutex> lock(this->mtx_);
    return this->contents_.LookupGreater(key, cmp, limit, offset);
  }
  template <typename X = void,
            typename std::enable_if<T::KeyOrdered::value, X>::type * = nullptr>
  std::vector<Record> LookupGreater(const Key &key, int limit = -1) const {
    std::shared_lock<std::shared_mutex> lock(this->mtx_);
    return this->contents_.LookupGreater(key, limit);
  }

  // Override Operator functions.
  std::string DebugString() const override {
    std::string result = Operator::DebugString();
    result += "  \"keyed_by\": [";
    for (ColumnID key : this->key_cols_) {
      result += std::to_string(key) + ", ";
    }
    if (this->key_cols_.size() > 0) {
      result.pop_back();
      result.pop_back();
    }
    result += "],\n";
    std::string order_str = "no order";
    if (this->KeyOrdered()) {
      order_str = "Key ordered";
    } else if (this->RecordOrdered()) {
      order_str = "Record ordered";
    }
    result += "  \"order\": \"" + order_str + "\",\n";
    result += "  \"LIMIT\": " + std::to_string(this->limit_) + ",\n";
    result += "  \"OFFSET\": " + std::to_string(this->offset_) + ",\n";
    return result;
  }
  Record DebugRecord() const override {
    Record record = Operator::DebugRecord();
    std::string info = "[";
    for (auto key : this->key_cols_) {
      info += std::to_string(key) + ",";
    }
    if (this->key_cols_.size() > 0) {
      info.pop_back();
    }
    info += "]";
    if (this->KeyOrdered()) {
      info += " Key ordered";
    } else if (this->RecordOrdered()) {
      info += " Record ordered";
    } else {
      info += " no order";
    }
    info += " LIMIT = " + std::to_string(this->limit_);
    info += " OFFSET = " + std::to_string(this->offset_);
    record.SetString(std::make_unique<std::string>(info), 5);
    return record;
  }
  uint64_t SizeInMemory() const override {
    return this->contents_.SizeInMemory();
  }

 protected:
  // Override Operator functions.
  std::vector<Record> Process(NodeIndex source, std::vector<Record> &&records,
                              const Promise &promise) {
    std::unique_lock<std::shared_mutex> lock(this->mtx_);
    for (Record &r : records) {
      Key key = r.GetValues(this->key_cols_);
      if (r.IsPositive()) {
        if (!this->contents_.Insert(key, std::move(r))) {
          LOG(FATAL) << "Failed to insert record in matview";
        }
      } else {
        if (!this->contents_.Delete(key, std::move(r))) {
          LOG(FATAL) << "Failed to delete record in matview";
        }
      }
    }
    return {};
  }
  void ComputeOutputSchema() override {
    this->output_schema_ = this->input_schemas_.at(0);
    this->contents_.Initialize(this->output_schema_);
  }
  std::unique_ptr<Operator> Clone() const override {
    if constexpr (!T::NoCompare::value) {
      return std::make_unique<MatViewOperatorT<T>>(this->key_cols_,
                                                   this->contents_.compare(),
                                                   this->limit_, this->offset_);
    } else {
      return std::make_unique<MatViewOperatorT<T>>(this->key_cols_,
                                                   this->limit_, this->offset_);
    }
  }

 private:
  T contents_;
  mutable std::shared_mutex mtx_;
};

using UnorderedMatViewOperator = MatViewOperatorT<UnorderedGroupedData>;
using RecordOrderedMatViewOperator = MatViewOperatorT<RecordOrderedGroupedData>;
using KeyOrderedMatViewOperator = MatViewOperatorT<KeyOrderedGroupedData>;

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_MATVIEW_H_
