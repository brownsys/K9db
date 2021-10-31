#ifndef PELTON_DATAFLOW_OPS_MATVIEW_H_
#define PELTON_DATAFLOW_OPS_MATVIEW_H_

#include <memory>
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

  virtual size_t count() const = 0;
  virtual const std::vector<ColumnID> &key_cols() const = 0;
  virtual bool Contains(const Key &key) const = 0;
  virtual const_RecordIterable Lookup(const Key &key, int limit = -1,
                                      size_t offset = 0) const = 0;
  virtual const_RecordIterable LookupGreater(const Key &key, const Record &cmp,
                                             int limit = -1,
                                             size_t offset = 0) const = 0;
  virtual const_KeyIterable Keys() const = 0;
  virtual bool RecordOrdered() const = 0;
  virtual bool KeyOrdered() const = 0;

 protected:
  // We do not know if we are ordered or unordered, this type is revealed
  // to us by the derived class as an argument.
  MatViewOperator() : Operator(Operator::Type::MAT_VIEW) {}

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
  MatViewOperatorT(const MatViewOperatorT &other) = delete;
  MatViewOperatorT &operator=(const MatViewOperatorT &other) = delete;

  template <typename = typename std::enable_if<
                !std::is_same<T, RecordOrderedGroupedData>::value>>
  explicit MatViewOperatorT(const std::vector<ColumnID> &key_cols,
                            int limit = -1, size_t offset = 0)
      : MatViewOperator(),
        key_cols_(key_cols),
        contents_(),
        compare_(Record::Compare{{}}),
        limit_(limit),
        offset_(offset) {}

  template <typename = typename std::enable_if<
                std::is_same<T, RecordOrderedGroupedData>::value>>
  explicit MatViewOperatorT(const std::vector<ColumnID> &key_cols,
                            const Record::Compare &compare, int limit = -1,
                            size_t offset = 0)
      : MatViewOperator(),
        key_cols_(key_cols),
        contents_(compare),
        compare_(Record::Compare{compare.Cols()}),
        limit_(limit),
        offset_(offset) {}

  size_t count() const override { return this->contents_.count(); }

  const std::vector<ColumnID> &key_cols() const override {
    return this->key_cols_;
  }

  bool Contains(const Key &key) const override {
    return this->contents_.Contains(key);
  }

  const_RecordIterable Lookup(const Key &key, int limit = -1,
                              size_t offset = 0) const override {
    limit = limit == -1 ? this->limit_ : limit;
    offset = offset == 0 ? this->offset_ : offset;
    return this->contents_.Lookup(key, limit, offset);
  }

  const_KeyIterable Keys() const override { return this->contents_.Keys(); }

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

  const_RecordIterable LookupGreater(const Key &key, const Record &cmp,
                                     int limit = -1,
                                     size_t offset = 0) const override {
    limit = limit == -1 ? this->limit_ : limit;
    offset = offset == 0 ? this->offset_ : offset;
    return this->contents_.LookupGreater(key, cmp, limit, offset);
  }

  // Debugging information
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
    return result;
  }
  uint64_t SizeInMemory() const override {
    return this->contents_.SizeInMemory();
  }

 protected:
  std::vector<Record> Process(NodeIndex source, std::vector<Record> &&records) {
    const std::vector<ColumnID> &keys = this->input_schemas_.at(0).keys();
    bool by_pk = keys.size() > 0 && keys == this->key_cols_;
    for (Record &r : records) {
      if (!this->contents_.Insert(r.GetValues(this->key_cols_), std::move(r),
                                  by_pk)) {
        LOG(FATAL) << "Failed to insert record in matview";
      }
    }

    return {};
  }

  void ComputeOutputSchema() override {
    this->output_schema_ = this->input_schemas_.at(0);
  }

  // Make a clone of this node (without any data).
  std::unique_ptr<Operator> Clone() const override {
    if constexpr (std::is_same<T, UnorderedGroupedData>::value) {
      return std::make_unique<MatViewOperatorT<UnorderedGroupedData>>(
          this->key_cols_, this->limit_, this->offset_);
    } else if constexpr (std::is_same<T, RecordOrderedGroupedData>::value) {
      return std::make_unique<MatViewOperatorT<RecordOrderedGroupedData>>(
          this->key_cols_, this->compare_, this->limit_, this->offset_);
    } else if constexpr (std::is_same<T, KeyOrderedGroupedData>::value) {
      return std::make_unique<MatViewOperatorT<KeyOrderedGroupedData>>(
          this->key_cols_, this->limit_, this->offset_);
    }
  }

 private:
  std::vector<ColumnID> key_cols_;
  T contents_;
  Record::Compare compare_;
  int limit_;
  size_t offset_;
};

using UnorderedMatViewOperator = MatViewOperatorT<UnorderedGroupedData>;
using RecordOrderedMatViewOperator = MatViewOperatorT<RecordOrderedGroupedData>;
using KeyOrderedMatViewOperator = MatViewOperatorT<KeyOrderedGroupedData>;

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_MATVIEW_H_
