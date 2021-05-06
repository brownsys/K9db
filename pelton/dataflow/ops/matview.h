#ifndef PELTON_DATAFLOW_OPS_MATVIEW_H_
#define PELTON_DATAFLOW_OPS_MATVIEW_H_

#include <utility>
#include <vector>

#include "pelton/dataflow/key.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/grouped_data.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

// Virtual parent class: this is what the rest of our system uses and interacts
// with. Allows reading and writing into the materialized view without knowing
// the underlying storage layout and order.
class MatViewOperator : public Operator {
 public:
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
  template <typename = typename std::enable_if<
                !std::is_same<T, RecordOrderedGroupedData>::value>>
  explicit MatViewOperatorT(const std::vector<ColumnID> &key_cols,
                            int limit = -1, size_t offset = 0)
      : MatViewOperator(),
        key_cols_(key_cols),
        contents_(),
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

 protected:
  bool Process(NodeIndex source, const std::vector<Record> &records,
               std::vector<Record> *output) override {
    bool by_pk = false;
    if (records.size() > 0) {
      const std::vector<ColumnID> &keys = records.at(0).schema().keys();
      by_pk = keys.size() > 0 && keys == this->key_cols_;
    }

    for (const Record &r : records) {
      if (!this->contents_.Insert(r.GetValues(this->key_cols_), r, by_pk)) {
        return false;
      }
    }

    return true;
  }

  void ComputeOutputSchema() override {
    this->output_schema_ = this->input_schemas_.at(0);
  }

  uint64_t SizeInMemory() const override {
    return this->contents_.SizeInMemory();
  }

 private:
  std::vector<ColumnID> key_cols_;
  T contents_;
  int limit_;
  size_t offset_;
};

using UnorderedMatViewOperator = MatViewOperatorT<UnorderedGroupedData>;
using RecordOrderedMatViewOperator = MatViewOperatorT<RecordOrderedGroupedData>;
using KeyOrderedMatViewOperator = MatViewOperatorT<KeyOrderedGroupedData>;

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_MATVIEW_H_
