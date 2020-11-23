#ifndef PELTON_DATAFLOW_SCHEMA_H_
#define PELTON_DATAFLOW_SCHEMA_H_

namespace dataflow {

enum DataType {
  UINT,
  INT,
  TEXT,
  DATETIME,
};

bool is_inlineable(DataType t) {
  switch (t) {
    case UINT:
    case INT:
      return true;
    default:
      return false;
  }
}

class Schema {
 public:
  Schema(std::vector<DataType> columns) : types_(columns) {
    num_inline_columns_ = 0;
    num_pointer_columns_ = 0;
    for (auto t : columns) {
      if (is_inlineable(t)) {
        true_indices_.push_back(std::make_pair(true, num_inline_columns_));
        num_inline_columns_++;
      } else {
        true_indices_.push_back(std::make_pair(false, num_pointer_columns_));
        num_pointer_columns_++;
      }
    }
  }

  std::pair<bool, size_t> RawColumnIndex(size_t index) const {
    CHECK_LT(index, true_indices_.size())
        << "column index " << index << " out of bounds";
    return true_indices_[index];
  }

  size_t num_inline_columns() const { return num_inline_columns_; }
  size_t num_pointer_columns() const { return num_pointer_columns_; }

 private:
  const std::vector<DataType> types_;
  size_t num_inline_columns_;
  size_t num_pointer_columns_;
  std::vector<std::pair<bool, size_t>> true_indices_;
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_SCHEMA_H_
