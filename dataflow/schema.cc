#include "dataflow/schema.h"

namespace dataflow {

Schema::Schema(std::vector<DataType> columns) : types_(columns) {
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

}  // namespace dataflow