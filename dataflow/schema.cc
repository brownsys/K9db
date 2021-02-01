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

const Schema & SchemaFactory::create_or_get(const std::vector<DataType> &column_types) {
  auto& f = SchemaFactory::instance();
  auto it = f.schemas_.find(column_types);
  if(it == f.schemas_.end())
    f.schemas_[column_types] = new Schema(column_types);
  Schema *schema = f.schemas_[column_types];
  assert(schema);
  return *schema;
}

SchemaFactory::~SchemaFactory() {
  for(auto s : schemas_) {
    delete s.second;
    s.second = nullptr;
  }

  schemas_.clear();
}

}  // namespace dataflow
