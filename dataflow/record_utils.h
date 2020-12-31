#ifndef PELTON_DATAFLOW_OPS_RECORD_UTILS_H_
#define PELTON_DATAFLOW_OPS_RECORD_UTILS_H_

#include "dataflow/record.h"

namespace dataflow {
namespace record {

bool Equal(DataType t, const Record& a, const Record& b, size_t index) {
  switch (t) {
    case DataType::kText:
      return *a.as_string(index) == *b.as_string(index);
    case DataType::kUInt:
      return a.as_uint(index) == b.as_uint(index);
    case DataType::kInt:
      return a.as_int(index) == b.as_int(index);
    default:
      LOG(FATAL) << "unimplemented equals comparison!";
  }
}

bool NotEqual(DataType t, const Record& a, const Record& b, size_t index) {
  return !Equal(t, a, b, index);
}

bool LessThan(DataType t, const Record& a, const Record& b, size_t index) {
  switch (t) {
    case DataType::kText:
      return *a.as_string(index) < *b.as_string(index);
    case DataType::kUInt:
      return a.as_uint(index) < b.as_uint(index);
    case DataType::kInt:
      return a.as_int(index) < b.as_int(index);
    default:
      LOG(FATAL) << "unimplemented less-than comparison!";
  }
}

bool LessThanOrEqual(DataType t, const Record& a, const Record& b,
                     size_t index) {
  return LessThan(t, a, b, index) || Equal(t, a, b, index);
}

bool GreaterThan(DataType t, const Record& a, const Record& b, size_t index) {
  switch (t) {
    case DataType::kText:
      return *a.as_string(index) > *b.as_string(index);
    case DataType::kUInt:
      return a.as_uint(index) > b.as_uint(index);
    case DataType::kInt:
      return a.as_int(index) > b.as_int(index);
    default:
      LOG(FATAL) << "unimplemented greater comparison!";
  }
}

bool GreaterThanOrEqual(DataType t, const Record& a, const Record& b,
                        size_t index) {
  return GreaterThan(t, a, b, index) || Equal(t, a, b, index);
}

}  // namespace record
}  // namespace dataflow

#endif  // PELTON_DATAFLOW_OPS_RECORD_UTILS_H_
