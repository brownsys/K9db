#ifndef PELTON_DATAFLOW_OPS_RECORD_UTILS_H_
#define PELTON_DATAFLOW_OPS_RECORD_UTILS_H_

#include "dataflow/record.h"

namespace dataflow {

bool Eq(DataType t, Record& a, Record& b, size_t index) {
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

bool NotEq(DataType t, Record& a, Record& b, size_t index) {
  switch (t) {
    case DataType::kText:
      return *a.as_string(index) != *b.as_string(index);
    case DataType::kUInt:
      return a.as_uint(index) != b.as_uint(index);
    case DataType::kInt:
      return a.as_int(index) != b.as_int(index);
    default:
      LOG(FATAL) << "unimplemented equals comparison!";
  }
}

bool Lt(DataType t, Record& a, Record& b, size_t index) {
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

bool LtEq(DataType t, Record& a, Record& b, size_t index) {
  switch (t) {
    case DataType::kText:
      return *a.as_string(index) <= *b.as_string(index);
    case DataType::kUInt:
      return a.as_uint(index) <= b.as_uint(index);
    case DataType::kInt:
      return a.as_int(index) <= b.as_int(index);
    default:
      LOG(FATAL) << "unimplemented less-or-equal comparison!";
  }
}

bool Gt(DataType t, Record& a, Record& b, size_t index) {
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

bool GtEq(DataType t, Record& a, Record& b, size_t index) {
  switch (t) {
    case DataType::kText:
      return *a.as_string(index) >= *b.as_string(index);
    case DataType::kUInt:
      return a.as_uint(index) >= b.as_uint(index);
    case DataType::kInt:
      return a.as_int(index) >= b.as_int(index);
    default:
      LOG(FATAL) << "unimplemented greater-or-equal comparison!";
  }
}

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_OPS_RECORD_UTILS_H_
