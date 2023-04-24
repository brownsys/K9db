#ifndef K9DB_DATAFLOW_OPS_FILTER_ENUM_H_
#define K9DB_DATAFLOW_OPS_FILTER_ENUM_H_

namespace k9db {
namespace dataflow {

enum FilterOperationEnum {
  LESS_THAN,
  LESS_THAN_OR_EQUAL,
  GREATER_THAN,
  GREATER_THAN_OR_EQUAL,
  EQUAL,
  NOT_EQUAL,
  IS_NULL,
  IS_NOT_NULL
};

}  // namespace dataflow
}  // namespace k9db

#endif  // K9DB_DATAFLOW_OPS_FILTER_ENUM_H_
