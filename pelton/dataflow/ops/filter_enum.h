#ifndef PELTON_DATAFLOW_OPS_FILTER_ENUM_H_
#define PELTON_DATAFLOW_OPS_FILTER_ENUM_H_

namespace pelton {
namespace dataflow {

enum FilterOperationEnum {
  LESS_THAN,
  LESS_THAN_OR_EQUAL,
  GREATER_THAN,
  GREATER_THAN_OR_EQUAL,
  EQUAL,
  NOT_EQUAL,
  IS_NULL,
  IS_NOT_NULL,
  LIKE
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_FILTER_ENUM_H_
