#ifndef K9DB_DATAFLOW_OPERATOR_TYPE_H_
#define K9DB_DATAFLOW_OPERATOR_TYPE_H_

namespace k9db {
namespace dataflow {

enum class OperatorType {
  INPUT,
  IDENTITY,
  MAT_VIEW,
  FORWARD_VIEW,
  FILTER,
  UNION,
  EQUIJOIN,
  PROJECT,
  AGGREGATE,
  EXCHANGE,
};

}  // namespace dataflow
}  // namespace k9db

#endif  // K9DB_DATAFLOW_OPERATOR_TYPE_H_
