// This file contains helper macros around absl::StatusOr.

#ifndef SHARDS_UTIL_STATUS_H_
#define SHARDS_UTIL_STATUS_H_

#include <utility>

#define CHECK_STATUS(status) \
  if (!status.ok()) return status

#define __ASSIGN_OR_RETURN_VAR_NAME(arg) __ASSIGN_OR_RETURN_RESULT_##arg
#define __ASSIGN_OR_RETURN_VAL(arg) __ASSIGN_OR_RETURN_VAR_NAME(arg)
#define ASSIGN_OR_RETURN(lexpr, rexpr)                \
  auto __ASSIGN_OR_RETURN_VAL(__LINE__) = rexpr;      \
  if (!__ASSIGN_OR_RETURN_VAL(__LINE__).ok())         \
    return __ASSIGN_OR_RETURN_VAL(__LINE__).status(); \
  lexpr = __ASSIGN_OR_RETURN_VAL(__LINE__).value()

#define __MOVE_OR_RETURN_VAR_NAME(arg) __MOVE_OR_RETURN_RESULT_##arg
#define __MOVE_OR_RETURN_VAL(arg) __MOVE_OR_RETURN_VAR_NAME(arg)
#define MOVE_OR_RETURN(lexpr, rexpr)                \
  auto __MOVE_OR_RETURN_VAL(__LINE__) = rexpr;      \
  if (!__MOVE_OR_RETURN_VAL(__LINE__).ok())         \
    return __MOVE_OR_RETURN_VAL(__LINE__).status(); \
  lexpr = std::move(__MOVE_OR_RETURN_VAL(__LINE__).value())

#endif  // SHARDS_UTIL_STATUS_H_
