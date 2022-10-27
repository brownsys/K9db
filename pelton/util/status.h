// This file contains helper macros around absl::StatusOr.

#ifndef PELTON_UTIL_STATUS_H_
#define PELTON_UTIL_STATUS_H_

#include <utility>

#include "glog/logging.h"

#define __COMMA ,

#define __CHECK_STATUS_VAR_NAME(arg) __CHECK_STATUS_RESULT_##arg
#define __CHECK_STATUS_VAL(arg) __CHECK_STATUS_VAR_NAME(arg)
#define CHECK_STATUS(status)                  \
  auto __CHECK_STATUS_VAL(__LINE__) = status; \
  if (!__CHECK_STATUS_VAL(__LINE__).ok()) return __CHECK_STATUS_VAL(__LINE__)

#define __CHECK_STATUS_OR_VAR_NAME(arg) __CHECK_STATUS_OR_RESULT_##arg
#define __CHECK_STATUS_OR_VAL(arg) __CHECK_STATUS_OR_VAR_NAME(arg)
#define CHECK_STATUS_OR(status_)                  \
  auto __CHECK_STATUS_OR_VAL(__LINE__) = status_; \
  if (!__CHECK_STATUS_OR_VAL(__LINE__).ok())      \
  return __CHECK_STATUS_OR_VAL(__LINE__).status()

#define __PANIC_VAR_NAME(arg) __PANIC_RESULT_##arg
#define __PANIC_VAL(arg) __PANIC_VAR_NAME(arg)
#define PANIC(status)                  \
  auto __PANIC_VAL(__LINE__) = status; \
  if (!__PANIC_VAL(__LINE__).ok())     \
  LOG(FATAL) << __PANIC_VAL(__LINE__).getState()

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

#define __ASSIGN_OR_PANIC_VAR_NAME(arg) __ASSIGN_OR_PANIC_RESULT_##arg
#define __ASSIGN_OR_PANIC_VAL(arg) __ASSIGN_OR_PANIC_VAR_NAME(arg)
#define ASSIGN_OR_PANIC(lexpr, rexpr)                       \
  auto __ASSIGN_OR_PANIC_VAL(__LINE__) = rexpr;             \
  if (!__ASSIGN_OR_PANIC_VAL(__LINE__).ok())                \
    LOG(FATAL) << __ASSIGN_OR_PANIC_VAL(__LINE__).status(); \
  lexpr = __ASSIGN_OR_PANIC_VAL(__LINE__).value()

#define __MOVE_OR_PANIC_VAR_NAME(arg) __MOVE_OR_PANIC_RESULT_##arg
#define __MOVE_OR_PANIC_VAL(arg) __MOVE_OR_PANIC_VAR_NAME(arg)
#define MOVE_OR_PANIC(lexpr, rexpr)                       \
  auto __MOVE_OR_PANIC_VAL(__LINE__) = rexpr;             \
  if (!__MOVE_OR_PANIC_VAL(__LINE__).ok())                \
    LOG(FATAL) << __MOVE_OR_PANIC_VAL(__LINE__).status(); \
  lexpr = std::move(__MOVE_OR_PANIC_VAL(__LINE__).value())

#define ASSERT_RET(expr, err, msg) \
  if (!(expr)) return absl::err##Error(msg)

#endif  // PELTON_UTIL_STATUS_H_
