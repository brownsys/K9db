// Manages sqlite3 connections to the different shard/mini-databases.

#include "pelton/shards/sqlexecutor/executable.h"

#include <cassert>

namespace pelton {
namespace shards {
namespace sqlexecutor {

// ExecutableStatement.
const std::string &ExecutableStatement::shard_suffix() const {
  return this->shard_suffix_;
}

const std::string &ExecutableStatement::sql_statement() const {
  return this->sql_statement_;
}

// SimpleExecutableStatement.
bool SimpleExecutableStatement::Execute(::sqlite3 *connection,
                                        Callback callback, void *context,
                                        char **errmsg) {
  static Callback callback_no_capture = callback;
  return ::sqlite3_exec(
             connection, this->sql_statement_.c_str(),
             [](void *context, int colnum, char **colvals, char **colnames) {
               return callback_no_capture(context, colnum, colvals, colnames);
             },
             context, errmsg) == SQLITE_OK;
}

// SelectExecutableStatement.
bool SelectExecutableStatement::Execute(::sqlite3 *connection,
                                        Callback callback, void *context,
                                        char **errmsg) {
  CALLBACK_NO_CAPTURE = &callback;
  THIS_NO_CAPTURE = this;
  int result = ::sqlite3_exec(
      connection, this->sql_statement_.c_str(),
      [](void *context, int _colnum, char **colvals, char **colnames) {
        size_t colnum = static_cast<size_t>(_colnum);
        assert(colnum >= THIS_NO_CAPTURE->coli_);
        // Append stored column name and value to result at the stored index.
        char **appended_colvals = new char *[colnum + 1];
        char **appended_colnames = new char *[colnum + 1];
        appended_colvals[THIS_NO_CAPTURE->coli_] = THIS_NO_CAPTURE->colv_;
        appended_colnames[THIS_NO_CAPTURE->coli_] = THIS_NO_CAPTURE->coln_;
        for (size_t i = 0; i < THIS_NO_CAPTURE->coli_; i++) {
          appended_colvals[i] = colvals[i];
          appended_colnames[i] = colnames[i];
        }
        for (size_t i = THIS_NO_CAPTURE->coli_; i < colnum; i++) {
          appended_colvals[i + 1] = colvals[i];
          appended_colnames[i + 1] = colnames[i];
        }
        // Forward appended results to callback.
        int result = (*CALLBACK_NO_CAPTURE)(
            context, colnum + 1, appended_colvals, appended_colnames);
        // Deallocate appended results.
        delete[] appended_colvals;
        delete[] appended_colnames;
        return result;
      },
      context, errmsg);

  return result == SQLITE_OK;
}

}  // namespace sqlexecutor
}  // namespace shards
}  // namespace pelton
