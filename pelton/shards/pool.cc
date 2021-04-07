// Manages mysql connections to the different shard/mini-databases.
#include "pelton/shards/pool.h"

#include <cstring>
#include <sstream>

#include "absl/strings/str_cat.h"
#include "glog/logging.h"
#include "mysql-cppconn-8/mysqlx/xdevapi.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/util/perf.h"

namespace pelton {
namespace shards {

#define SESSION reinterpret_cast<mysqlx::Session *>(this->session_)

namespace {

char *CopyCString(const std::string &str) {
  char *result = new char[str.size() + 1];
  // NOLINTNEXTLINE
  strcpy(result, str.c_str());
  return result;
}

char **GetColumnNames(mysqlx::SqlResult *result) {
  char **names = new char *[result->getColumnCount()];
  for (size_t i = 0; i < result->getColumnCount(); i++) {
    names[i] = CopyCString(result->getColumn(i).getColumnName());
  }
  return names;
}

char **GetColumnNames(mysqlx::SqlResult *result, const std::string &colname,
                      size_t colindex) {
  char **names = new char *[result->getColumnCount() + 1];
  names[colindex] = CopyCString(colname);
  for (size_t i = 0; i < colindex; i++) {
    names[i] = CopyCString(result->getColumn(i).getColumnName());
  }
  for (size_t i = colindex; i < result->getColumnCount(); i++) {
    names[i + 1] = CopyCString(result->getColumn(i).getColumnName());
  }
  return names;
}

inline void ReturnResult(mysqlx::SqlResult *result, const OutputChannel &out) {
  // Allocate memory.
  char **names = GetColumnNames(result);
  char **values = new char *[result->getColumnCount()];

  // Callback for each row.
  while (result->count() > 0) {
    mysqlx::Row row = result->fetchOne();
    for (size_t i = 0; i < result->getColumnCount(); i++) {
      std::ostringstream stream;
      row.get(i).print(stream);
      values[i] = CopyCString(stream.str());
    }
    out.callback(out.context, result->getColumnCount(), values, names);
    for (size_t i = 0; i < result->getColumnCount(); i++) {
      delete[] values[i];
    }
  }

  // Delete allocated memory.
  for (size_t i = 0; i < result->getColumnCount(); i++) {
    delete[] names[i];
  }
  delete[] names;
  delete[] values;
}

inline void ReturnResult(mysqlx::SqlResult *result, const OutputChannel &out,
                         const std::string &colname, const std::string &colval,
                         size_t colindex) {
  // Allocate memory.
  char **names = GetColumnNames(result, colname, colindex);
  char **values = new char *[result->getColumnCount() + 1];
  values[colindex] = CopyCString(colval);

  // Callback for each row.
  while (result->count() > 0) {
    mysqlx::Row row = result->fetchOne();
    for (size_t i = 0; i < colindex; i++) {
      std::ostringstream stream;
      row.get(i).print(stream);
      values[i] = CopyCString(stream.str());
    }
    for (size_t i = colindex; i < result->getColumnCount(); i++) {
      std::ostringstream stream;
      row.get(i).print(stream);
      values[i + 1] = CopyCString(stream.str());
    }
    out.callback(out.context, result->getColumnCount() + 1, values, names);
    for (size_t i = 0; i < colindex; i++) {
      delete[] values[i];
    }
    for (size_t i = colindex; i < result->getColumnCount(); i++) {
      delete[] values[i + 1];
    }
  }

  // Delete allocated memory.
  for (size_t i = 0; i < result->getColumnCount() + 1; i++) {
    delete[] names[i];
  }
  delete[] names;
  delete[] values;
}

}  // namespace

// Destructor.
ConnectionPool::~ConnectionPool() {
  if (SESSION != nullptr) {
    SESSION->close();
    delete SESSION;
    this->session_ = nullptr;
  }
}

// Initialization: open a connection to the default unsharded database.
void ConnectionPool::Initialize(const std::string &username,
                                const std::string &password) {
  this->session_ = new mysqlx::Session("root:password@localhost");
}

// Manage using shards in session_.
void ConnectionPool::OpenDefaultShard() {
  LOG(INFO) << "Shard: default shard";
  SESSION->sql("CREATE DATABASE IF NOT EXISTS default_db;").execute();
  SESSION->sql("USE default_db").execute();
}
void ConnectionPool::OpenShard(const ShardKind &shard_kind,
                               const UserId &user_id) {
  std::string shard_name = sqlengine::NameShard(shard_kind, user_id);
  LOG(INFO) << "Shard: " << shard_name;
  SESSION->sql("CREATE DATABASE IF NOT EXISTS " + shard_name).execute();
  SESSION->sql("USE " + shard_name).execute();
}

// Execution of SQL statements.
// Execute statement against the default un-sharded database.
absl::Status ConnectionPool::ExecuteDefault(const std::string &sql,
                                            const OutputChannel &output) {
  perf::Start("ExecuteDefault");

  this->OpenDefaultShard();
  LOG(INFO) << "Statement: " << sql;

  mysqlx::SqlResult result = SESSION->sql(sql).execute();
  if (result.hasData()) {
    ReturnResult(&result, output);
  } else {
    LOG(INFO) << "Affected rows: " << result.count();
  }

  perf::End("ExecuteDefault");
  return absl::OkStatus();
}

// Execute statement against given user shard(s).
absl::Status ConnectionPool::ExecuteShard(const std::string &sql,
                                          const ShardingInformation &info,
                                          const UserId &user_id,
                                          const OutputChannel &output) {
  perf::Start("ExecuteShard");

  this->OpenShard(info.shard_kind, user_id);
  LOG(INFO) << "Statement:" << sql;

  mysqlx::SqlResult result = SESSION->sql(sql).execute();
  if (result.hasData()) {
    ReturnResult(&result, output, info.shard_by, user_id, info.shard_by_index);
  } else {
    LOG(INFO) << "Affected rows: " << result.count();
  }

  perf::End("ExecuteShard");
  return absl::OkStatus();
}

absl::Status ConnectionPool::ExecuteShards(
    const std::string &sql, const ShardingInformation &info,
    const std::unordered_set<UserId> &user_ids, const OutputChannel &output) {
  for (const UserId &user_id : user_ids) {
    auto result = this->ExecuteShard(sql, info, user_id, output);
    if (!result.ok()) {
      return result;
    }
  }
  return absl::OkStatus();
}

// Removing a shard is equivalent to deleting its database.
void ConnectionPool::RemoveShard(const std::string &shard_name) {
  LOG(INFO) << "Remove Shard: " << shard_name;
  SESSION->sql("DROP DATABASE " + shard_name).execute();
}

}  // namespace shards
}  // namespace pelton
