#include "baseline/memcached.h"

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
// NOLINTNEXTLINE
#include <regex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "libmemcached/memcached.h"
#include "mariadb/conncpp.hpp"

namespace {

// NOLINTNEXTLINE
static std::string prefix;

// MariaDB connection.
static std::unique_ptr<sql::Connection> conn;
static std::unique_ptr<sql::Statement> stmt;

// Memcached connection.
static memcached_st *memc = memcached_create(NULL);
static memcached_server_st *servers = nullptr;

// State about the current cached views.
static std::unordered_map<std::string, std::unordered_set<int>> views;
static std::unordered_map<int, std::string> queries;
static std::unordered_map<int, std::vector<std::string>> keys;
static std::unordered_map<int, std::vector<Type>> types;
static std::unordered_map<std::string, size_t> key_counts;
static std::unordered_map<int, std::unordered_set<std::string>> all_keys;
static std::unordered_map<int, uint64_t> sizes;

// Regex for parsing table names.
static std::regex regex_table{
    "(FROM|from|JOIN|join)\\s+([A-Za-z0-9_]+)(\\s|;|$)"};
// Regex for parsing keys.
static std::regex regex_postand{"(.*) ([A-Za-z0-9\\._]+) \\= \\? AND(.*)"};
static std::regex regex_preand{"(.*) AND ([A-Za-z0-9\\._]+) \\= \\?(.*)"};
static std::regex regex_where{"(.*) WHERE ([A-Za-z0-9\\._]+) \\= \\?(.*)"};
static std::regex regex_having{"(.*) HAVING ([A-Za-z0-9\\._]+) \\= \\?(.*)"};
static std::regex regex_var{std::regex("([A-Za-z0-9]+\\.)?([A-Za-z0-9_]+)")};

// Connect to memcached.
void MemcachedConnect() {
  memcached_return status;
  servers = memcached_server_list_append(nullptr, "localhost", 11211, &status);
  status = memcached_server_push(memc, servers);
  if (status != MEMCACHED_SUCCESS) {
    std::cout << "Couldn't add server: " << memcached_strerror(memc, status)
              << std::endl;
    assert(false);
  }
}
// Connect to MariaDB.
void MariaDBConnect(const char *db, const char *db_username,
                    const char *db_password) {
  sql::ConnectOptionsMap props;
  props["hostName"] = "localhost";
  props["userName"] = db_username;
  props["password"] = db_password;

  sql::Driver *driver = sql::mariadb::get_driver_instance();
  conn = std::unique_ptr<sql::Connection>(driver->connect(props));
  stmt = std::unique_ptr<sql::Statement>(conn->createStatement());
  stmt->execute((std::string("CREATE DATABASE IF NOT EXISTS ") + db).c_str());
  stmt->execute((std::string("USE ") + db).c_str());
}

// Transforming format from our format to Memcached.
std::string ToMemcached(const Record &record) {
  std::string row = "";
  for (size_t i = 0; i < record.size; i++) {
    const Value &val = record.values[i];
    switch (val.type) {
      case TEXT: {
        row += std::string(GetStr(&val));
        break;
      }
      case INT: {
        int64_t v = GetInt(&val);
        std::string tmp = std::to_string(v);
        bool negative = tmp.at(0) == '-';
        while (tmp.size() < 8) {
          if (negative) {
            tmp = "-0" + tmp.substr(1);
          } else {
            tmp = "0" + tmp;
          }
        }
        row += tmp;
        break;
      }
      case UINT: {
        uint64_t v = GetUInt(&val);
        std::string tmp = std::to_string(v);
        while (tmp.size() < 8) {
          tmp = "0" + tmp;
        }
        row += tmp;
        break;
      }
    }
    row += "|";
  }
  return row;
}

// Transforming format from memcached to our format.
Record FromMemcached(const std::vector<Type> &types, const std::string &str) {
  Value *values = new Value[types.size()];
  size_t sidx = 0;
  size_t eidx = 0;
  for (size_t i = 0; i < types.size(); i++) {
    while (str.at(eidx) != '|') {
      eidx++;
    }
    std::string value = str.substr(sidx, eidx - sidx);
    switch (types.at(i)) {
      case TEXT:
        values[i] = SetStr(value.c_str());
        break;
      case INT:
        values[i] = SetInt(strtoll(value.c_str(), nullptr, 10));
        break;
      case UINT:
        values[i] = SetUInt(strtoull(value.c_str(), nullptr, 10));
        break;
    }
    eidx++;
    sidx = eidx;
  }
  return {types.size(), values};
}

// Transforming format from DB to our format.
Record FromDB(sql::ResultSet *row, sql::ResultSetMetaData *schema,
              const std::vector<size_t> &indices) {
  Value *values = new Value[indices.size()];
  for (size_t idx = 0; idx < indices.size(); idx++) {
    size_t i = indices.at(idx);
    switch (schema->getColumnType(i)) {
      case sql::DataType::VARCHAR:
      case sql::DataType::NVARCHAR:
      case sql::DataType::CHAR:
      case sql::DataType::NCHAR:
      case sql::DataType::LONGVARCHAR:
      case sql::DataType::LONGNVARCHAR:
      case sql::DataType::TIMESTAMP: {
        auto str = row->getString(i);
        values[idx] = SetStr(str.c_str());
        break;
      }
      case sql::DataType::TINYINT:
      case sql::DataType::SMALLINT:
      case sql::DataType::BIGINT:
      case sql::DataType::BIT:
      case sql::DataType::INTEGER:
      case sql::DataType::DECIMAL:
      case sql::DataType::NUMERIC: {
        values[idx] = SetInt(row->getInt(i));
        break;
      }
    }
  }
  return {indices.size(), values};
}

// Index and type lookups.
std::vector<size_t> KeyIndices(sql::ResultSetMetaData *schema,
                               const std::vector<std::string> &keys) {
  std::vector<size_t> key_indices;
  for (const auto &key_name : keys) {
    bool found = false;
    for (size_t i = 1; i <= schema->getColumnCount(); i++) {
      if (schema->getColumnName(i) == key_name) {
        key_indices.push_back(i);
        found = true;
        break;
      }
    }
    if (!found) {
      LOG(FATAL) << "Key " << key_name << " is not part of the result";
    }
  }
  return key_indices;
}
std::vector<size_t> AllIndices(sql::ResultSetMetaData *schema) {
  std::vector<size_t> all_indices;
  for (size_t i = 1; i <= schema->getColumnCount(); i++) {
    all_indices.push_back(i);
  }
  return all_indices;
}
std::vector<Type> AllTypes(sql::ResultSetMetaData *schema) {
  std::vector<Type> all_types;
  for (size_t i = 1; i <= schema->getColumnCount(); i++) {
    switch (schema->getColumnType(i)) {
      case sql::DataType::VARCHAR:
      case sql::DataType::NVARCHAR:
      case sql::DataType::CHAR:
      case sql::DataType::NCHAR:
      case sql::DataType::LONGVARCHAR:
      case sql::DataType::LONGNVARCHAR:
      case sql::DataType::TIMESTAMP: {  // Date as string.
        all_types.push_back(Type::TEXT);
        break;
      }
      case sql::DataType::TINYINT:
      case sql::DataType::SMALLINT:
      case sql::DataType::BIGINT:
      case sql::DataType::BIT:
      case sql::DataType::INTEGER:
      case sql::DataType::DECIMAL:
      case sql::DataType::NUMERIC: {
        all_types.push_back(Type::INT);
        break;
      }
      default:
        LOG(FATAL) << "Unrecognized type " << schema->getColumnType(i);
    }
  }
  return all_types;
}

// The actual caching.
void ComputeAndCache(int id, const char *query) {
  std::string str = query;
  // Deduce the keys.
  std::vector<std::string> query_keys;
  std::smatch matches;
  while (std::regex_match(str, matches, regex_postand) ||
         std::regex_match(str, matches, regex_preand) ||
         std::regex_match(str, matches, regex_where) ||
         std::regex_match(str, matches, regex_having)) {
    std::string key_col = matches[2];
    str = std::string(matches[1]) + std::string(matches[3]);
    if (std::regex_match(key_col, matches, regex_var)) {
      query_keys.push_back(matches[2]);
    } else {
      LOG(FATAL) << "where column did not match regex " << key_col;
    }
  }

  // Execute the query.
  std::unique_ptr<sql::ResultSet> rows{stmt->executeQuery(str.c_str())};
  std::unique_ptr<sql::ResultSetMetaData> schema{rows->getMetaData()};

  // Find key columns indices and types.
  std::vector<size_t> key_indices = KeyIndices(schema.get(), query_keys);
  std::vector<size_t> all_indices = AllIndices(schema.get());
  std::vector<Type> all_types = AllTypes(schema.get());

  // Read each row and put it in memcached.
  while (rows->next()) {
    // Transform row to our representation.
    Record key = FromDB(rows.get(), schema.get(), key_indices);
    Record val = FromDB(rows.get(), schema.get(), all_indices);

    // Transform our representation to memcached.
    std::string serialized_key = prefix + std::to_string(id) + ToMemcached(key);
    std::string serialized_val = ToMemcached(val);
    all_keys[id].insert(serialized_key);
    serialized_key += "@" + std::to_string(key_counts[serialized_key]++);

    // Turn c++-strings into c-buffers.
    size_t keylen = serialized_key.size();
    size_t vallen = serialized_val.size();
    char *keybuf = new char[keylen + 1];
    char *valbuf = new char[vallen + 1];
    // NOLINTNEXTLINE
    strcpy(keybuf, serialized_key.c_str());
    // NOLINTNEXTLINE
    strcpy(valbuf, serialized_val.c_str());

    // Put C-buffs in memcached.
    auto status = memcached_add(memc, keybuf, keylen, valbuf, vallen, 0,
                                static_cast<uint32_t>(0));
    if (status != MEMCACHED_SUCCESS) {
      std::cout << "Couldn't set row: " << memcached_strerror(memc, status)
                << std::endl;
      std::cout << serialized_key << std::endl;
      std::cout << serialized_val << std::endl;
      assert(false);
    }

    delete[] keybuf;
    delete[] valbuf;
    DestroyRecord(&key);
    DestroyRecord(&val);
  }

  // Update state.
  keys.emplace(id, std::move(query_keys));
  types.emplace(id, std::move(all_types));
}

}  // namespace

// Destructors.
void DestroyValue(Value *v) {
  if (v->type == TEXT) {
    char *ptr = reinterpret_cast<char *>(v->value);
    delete[] ptr;
  }
}
void DestroyRecord(Record *r) {
  if (r->values != nullptr) {
    for (size_t i = 0; i < r->size; i++) {
      DestroyValue(r->values + i);
    }
    delete[] r->values;
  }
}
void DestroyResult(Result *r) {
  if (r->records != nullptr) {
    for (size_t i = 0; i < r->size; i++) {
      DestroyRecord(r->records + i);
    }
    delete[] r->records;
  }
}

// Get value by type.
int64_t GetInt(const Value *val) {
  return *reinterpret_cast<const int64_t *>(&val->value);
}
uint64_t GetUInt(const Value *val) { return val->value; }
const char *GetStr(const Value *val) {
  return reinterpret_cast<char *>(val->value);
}

// Create value by type.
Value SetInt(int64_t v) { return {INT, *reinterpret_cast<uint64_t *>(&v)}; }
Value SetUInt(uint64_t v) { return {UINT, v}; }
Value SetStr(const char *v) {
  char *dst = new char[strlen(v) + 1];
  // NOLINTNEXTLINE
  strcpy(dst, v);
  return {TEXT, reinterpret_cast<uint64_t>(dst)};
}

// Initialization.
bool Initialize(const char *db_name, const char *db_username,
                const char *db_password, const char *seed) {
  prefix = seed;
  // Connect to server.
  MemcachedConnect();
  MariaDBConnect(db_name, db_username, db_password);
  return true;
}

// Cache a query in memcached.
// returns -1 on error, otherwise returns a cache id which can be used to
// lookup data for the query in cache.
int Cache(const char *query) {
  int id = keys.size();

  std::string str = query;

  auto begin = std::sregex_iterator(str.begin(), str.end(), regex_table);
  auto end = std::sregex_iterator();
  for (auto it = begin; it != end; ++it) {
    std::smatch matches = *it;
    std::string table_name = matches[2];
    views[table_name].insert(id);
  }

  queries.emplace(id, query);

  // Compute the query and cache the results.
  ComputeAndCache(id, query);

  // Return id.
  return id;
}

// Recompute all cached queries involving given table.
void Update(const char *table) {
  for (int id : views[table]) {
    // Delete all the data associated with id from memcached.
    for (const auto &base_key : all_keys[id]) {
      for (size_t i = 0; i < key_counts.at(base_key); i++) {
        std::string serialized_key = base_key + "@" + std::to_string(i);

        // Turn c++-strings into c-buffers.
        size_t keylen = serialized_key.size();
        char *keybuf = new char[keylen + 1];
        // NOLINTNEXTLINE
        strcpy(keybuf, serialized_key.c_str());

        // Remove key from memcached.
        auto status = memcached_delete(memc, keybuf, keylen, 0);
        if (status != MEMCACHED_SUCCESS) {
          std::cout << "Couldn't remove key: "
                    << memcached_strerror(memc, status) << std::endl;
          std::cout << serialized_key << std::endl;
          assert(false);
        }

        delete[] keybuf;
      }
      key_counts.erase(base_key);
    }

    // Empty the state about the current cached views.
    keys.erase(id);
    types.erase(id);
    all_keys.erase(id);
    sizes.erase(id);

    // Compute it again.
    ComputeAndCache(id, queries.at(id).c_str());
  }
}

Result Read(int id, const Record *key) {
  std::string serialized_key = prefix + std::to_string(id) + ToMemcached(*key);
  if (key_counts.count(serialized_key) > 0) {
    Record *records = new Record[key_counts.at(serialized_key)];
    for (size_t i = 0; i < key_counts.at(serialized_key); i++) {
      std::string read_key = serialized_key + "@" + std::to_string(i);

      // Turn c++-string to c-buffer.
      size_t keylen = read_key.size();
      char *keybuf = new char[keylen + 1];
      // NOLINTNEXTLINE
      strcpy(keybuf, read_key.c_str());

      // Read from memcached.
      size_t vallen;
      char *valbuf = memcached_get(memc, keybuf, keylen, &vallen, 0,
                                   static_cast<uint32_t>(0));
      std::string serialized_val{valbuf, vallen};

      // De-serialize memcached format.
      records[i] = FromMemcached(types.at(id), serialized_val);
      delete[] keybuf;
      free(valbuf);
    }
    return {key_counts.at(serialized_key), records};
  } else {
    return {0, new Record[0]};
  }
}

// For tests.
void __ExecuteDB(const char *stmt_str) { stmt->execute(stmt_str); }

// Close
void Close() {
  memcached_free(memc);
  memcached_server_list_free(servers);
}
