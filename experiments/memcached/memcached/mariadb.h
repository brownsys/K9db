// Connection to mariadb.
#ifndef EXPERIMENTS_MEMCACHED_MEMCACHED_MARIADB_H_
#define EXPERIMENTS_MEMCACHED_MEMCACHED_MARIADB_H_

#include <cassert>
#include <cstdlib>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "mariadb/conncpp.hpp"

class MariaDBConnection {
 public:
  explicit MariaDBConnection(const std::string &host) {
    // Connect to mariadb server.
    sql::ConnectOptionsMap props;
    props["hostName"] = host.c_str();
    props["userName"] = "k9db";
    props["password"] = "password";

    sql::Driver *driver = sql::mariadb::get_driver_instance();
    if (!driver) {
      throw std::runtime_error("no driver");
    }

    this->conn_ = std::unique_ptr<sql::Connection>(driver->connect(props));
    this->stmt_ = std::unique_ptr<sql::Statement>(conn_->createStatement());
    this->stmt_->execute("USE lobsters");
  }

  std::unordered_map<MemcachedKey, std::vector<MemcachedRecord>> Query(
      const std::vector<std::string> &key_columns, const std::string &q,
      int limit) {
    // Execute the query.
    std::unique_ptr<sql::ResultSet> rows{this->stmt_->executeQuery(q.c_str())};
    std::unique_ptr<sql::ResultSetMetaData> schema{rows->getMetaData()};

    // Find the index of the key column in the result set.
    std::vector<size_t> key_indices;
    for (const std::string &key_column : key_columns) {
      for (size_t i = 1; i <= schema->getColumnCount(); i++) {
        if (schema->getColumnName(i) == key_column) {
          key_indices.push_back(i);
          break;
        }
      }
    }
    if (key_indices.size() != key_columns.size()) {
      std::cerr << "Key column(s) missing from result" << std::endl;
      assert(false);
    }

    // We will put the data here.
    std::unordered_map<MemcachedKey, std::vector<MemcachedRecord>> data;

    // Read each row and put it in data with the correct key.
    while (rows->next()) {
      // Transform row to our representation.
      MemcachedKey key = EncodeBaseKey(rows.get(), schema.get(), key_indices);
      MemcachedRecord record = EncodeRow(rows.get(), schema.get());
      if (limit == -1 || data[key].size() < static_cast<size_t>(limit)) {
        data[key].push_back(record);
      }
    }
    return data;
  }

 private:
  std::unique_ptr<sql::Connection> conn_;
  std::unique_ptr<sql::Statement> stmt_;
};

#endif  // EXPERIMENTS_MEMCACHED_MEMCACHED_MARIADB_H_
