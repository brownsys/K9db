// Connection to memcached.
#ifndef EXPERIMENTS_MEMCACHED_MEMORY_MEMCACHED_ENCODE_H_
#define EXPERIMENTS_MEMCACHED_MEMORY_MEMCACHED_ENCODE_H_

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

#include "mariadb/conncpp.hpp"

using MemcachedKey = std::string;
using MemcachedRecord = std::string;

// Encode values.
std::string EncodeValue(uint64_t val) {
  std::string v = std::to_string(val);
  while (v.size() < sizeof(uint64_t)) {
    v = "0" + v;
  }
  return v;
}
std::string EncodeValue(int64_t val) {
  std::string v = std::to_string(val < 0 ? (-1 * val) : val);
  while (v.size() < sizeof(uint64_t)) {
    v = "0" + v;
  }
  if (val < 0) {
    v = "-" + v;
  }
  return v;
}
std::string EncodeValue(std::string val) { return val; }
std::string EncodeValue(sql::SQLString val) { return std::string(val.c_str()); }

// Encode a mariadb column.
std::string EncodeColumn(sql::ResultSet *row, sql::ResultSetMetaData *schema,
                         size_t i) {
  if (i == 0) {
    return "";
  }
  switch (schema->getColumnType(i)) {
    case sql::DataType::VARCHAR:
    case sql::DataType::NVARCHAR:
    case sql::DataType::CHAR:
    case sql::DataType::NCHAR:
    case sql::DataType::LONGVARCHAR:
    case sql::DataType::LONGNVARCHAR:
    case sql::DataType::TIMESTAMP: {
      return EncodeValue(row->getString(i));
    }
    case sql::DataType::TINYINT:
    case sql::DataType::SMALLINT:
    case sql::DataType::BIGINT:
    case sql::DataType::BIT:
    case sql::DataType::INTEGER:
    case sql::DataType::DECIMAL:
    case sql::DataType::NUMERIC: {
      return EncodeValue(static_cast<int64_t>(row->getInt(i)));
    }
    default:
      std::cerr << "Unknown mariadb type " << schema->getColumnType(i)
                << std::endl;
      assert(false);
  }
}

// Encode a base key.
MemcachedKey EncodeBaseKey(sql::ResultSet *row, sql::ResultSetMetaData *schema,
                           const std::vector<size_t> &key_indices) {
  std::string key = "|";
  for (size_t index : key_indices) {
    key += EncodeColumn(row, schema, index) + "|";
  }
  return key;
}

// Encode a complete key.
MemcachedKey EncodeKey(uint64_t query, const MemcachedKey &key, uint64_t i) {
  return EncodeValue(query) + "@" + key + "@" + EncodeValue(i);
}

// Encode a complete row.
MemcachedRecord EncodeRow(sql::ResultSet *row, sql::ResultSetMetaData *schema) {
  std::string record = "|";
  for (size_t i = 1; i <= schema->getColumnCount(); i++) {
    record += EncodeColumn(row, schema, i) + "|";
  }
  return record;
}

#endif  // EXPERIMENTS_MEMCACHED_MEMORY_MEMCACHED_ENCODE_H_
