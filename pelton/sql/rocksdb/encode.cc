#include "pelton/sql/rocksdb/encode.h"

#include <memory>

#include "glog/logging.h"

namespace pelton {
namespace sql {

// Helpers.
std::string EncodeValue(sqlast::ColumnDefinition::Type type,
                        const std::string &value) {
  if (value == "NULL") {
    return "" + __ROCKSNULL;
  }
  switch (type) {
    case sqlast::ColumnDefinition::Type::INT:
    case sqlast::ColumnDefinition::Type::UINT:
      return value;
    case sqlast::ColumnDefinition::Type::TEXT:
      return value.substr(1, value.size() - 2);
    case sqlast::ColumnDefinition::Type::DATETIME:
      if (value.starts_with('"') || value.starts_with('\'')) {
        return value.substr(1, value.size() - 2);
      }
      return value;
    default:
      LOG(FATAL) << "UNREACHABLE";
  }
}

rocksdb::Slice ExtractColumn(const rocksdb::Slice &slice, size_t col) {
  const char *data = slice.data();
  size_t start = 0;
  size_t size = 0;
  for (size_t i = 0; i < slice.size(); i++) {
    if (data[i] == __ROCKSSEP) {
      if (col == 0) {
        return rocksdb::Slice(data + start, size);
      } else {
        col--;
        start = i + 1;
        size = 0;
        continue;
      }
    }
    size++;
  }
  LOG(FATAL) << "Cannot extract column " << col << " from '" << slice.ToString()
             << "'";
}

/*
 * RocksdbRecord
 */
// Construct a record when handling SQL statements.
RocksdbRecord RocksdbRecord::FromInsert(const sqlast::Insert &stmt,
                                        const dataflow::SchemaRef &schema,
                                        const std::string &shard_name) {
  // Encode key.
  std::string key = shard_name;
  key.push_back(__ROCKSSEP);
  const auto &keys = schema.keys();
  CHECK_EQ(keys.size(), 1u) << "schema has too many keys " << schema;
  int idx = keys.at(0);
  if (stmt.HasColumns()) {
    idx = stmt.GetColumnIndex(schema.NameOf(idx));
    CHECK_GE(idx, 0) << "Insert does not specify PK";
  }
  const std::string &pk_value = stmt.GetValue(idx);
  CHECK_NE(pk_value, "NULL");
  key.append(EncodeValue(schema.TypeOf(keys.at(0)), pk_value));
  key.push_back(__ROCKSSEP);

  // Encode Value.
  std::string value;
  for (size_t i = 0; i < schema.size(); i++) {
    int idx = i;
    if (stmt.HasColumns()) {
      idx = stmt.GetColumnIndex(schema.NameOf(i));
    }
    const std::string &val = stmt.GetValue(idx);
    value.append(EncodeValue(schema.TypeOf(i), val));
    value.push_back(__ROCKSSEP);
  }
  return RocksdbRecord(std::move(key), std::move(value));
}

// For reading/decoding.
dataflow::Record RocksdbRecord::DecodeRecord(
    const dataflow::SchemaRef &schema) const {
  dataflow::Record record{schema, false};
  size_t ptr = 0;
  for (size_t idx = 0; idx < schema.size(); idx++) {
    size_t len = 0;
    while (this->value_[ptr + len] != __ROCKSSEP) {
      len++;
    }

    // NULL?
    if (len == 1 && this->value_[ptr] == __ROCKSNULL) {
      record.SetNull(true, idx);
      continue;
    }

    // NOT NULL.
    std::string value = this->value_.substr(ptr, len);
    switch (schema.TypeOf(idx)) {
      case sqlast::ColumnDefinition::Type::UINT: {
        record.SetUInt(std::stoul(value), idx);
        break;
      }
      case sqlast::ColumnDefinition::Type::INT: {
        record.SetInt(std::stol(value), idx);
        break;
      }
      case sqlast::ColumnDefinition::Type::TEXT: {
        record.SetString(std::make_unique<std::string>(std::move(value)), idx);
        break;
      }
      case sqlast::ColumnDefinition::Type::DATETIME: {
        record.SetDateTime(std::make_unique<std::string>(std::move(value)),
                           idx);
        break;
      }
      default:
        LOG(FATAL) << "UNREACHABLE";
    }

    ptr += len + 1;
  }

  return record;
}

// For updating.
RocksdbRecord RocksdbRecord::Update(
    const std::unordered_map<size_t, std::string> &update,
    const dataflow::SchemaRef &schema, const std::string &shard_name) const {
  // Encode key.
  std::string key = shard_name + __ROCKSSEP;
  const auto &keys = schema.keys();
  CHECK_EQ(keys.size(), 1u) << "schema has too many keys " << schema;
  int idx = keys.at(0);
  if (update.count(idx) == 1) {
    CHECK_NE(update.at(idx), "NULL");
    key += update.at(idx) + __ROCKSSEP;
  } else {
    rocksdb::Slice pk = this->GetPK();
    key.append(pk.data(), pk.size());
    key.push_back(__ROCKSSEP);
  }
  // Encode Value.
  std::string value;
  size_t i = 0;
  for (size_t col = 0; col < schema.size(); col++) {
    if (update.count(col) == 1) {
      value += update.at(col) + __ROCKSSEP;
      while (this->value_[i] != __ROCKSSEP) {
        i++;
      }
      i++;
    } else {
      while (this->value_[i] != __ROCKSSEP) {
        value.push_back(this->value_[i++]);
      }
      value.push_back(__ROCKSSEP);
      i++;
    }
  }
  return RocksdbRecord(std::move(key), std::move(value));
}

/*
 * RocksdbIndexRecord
 */
// Construct when reading from rocksdb.
RocksdbIndexRecord::RocksdbIndexRecord(const rocksdb::Slice &slice)
    : key_(slice.ToString()) {}

// When handling SQL statements.
RocksdbIndexRecord::RocksdbIndexRecord(const rocksdb::Slice &index_value,
                                       const std::string &shard_name,
                                       const rocksdb::Slice &pk)
    : key_() {
  this->key_ = index_value.ToString() + __ROCKSSEP + shard_name + __ROCKSSEP +
               pk.ToString() + __ROCKSSEP;
}

// For writing/encoding.
rocksdb::Slice RocksdbIndexRecord::Key() const {
  return rocksdb::Slice(this->key_);
}

// For reading/decoding.
rocksdb::Slice RocksdbIndexRecord::ExtractShardName() const {
  return ExtractColumn(this->key_, 1);
}
rocksdb::Slice RocksdbIndexRecord::ExtractPK() const {
  return ExtractColumn(this->key_, 2);
}

}  // namespace sql
}  // namespace pelton
