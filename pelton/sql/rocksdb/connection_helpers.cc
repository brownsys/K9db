#include "glog/logging.h"
#include "pelton/sql/rocksdb/connection.h"
#include "pelton/util/status.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"

namespace pelton {
namespace sql {

/*
 * HELPERS.
 */
/*
// Get record matching values in a value mapper FROM ALL SHARDS (either by key,
// index, or it).
std::pair<std::vector<std::string>, std::vector<std::string>>
RocksdbConnection::GetRecords(TableID table_id, const ValueMapper &value_mapper,
                              bool return_shards) {
  // Read Metadata.
  rocksdb::ColumnFamilyHandle *handler = this->handlers_.at(table_id).get();
  const dataflow::SchemaRef &schema = this->schemas_.at(table_id);
  size_t pk = this->primary_keys_.at(table_id);
  const auto &indexed_columns = this->indexed_columns_.at(table_id);
  std::vector<RocksdbIndex> &indices = this->indices_.at(table_id);
  const std::string &pkname = schema.NameOf(pk);

  // Calling KeyFinder
  auto key_info = KeyFinder(table_id, value_mapper);
  auto keys_raw = key_info.second;
  std::vector<std::string> keys;
  std::vector<std::string> shards;
  std::vector<std::string> result;
  if (key_info.first) {
    if (return_shards) {
      for (size_t i = 0; i < keys_raw.size(); i++) {
        // TO-DO: does repeated access slow performance
        keys.push_back(keys_raw[i].first + __ROCKSSEP + keys_raw[i].second +
                       __ROCKSSEP);
        shards.push_back(keys_raw[i].first);
      }
    } else {
      for (size_t i = 0; i < keys_raw.size(); i++) {
        keys.push_back(keys_raw[i].first + __ROCKSSEP + keys_raw[i].second +
                       __ROCKSSEP);
      }
    }

    // Look in rocksdb.
    if (keys.size() == 1) {
      std::string str;
      rocksdb::ReadOptions opts;
      opts.total_order_seek = true;
      opts.verify_checksums = false;

      rocksdb::Status status = this->db_->Get(
          opts, handler, EncryptKey(this->global_key_, keys.front()), &str);
      if (status.ok()) {
        // TO-DO - could be optimised
        result.push_back(std::move(Decrypt(
            this->GetUserKey(keys[0].substr(0, keys[0].find(__ROCKSSEP))),
            str)));
      } else if (!status.IsNotFound()) {
        PANIC(status);
      }
    } else if (keys.size() > 1) {
      // Make slices for keys.
      rocksdb::Slice *slices = new rocksdb::Slice[keys.size()];
      std::string *tmp = new std::string[keys.size()];
      rocksdb::PinnableSlice *pins = new rocksdb::PinnableSlice[keys.size()];
      rocksdb::Status *statuses = new rocksdb::Status[keys.size()];
      for (size_t i = 0; i < keys.size(); i++) {
        keys.at(i) = EncryptKey(this->global_key_, keys.at(i));
        slices[i] = rocksdb::Slice(keys.at(i));
        pins[i] = rocksdb::PinnableSlice(tmp + i);
      }
      // Use MultiGet.
      rocksdb::ReadOptions opts;
      opts.total_order_seek = true;
      opts.verify_checksums = false;
      this->db_->MultiGet(opts, handler, keys.size(), slices, pins, statuses);
      // Read values that were found.
      for (size_t i = 0; i < keys.size(); i++) {
        rocksdb::Status &status = statuses[i];
        if (status.ok()) {
          if (pins[i].IsPinned()) {
            tmp[i].assign(pins[i].data(), pins[i].size());
          }
          result.push_back(std::move(Decrypt(
              this->GetUserKey(keys[i].substr(0, keys[i].find(__ROCKSSEP))),
              tmp[i])));
        } else if (!status.IsNotFound()) {
          PANIC(status);
        }
      }
      // Cleanup memory.
      delete[] statuses;
      delete[] pins;
      delete[] tmp;
      delete[] slices;
    }
  } else {
    LOG(FATAL) << "case not implemented!";
  }

  return {result, shards};
}

// Filter records by where clause in abstract statement.
std::vector<std::string> RocksdbConnection::Filter(
    const dataflow::SchemaRef &schema, const sqlast::AbstractStatement *sql,
    std::vector<std::string> &&rows) {
  std::vector<std::string> filtered;
  filtered.reserve(rows.size());
  for (std::string &row : rows) {
    FilterVisitor filter(rocksdb::Slice(row), schema);
    if (filter.Visit(sql)) {
      filtered.emplace_back(std::move(row));
    }
  }
  return filtered;
}

std::pair<bool, std::vector<std::pair<std::string, std::string>>>
RocksdbConnection::KeyFinder(TableID table_id,
                             const ValueMapper &value_mapper) {
  // Read Metadata.
  const dataflow::SchemaRef &schema = this->schemas_.at(table_id);
  size_t pk = this->primary_keys_.at(table_id);
  const auto &indexed_columns = this->indexed_columns_.at(table_id);
  std::vector<RocksdbIndex> &indices = this->indices_.at(table_id);
  const std::string &pkname = schema.NameOf(pk);

  // Lookup either by PK or index.
  std::vector<std::pair<std::string, std::string>> keys;
  // std::vector<std::string> keys_raw;

  // TO-DO (Vedant) this is expensive, will that effect performance?
  auto it = std::find(indexed_columns.begin(), indexed_columns.end(), pk);
  if (it != indexed_columns.end() && value_mapper.HasBefore(pkname)) {
    auto i = it - indexed_columns.begin();
    RocksdbIndex &index = indices.at(i);
    keys = index.Get(value_mapper.Before(pkname));
    // for (size_t i = 0; i < keys_raw.size(); i++) {
    //   keys.push_back(keys_raw[i].first + __ROCKSSEP + keys_raw[i].second +
    //                  __ROCKSSEP);
    // }
    return {true, keys};
  }
  for (size_t i = 0; i < indexed_columns.size(); i++) {
    RocksdbIndex &index = indices.at(i);
    const std::string &idxcolumn = schema.NameOf(indexed_columns.at(i));
    if (value_mapper.HasBefore(idxcolumn)) {
      keys = index.Get(value_mapper.Before(idxcolumn));
      // for (size_t i = 0; i < keys_raw.size(); i++) {
      //   keys.push_back(keys_raw[i].first + __ROCKSSEP + keys_raw[i].second +
      //                  __ROCKSSEP);
      // }
      return {true, keys};
    }
  }
  return {false, {}};
}
*/

}  // namespace sql
}  // namespace pelton
