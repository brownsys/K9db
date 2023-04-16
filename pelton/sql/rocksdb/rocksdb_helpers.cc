#include <type_traits>

#include "glog/logging.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sql/rocksdb/dedup.h"
#include "pelton/sql/rocksdb/filter.h"

namespace pelton {
namespace sql {
namespace rocks {

/*
 * Helper: Get records matching where condition.
 */
template <typename T, bool DEDUP>
std::vector<T> RocksdbSession::GetRecords(
    const std::string &table_name, const sqlast::BinaryExpression *const where,
    int limit) const {
  using SET = std::conditional<DEDUP, DedupIndexSet, IndexSet>::type;

  // T must be one of {SelectRecord, DeleteRecord}.
  constexpr bool Tselect = std::is_same<T, SelectRecord>::value;
  constexpr bool Tdelete = std::is_same<T, DeleteRecord>::value;
  static_assert(Tselect || Tdelete);

  const RocksdbTable &table = this->conn_->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();

  // Turn where condition into a value mapper.
  sqlast::ValueMapper value_mapper(schema);
  if (where != nullptr) {
    value_mapper.VisitBinaryExpression(*where);
  }

  // Hold resulting records.
  std::vector<T> records;

  // Look up existing indices.
  std::optional<SET> lookup;
  if constexpr (DEDUP) {
    lookup = table.IndexLookupDedup(&value_mapper, this->txn_.get(), limit);
  } else {
    lookup = table.IndexLookup(&value_mapper, this->txn_.get(), limit);
  }

  if (lookup.has_value()) {
    // Can lookup by index.
    // Index returns a set of lookup keys with no duplicate PKs!
    SET &set = *lookup;

    // Encrypt all keys; keep track of corresponding shards for decryption.
    std::vector<std::string> shards;
    std::vector<EncryptedKey> keys;
    for (auto it = set.begin(); it != set.end();) {
      auto node = set.extract(it++);
      RocksdbIndexRecord &key = node.value();
      shards.push_back(key.GetShard().ToString());
      keys.push_back(this->conn_->encryption_.EncryptKey(key.Release()));
    }

    // Multi Lookup by encrypted keys.
    std::vector<std::optional<EncryptedValue>> envalues =
        table.MultiGet(keys, this->txn_.get());

    // Decrypt all values.
    for (size_t i = 0; i < envalues.size(); i++) {
      std::optional<EncryptedValue> &opt = envalues.at(i);
      if (opt.has_value()) {
        std::string &shard = shards.at(i);
        RocksdbSequence value =
            this->conn_->encryption_.DecryptValue(shard, std::move(*opt));

        dataflow::Record record = value.DecodeRecord(schema, Tselect);
        if constexpr (Tselect) {
          records.push_back(std::move(record));
        } else if constexpr (Tdelete) {
          records.emplace_back(std::move(shard), std::move(keys.at(i)),
                               std::move(value), std::move(record));
        }
      }
    }
  } else {
    // No relevant index; iterate over everything.
    if (where != nullptr) {
      LOG(WARNING) << "Selecting by scan from table " << table_name;
    }

    // Iterate over everything while deduplicating by PK.
    RocksdbStream all = table.GetAll(this->txn_.get());
    DedupSet<std::string> dup_keys;
    for (auto [enkey, enval] : all) {
      EncryptedKey enkey_copy = enkey;
      if (limit != -1 && records.size() == static_cast<size_t>(limit)) {
        break;
      }

      // Decrypt key.
      RocksdbSequence key =
          this->conn_->encryption_.DecryptKey(std::move(enkey));

      // Use shard from decrypted key to decrypt value.
      if (!DEDUP || !dup_keys.Duplicate(key.At(1).ToString())) {
        // Decrypt record.
        std::string shard = key.At(0).ToString();
        RocksdbSequence value =
            this->conn_->encryption_.DecryptValue(shard, std::move(enval));
        dataflow::Record record = value.DecodeRecord(schema, Tselect);

        if constexpr (Tselect) {
          records.push_back(std::move(record));
        } else if constexpr (Tdelete) {
          records.emplace_back(std::move(shard), std::move(enkey_copy),
                               std::move(value), std::move(record));
        }
      }
    }
  }

  // Apply remaining filters (if any).
  if (!value_mapper.Empty()) {
    std::vector<T> filtered;
    for (size_t i = 0; i < records.size(); i++) {
      T &record = records.at(i);

      // Perform filter on record.
      bool filter;
      if constexpr (Tselect) {
        filter = InMemoryFilter(value_mapper, record);
      } else if constexpr (Tdelete) {
        filter = InMemoryFilter(value_mapper, record.record);
      }

      // Move record into result if filtered.
      if (filter) {
        filtered.push_back(std::move(record));
      }
    }
    records = std::move(filtered);
  }

  return records;
}

}  // namespace rocks
}  // namespace sql
}  // namespace pelton
