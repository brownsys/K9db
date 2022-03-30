// NOLINTNEXTLINE
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>

#include "pelton/util/status.h"
#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"

// Data generation.
namespace gen {

#define USER_COUNT 10000
#define PUR_COUNT 5
#define OBJ_COUNT 10

static size_t USER = 0;
static size_t PUR = 0;
static size_t COUNTER = 0;

static const std::vector<std::string> FIRST_COLS = {"dec"};
static const std::vector<std::string> SECOND_COLS = {"src", "obj", "cat",
                                                     "acl"};
static const std::vector<std::string> THIRD_COLS = {"shr"};

std::string randomDigits(size_t size) {
  std::string digits = "";
  for (size_t i = 0; i < size; i++) {
    // NOLINTNEXTLINE
    digits += std::to_string(rand() % 10);
  }
  return digits;
}

std::vector<std::string> GenerateRecord() {
  std::vector<std::string> record;
  // Key.
  record.push_back("key" + randomDigits(19));
  // Dec.
  for (const std::string &col : FIRST_COLS) {
    record.push_back(col + std::to_string(COUNTER));
  }
  // User.
  record.push_back("user" + std::to_string(USER));
  // Src, Obj, Cat, Acl.
  for (const std::string &col : SECOND_COLS) {
    record.push_back(col + std::to_string(COUNTER));
  }
  // Data.
  record.push_back(randomDigits(11) + "-" + randomDigits(51) + "-" +
                   randomDigits(12) + "-" + randomDigits(13) + "-" +
                   randomDigits(9));
  // Purpose.
  record.push_back("purpose" + std::to_string(PUR));
  // Shr.
  for (const std::string &col : THIRD_COLS) {
    record.push_back(col + std::to_string(COUNTER));
  }
  // Increase counters.
  USER = (USER + 1) % USER_COUNT;
  PUR = (PUR + 1) % PUR_COUNT;
  COUNTER = (COUNTER + 1) % OBJ_COUNT;
  return record;
}

}  // namespace gen

// Encoding.
namespace encode {

#define __ROCKSSEP 30

std::string Encode(const std::vector<std::string> &record) {
  std::string str = "";
  for (const std::string &val : record) {
    str += val + static_cast<char>(__ROCKSSEP);
  }
  return str;
}

}  // namespace encode.

// Rocksdb wrappers.
namespace rdb {

class PrefixTransform : public rocksdb::SliceTransform {
 public:
  PrefixTransform() = default;

  const char *Name() const override { return "MyBenchTransform"; }
  bool InDomain(const rocksdb::Slice &key) const override { return true; }
  rocksdb::Slice Transform(const rocksdb::Slice &key) const override {
    return key;
  }
};

static rocksdb::DB *db = nullptr;
static rocksdb::ColumnFamilyHandle *handle = nullptr;

void Open() {
  std::string path = "/tmp/pelton/rocksdb/bench";

  // Block options.
  rocksdb::BlockBasedTableOptions block_opts;
  block_opts.data_block_index_type =
      rocksdb::BlockBasedTableOptions::kDataBlockBinaryAndHash;
  block_opts.index_type = rocksdb::BlockBasedTableOptions::kHashSearch;
  block_opts.block_cache = rocksdb::NewLRUCache(500 * 1048576);  // 500MB cache.

  // Options.
  rocksdb::Options opts;
  // opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(block_opts));
  opts.create_if_missing = true;
  opts.error_if_exists = true;
  // opts.IncreaseParallelism();
  // opts.OptimizeLevelStyleCompaction();
  // opts.prefix_extractor.reset(new PrefixTransform());
  // opts.comparator = rocksdb::BytewiseComparator();

  // Open the database.
  PANIC_STATUS(rocksdb::DB::Open(opts, path, &db));
}

void CreateTable() {
  rocksdb::ColumnFamilyOptions options;
  // options.prefix_extractor.reset(new PrefixTransform());
  // options.comparator = rocksdb::BytewiseComparator();
  PANIC_STATUS(db->CreateColumnFamily(options, "usertable", &handle));
}

void Insert(const std::string &key, const std::string &row) {
  auto options = rocksdb::WriteOptions();
  options.sync = true;
  PANIC_STATUS(db->Put(options, handle, key, row));
}

std::string Read(const std::string &key) {
  std::string row;
  rocksdb::ReadOptions options;
  PANIC_STATUS(db->Get(options, handle, key, &row));
  return row;
}

void Close() { db->Close(); }

}  // namespace rdb

#define ROW_COUNT 10000
#define READ_COUNT 20000

int main(int argc, char **argv) {
  auto tstart = std::chrono::steady_clock::now();
  // Generate data.
  std::vector<std::pair<std::string, std::string>> data;
  for (size_t i = 0; i < ROW_COUNT; i++) {
    std::vector<std::string> record = gen::GenerateRecord();
    data.emplace_back(record.at(0), encode::Encode(record));
  }

  // Open database.
  rdb::Open();
  rdb::CreateTable();
  std::cout << "DB Opened!" << std::endl;
  std::cout << std::endl;

  // Insert data.
  auto start = std::chrono::steady_clock::now();
  for (const auto &[key, row] : data) {
    rdb::Insert(key, row);
  }
  auto end = std::chrono::steady_clock::now();
  auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start)
                  .count();
  std::cout << "Inserts " << diff << "[us]" << std::endl;
  std::cout << "Inserts " << double(ROW_COUNT) / (double(diff) / 1000 / 1000)
            << "ops/s" << std::endl;

  // Read data.
  start = std::chrono::steady_clock::now();
  for (size_t i = 0; i < READ_COUNT; i++) {
    // NOLINTNEXTLINE
    const std::string &key = data.at(rand() % data.size()).first;
    std::string row = rdb::Read(key);
  }
  end = std::chrono::steady_clock::now();
  diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start)
             .count();
  std::cout << std::endl;
  std::cout << "Reads " << diff << "[us]" << std::endl;
  std::cout << "Reads " << double(READ_COUNT) / (double(diff) / 1000 / 1000)
            << "ops/s" << std::endl;

  // Done.
  auto tend = std::chrono::steady_clock::now();
  diff = std::chrono::duration_cast<std::chrono::microseconds>(tend - tstart)
             .count();
  std::cout << std::endl;
  std::cout << "Total time " << (diff / 1000) << "[ms]" << std::endl;

  // Shutdown.
  rdb::Close();
}
