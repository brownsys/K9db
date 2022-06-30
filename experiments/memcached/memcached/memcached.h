// Connection to memcached.
#ifndef EXPERIMENTS_MEMCACHED_MEMCACHED_MEMCACHED_H_
#define EXPERIMENTS_MEMCACHED_MEMCACHED_MEMCACHED_H_

#include <cassert>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "libmemcached/memcached.h"
#include "memcached/encode.h"

class MemcachedConnection {
 public:
  MemcachedConnection() : conn_(memcached_create(NULL)), servers_(nullptr) {
    memcached_return st;
    // Connect to Memcached server.
    servers_ = memcached_server_list_append(nullptr, "localhost", 11211, &st);
    st = memcached_server_push(this->conn_, this->servers_);
    if (st != MEMCACHED_SUCCESS) {
      std::cerr << "Couldn't add server: "
                << memcached_strerror(this->conn_, st) << std::endl;
      assert(false);
    }
  }

  ~MemcachedConnection() {
    memcached_free(this->conn_);
    if (this->servers_ != nullptr) {
      memcached_server_list_free(this->servers_);
    }
  }

  // Writing to a memcached server.
  uint64_t Write(size_t query, const MemcachedKey &key,
                 const std::vector<MemcachedRecord> &records) {
    uint64_t size = 0;
    for (size_t i = 0; i < records.size(); i++) {
      // Key.
      std::string complete_key = EncodeKey(query, key, i);
      size_t keylen = complete_key.size();
      char *keybuf = new char[keylen];
      memcpy(keybuf, complete_key.c_str(), keylen);

      // Record.
      size_t recordlen = records.at(i).size();
      char *recordbuf = new char[recordlen];
      memcpy(recordbuf, records.at(i).c_str(), recordlen);

      // Put C-buffs in memcached.
      size += keylen + recordlen;
      auto status = memcached_add(this->conn_, keybuf, keylen, recordbuf,
                                  recordlen, 0, static_cast<uint32_t>(0));
      if (status != MEMCACHED_SUCCESS) {
        std::cerr << "Couldn't set row: "
                  << memcached_strerror(this->conn_, status) << std::endl;
        std::cerr << complete_key << std::endl;
        std::cerr << records.at(i) << std::endl;
        assert(false);
      }

      // Free buffers.
      delete[] keybuf;
      delete[] recordbuf;
    }
    return size;
  }

 private:
  memcached_st *conn_;
  memcached_server_st *servers_;
};

#endif  // EXPERIMENTS_MEMCACHED_MEMCACHED_MEMCACHED_H_
