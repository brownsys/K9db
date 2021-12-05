#ifndef BASELINE_MEMCACHED_H_
#define BASELINE_MEMCACHED_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

// Represent a value in plain C.
typedef enum { UINT, INT, TEXT } Type;

typedef struct {
  Type type;
  uint64_t value;
} Value;

typedef struct {
  size_t size;
  Value *values;
} Record;

typedef struct {
  size_t size;
  Record *records;
} Result;

// Destructors.
void DestroyValue(Value *v);
void DestroyRecord(Record *r);
void DestroyResult(Result *r);

// Get value by type.
int64_t GetInt(const Value *val);
uint64_t GetUInt(const Value *val);
const char *GetStr(const Value *val);

// Create value by type.
Value SetInt(int64_t v);
Value SetUInt(uint64_t v);
Value SetStr(const char *v);

// Initialize the DB connection
bool Initialize(const char *db_name, const char *db_username,
                const char *db_password, const char *seed);

// Cache a query in memcached.
// returns -1 on error, otherwise returns a cache id which can be used to
// lookup data for the query in cache.
int Cache(const char *query);

// Evict and recompute all caches for given table.
void Update(const char *table);

// Read by id and key.
Result Read(int id, const Record *key);

// For tests.
void __ExecuteDB(const char *stmt);

// Freeing memory.
void Close();

#ifdef __cplusplus
}
#endif  // __cplusplus

#endif  // BASELINE_MEMCACHED_H_
