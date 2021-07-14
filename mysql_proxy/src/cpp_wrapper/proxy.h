#ifndef __PROXY_H__
#define __PROXY_H__
#include <stdbool.h>
#include <stddef.h>
// #include "../../../pelton/sqlast/ast_schema_enums.h"

#ifdef __cplusplus
extern "C" {
#endif
enum ColumnDefinitionTypeEnum { UINT, INT, TEXT, DATETIME };
struct CResult {
  // pelton::sqlast::ColumnDefinitionTypeEnum col_types[64];
  enum ColumnDefinitionTypeEnum col_types[64];
  char *col_names[64]; 
  size_t num_rows; // records
  size_t num_cols;
  union RecordData {
    long unsigned int UINT;
    int INT;
    char *TEXT;
    char *DATETIME;
  } records[];
};

struct ConnectionC {
  void *cpp_conn;
  bool connected;
};

struct ConnectionC open_c(const char *db_dir, const char *db_username,
                          const char *db_password);
bool exec_ddl(struct ConnectionC *c_conn, const char *query);
int exec_update(struct ConnectionC *c_conn, const char *query);
struct CResult *exec_select(struct ConnectionC *c_conn, const char *query);
bool close_conn(struct ConnectionC *c_conn);
void destroy_select(struct CResult *c_result);
void destroy_conn(void *c_conn);

#ifdef __cplusplus
}
#endif

#endif // __PROXY_H__