#ifndef __OPEN_H__
#define __OPEN_H__
#include <stdbool.h>
#include "../../../pelton/sqlast/ast_schema_enums.h"

#ifdef __cplusplus
extern "C"
{
#endif
    struct CResult
    {
        char col_names[64][64]; // => max 64 col names, each 64 chars in length
        // char col_types[64][64]; // => specified here so rust knows which union field to access
        pelton::sqlast::ColumnDefinitionTypeEnum col_types[64];
        unsigned long num_rows; // number of records
        unsigned long num_cols;
        union RecordData
        {
            long unsigned int UINT;
            int INT;
            char* TEXT;
            char* DATETIME;
        } * records[];
    };

    struct ConnectionC
    {
        void *cpp_conn;
        bool connected;
    };

    struct ConnectionC open_c(const char *db_dir, const char *db_username, const char *db_password);
    bool exec_ddl(struct ConnectionC *c_conn, const char *query);
    int exec_update(struct ConnectionC *c_conn, const char *query);
    struct CResult *exec_select(struct ConnectionC *c_conn, const char *query);
    bool close_conn(struct ConnectionC *c_conn);
    void destroy_select(struct CResult *c_result);
    void destroy_conn(void *c_conn);

#ifdef __cplusplus
}
#endif

#endif // __OPEN_H__