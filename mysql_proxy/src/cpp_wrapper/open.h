#ifndef __OPEN_H__
#define __OPEN_H__
#include <stdbool.h>

#ifdef __cplusplus
extern "C"
{
#endif
    // enum ColType
    // {
    //     UINT,
    //     INT,
    //     TEXT,
    //     DATETIME
    // };
    struct CRecord
    {
        union RecordData
        {
            long unsigned int UINT;
            int INT;
            // char* TEXT;
            // char* DATETIME;
        } record_data;
    };
    // struct CSchema {
    // int num_cols;
    // int num_rows;
    // ColType col_types[num_cols];
    // ColType col_names[num_cols];
    // };
    struct CResult
    {
        long unsigned int num_records;
        // pointer to first element of vector of CRecord structs (avoids having to declare a fixed size for array)
        struct CRecord records[];
        // CSchema schema;
    };

    // convert std::string & unique_ptr to C char*, making sure to deallocate all memory when done
    // could have class for string that has destructor

    struct ConnectionC
    {
        void *cpp_conn;
        bool connected;
    };
    struct ConnectionC open_c(const char *db_dir, const char *db_username, const char *db_password);
    bool exec_ddl(struct ConnectionC *c_conn, const char *query);
    int exec_update(struct ConnectionC *c_conn, const char *query);
    struct CResult* exec_select(struct ConnectionC *c_conn, const char *query);
    bool close_c(struct ConnectionC *c_conn);
    void destroy_select(struct CResult *c_result);
    void destroy_conn(void *c_conn);

#ifdef __cplusplus
}
#endif

#endif // __OPEN_H__