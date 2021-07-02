#ifndef __OPEN_H__
#define __OPEN_H__
#include <stdbool.h>

#ifdef __cplusplus
extern "C"
{
#endif
    struct QueryResponse {
        const char* response_type;
        bool ddl;
        int update;
    };
    struct ConnectionC
    {
        void *cpp_conn;
        bool connected;
    };
    struct ConnectionC open_c(const char* db_dir,const char* db_username, const char* db_password);
    bool close_c(struct ConnectionC *c_conn);
    struct QueryResponse exec_c(struct ConnectionC *c_conn, const char* query);
    void destroy(void* c_conn);

#ifdef __cplusplus
}
#endif

#endif // __OPEN_H__