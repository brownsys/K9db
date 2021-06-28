#ifndef __OPEN_H__
#define __OPEN_H__
#include <stdbool.h>

#ifdef __cplusplus
extern "C"
{
#endif
    struct ConnectionC
    {
        void *cpp_conn;
        bool connected;
    };
    struct ConnectionC open_c(const char *query,const char *db_username,const char *db_password);
    void destroy(void* c_conn);

#ifdef __cplusplus
}
#endif

#endif // __OPEN_H__