#ifndef __OPEN_H__
#define __OPEN_H__
#include <stdbool.h>

#ifdef __cplusplus
extern "C"
{
#endif
    struct ConnectionC {
      void *cpp_conn;
    };

    bool open_c(char *directory, char *db_username, char *db_password, struct ConnectionC connection);

    struct ConnectionC create();
    void destroy(struct ConnectionC c_conn);

#ifdef __cplusplus
}
#endif

#endif // __OPEN_H__
