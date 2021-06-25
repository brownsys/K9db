#ifndef __OPEN2_H__
#define __OPEN2_H__
#include <stdbool.h>

#ifdef __cplusplus
extern "C"
{
#endif
    struct ConnectionC;
    typedef struct ConnectionC ConnectionC_t;
    bool open_c(char *query, char *db_username, char *db_password);

#ifdef __cplusplus
}
#endif

#endif // __OPEN2_H__