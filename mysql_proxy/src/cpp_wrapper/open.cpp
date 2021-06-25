#include <stdlib.h>
#include "../../../pelton/pelton.h"
#include "open.h"

// struct to hold C++ class object
struct ConnectionC
{
    void *obj;
};

ConnectionC_t *create()
{
    ConnectionC_t *c_conn;
    Connection *cpp_conn;

    c_conn = (typeof(c_conn))malloc(sizeof(*c_conn));
    cpp_conn = new Connection();
    // ? call Initialize() or other method here?

    c_conn->cpp_conn = cpp_conn;
    return c_conn;
}

void destroy(ConnectionC_t *m)
{
    if (m == NULL)
        return;
    delete static_cast<Connection *>(m->obj);
    free(m);
}

// wrapper that calls c++ from c
bool open_c(char *query, char *db_username, char *db_password, ConnectionC_t *connection)
{
    // convert C parameters to c++ types (make instance of c++ class)
    Connection *obj;
    if (connection == NULL)
        return 0;
    // convert void pointer of ConnectionC struct into c++ class instance Connection 
    obj = static_cast<Connection *>(connection->obj);

    // convert char* to const std::string
    const std::string c_query(query);
    const std::string c_db_username(db_username);
    const std::string c_db_password(db_password);

    // call c++ function from C with converted types
    bool response = open_c(query, db_username, db_password, obj);

    return response;
}