#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "../../../pelton/pelton.h"
#include "open2.h"

// struct to hold C++ class object
struct ConnectionC
{
    void *cpp_conn;
    bool connected;

    // destructor so freed in rust when out of scope
    ~ConnectionC()
    {
        if (cpp_conn == NULL)
            return;
        delete static_cast<Connection *>(cpp_conn);
        free(this);
    }

    // for the destructor, do I need to free the C struct itself too? How?
    // rn just deleting the c++ 
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

void destroy(ConnectionC_t *c_conn)
{
    if (c_conn == NULL)
        return;
    delete static_cast<Connection *>(c_conn->cpp_conn);
    free(c_conn);
}

// wrapper that calls c++ from c
ConnectionC_t *open_c(char *query, char *db_username, char *db_password)
{
    ConnectionC_t *c_conn = create();
    // convert C parameters to c++ types (make instance of c++ class)
    Connection *cpp_conn;
    if (c_conn == NULL)
        return 0;
    // convert void pointer of ConnectionC struct into c++ class instance Connection
    cpp_conn = static_cast<Connection *>(c_conn->cpp_conn);

    // convert char* to const std::string
    const std::string c_query(query);
    const std::string c_db_username(db_username);
    const std::string c_db_password(db_password);

    // call c++ function from C with converted types
    bool response = open_c(query, db_username, db_password, cpp_conn);

    // set boolean for C++ response (connected or not)
    c_conn->connected = response;

    return c_conn;
}
