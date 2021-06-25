#include "open.h"

#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "../../../pelton/pelton.h"

ConnectionC create()
{
    ConnectionC c_conn = {new pelton::Connection()};
    return c_conn;
}

void destroy(ConnectionC c_conn)
{
    delete reinterpret_cast<pelton::Connection *>(c_conn.cpp_conn);
}

// wrapper that calls c++ from c
bool open_c(char *db_dir, char *db_username, char *db_password, ConnectionC c_conn)
{
    std::cout << "hello?" << std::endl;
    // convert void pointer of ConnectionC struct into c++ class instance Connection 
    pelton::Connection *cpp_conn =
        reinterpret_cast<pelton::Connection *>(c_conn.cpp_conn);

    // convert char* to const std::string
    const std::string c_db_dir(db_dir);
    const std::string c_db_username(db_username);
    const std::string c_db_password(db_password);

    // call c++ function from C with converted types
    bool response = pelton::open(db_dir, db_username, db_password, cpp_conn);
    
    std::cout << "hello again!" << response << std::endl;

    return response;
}
