#include "open.h"

#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "../../../pelton/pelton.h"

ConnectionC create()
{
    std::cout << "C-Wrapper: creating new pelton::Connection..." << std::endl;
    ConnectionC c_conn = {new pelton::Connection()};
    std::cout << "C-Wrapper: cpp_conn is stored at " << c_conn.cpp_conn << std::endl;
    return c_conn;
}

void destroy(ConnectionC c_conn)
{
    delete reinterpret_cast<pelton::Connection *>(c_conn.cpp_conn);
}

// wrapper that calls c++ from c
bool open_c(char *db_dir, char *db_username, char *db_password, ConnectionC c_conn)
{
    std::cout << "C-Wrapper: starting open_c()..." << std::endl;
    std::cout << "C-Wrapper: db_dir is: " << std::string(db_dir) << std::endl;
    std::cout << "C-Wrapper: db_username is: " << std::string(db_username) << std::endl;
    std::cout << "C-Wrapper: db_passwored is: " << std::string(db_password) << std::endl;
    std::cout << "C-Wrapper: cpp_conn is: " << c_conn.cpp_conn << std::endl;
    // convert void pointer of ConnectionC struct into c++ class instance Connection 
    pelton::Connection *cpp_conn =
        reinterpret_cast<pelton::Connection *>(c_conn.cpp_conn);

    // convert char* to const std::string
    const std::string c_db_dir(db_dir);
    const std::string c_db_username(db_username);
    const std::string c_db_password(db_password);

    // call c++ function from C with converted types
    std::cout << "C-Wrapper: running pelton::open..." << std::endl;
    bool response = pelton::open(db_dir, db_username, db_password, cpp_conn);
    
    std::cout << "C-Wrapper: C++ response from pelton is: " << response << std::endl;

    return response;
}
