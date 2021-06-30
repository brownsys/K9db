#include "open.h"

#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "../../../pelton/pelton.h"

ConnectionC create()
{
    std::cout << "C-Wrapper: creating new pelton::Connection" << std::endl;
    ConnectionC c_conn = {new pelton::Connection()};

    std::cout << "C-Wrapper: cpp_conn is stored at " << c_conn.cpp_conn << std::endl;
    return c_conn;
}

void destroy(void *cpp_conn)
{
    std::cout << "C-Wrapper: destroy" << std::endl;
    delete reinterpret_cast<pelton::Connection *>(cpp_conn);
}

ConnectionC open_c(const char *db_dir, const char *db_username, const char *db_password)
{
    std::cout << "C-Wrapper: starting open_c()" << std::endl;
    std::cout << "C-Wrapper: db_dir is: " << std::string(db_dir) << std::endl;
    std::cout << "C-Wrapper: db_username is: " << std::string(db_username) << std::endl;
    std::cout << "C-Wrapper: db_passwored is: " << std::string(db_password) << std::endl;

    ConnectionC c_conn = create();

    // convert void pointer of ConnectionC struct into c++ class instance Connection
    pelton::Connection *cpp_conn =
        reinterpret_cast<pelton::Connection *>(c_conn.cpp_conn);

    // convert char* to const std::string
    const std::string c_db_dir(db_dir);
    const std::string c_db_username(db_username);
    const std::string c_db_password(db_password);

    // call c++ function from C with converted types
    std::cout << "C-Wrapper: running pelton::open" << std::endl;
    bool response = pelton::open(c_db_dir, c_db_username, c_db_password, cpp_conn);
    std::cout << "C-Wrapper: open response from pelton is: " << response << std::endl;

    // set boolean for C++ response (connected or not)
    c_conn.connected = response;

    return c_conn;
}

bool close_c(ConnectionC c_conn)
{
    std::cout << "C-Wrapper: starting close_c" << std::endl;
    pelton::Connection *cpp_conn =
        reinterpret_cast<pelton::Connection *>(c_conn.cpp_conn);

    bool response = pelton::close(cpp_conn);
    std::cout << "C-Wrapper: close response is: " << response << std::endl;

    // ? ownership of c_conn transferred here, so rust won't trigger destructor, so no double free
    destroy(cpp_conn);
    return response;
}