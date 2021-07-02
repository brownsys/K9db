#include "open.h"

#include <variant>
#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "../../../pelton/pelton.h"

ConnectionC create()
{
    std::cout << "C-Wrapper: creating new pelton::Connection" << std::endl;
    // create struct, initialize fields
    ConnectionC c_conn = {new pelton::Connection(), false};

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
    std::cout << "C-Wrapper: starting open_c" << std::endl;
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
    if (response)
    {
        std::cout << "C-Wrapper: connection opened\n"
                  << std::endl;
    }
    else
    {
        std::cout << "C-Wrapper: failed to open connection\n"
                  << std::endl;
    }

    // set boolean for C++ response (connected or not)
    c_conn.connected = response;

    return c_conn;
}

ConnectionC close_c(ConnectionC c_conn)
{
    std::cout << "C-Wrapper: starting close_c" << std::endl;
    pelton::Connection *cpp_conn =
        reinterpret_cast<pelton::Connection *>(c_conn.cpp_conn);

    bool response = pelton::close(cpp_conn);
    if (response)
    {
        c_conn.connected = false;
        std::cout << "C-Wrapper: connection closed\n"
                  << std::endl;
    }
    else
    {
        std::cout << "C-Wrapper: failed to close connection\n"
                  << std::endl;
    }

    return c_conn;
}

bool exec_ddl(ConnectionC c_conn, std::string query)
{
    pelton::Connection *cpp_conn =
        reinterpret_cast<pelton::Connection *>(c_conn.cpp_conn);
    absl::StatusOr<pelton::SqlResult> result = pelton::exec(cpp_conn, query);
    if (result.ok() && result.value().IsStatement() && result.value().Success())
    {
        return true;
    }
    else
    {
        return false;
    }
}

int exec_update(ConnectionC c_conn, std::string query)
{
    pelton::Connection *cpp_conn =
        reinterpret_cast<pelton::Connection *>(c_conn.cpp_conn);
    absl::StatusOr<pelton::SqlResult> result = pelton::exec(cpp_conn, query);

    if (result.ok() && result.value().IsUpdate())
    {
        return result.value().UpdateCount();
    }
    else
    {
        return -1;
    }
}

ConnectionC exec_c(ConnectionC c_conn, const char *query)
{
    std::cout << "C-Wrapper: starting exec" << std::endl;
    const std::string query_string(query);
    std::string query_type;
    std::string response_type;
    bool bool_response;
    int int_response;

    // does pelton support DROP, DELETE, ALTER or SET?
    if (query_string.find("CREATE") != std::string::npos)
    {
        query_type = "DDL";
        response_type = "bool";
        bool_response = exec_ddl(c_conn, query_string);
        if (bool_response)
        {
            std::cout << "C-Wrapper: DDL query execution successful" << std::endl;
        }
        else
        {
            std::cout << "C-Wrapper: DDL query execution failed" << std::endl;
        }
    }
    else if (query_string.find("INSERT") != std::string::npos || query_string.find("UPDATE") != std::string::npos || query_string.find("DELETE") != std::string::npos)
    {
        query_type = "INSERT";
        response_type = "int";
        int_response = exec_ddl(c_conn, query_string);
        if (int_response != -1)
        {
            std::cout << "C-Wrapper: update query execution successful. Number of changed rows is: " << int_response << std::endl;
        }
        else
        {
            std::cout << "C-Wrapper: update query execution failed" << std::endl;
        }
    }
    std::cout << "C-Wrapper: query type is: " << query_type << std::endl;
    std::cout << "C-Wrapper: response type is: " << response_type << "\n"
              << std::endl;
    char *c_response_type = (char *)malloc(response_type.size() + 1);
    strcpy(c_response_type, response_type.c_str());

    QueryResponse q_response;

    if (response_type == "bool")
    {
        q_response = {.response_type = c_response_type, .ddl = bool_response};
    }
    else if (response_type == "int")
    {
        q_response = {.response_type = c_response_type, .update = int_response};
    }
    else
    {
        std::cout << "C-Wrapper: invalid response_type" << std::endl;
    }

    c_conn.query_response = q_response;
    return c_conn;
}