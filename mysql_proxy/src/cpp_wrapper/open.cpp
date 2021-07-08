#include "open.h"

#include <variant>
#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "../../../pelton/pelton.h"
#include "../../../pelton/dataflow/key.h"
#include "../../../pelton/dataflow/record.h"
#include "../../../pelton/dataflow/schema.h"
#include "../../../pelton/dataflow/types.h"
#include "../../../pelton/sqlast/ast.h"
#include "../../../pelton/util/ints.h"

namespace pelton
{
    inline namespace literals
    {
        uint64_t operator"" _u(unsigned long long x)
        {
            return static_cast<uint64_t>(x);
        }
    }
}

// namespace pelton
// {
//     namespace dataflow
//     {
using CType = pelton::sqlast::ColumnDefinition::Type;

ConnectionC create()
{
    std::cout << "C-Wrapper: creating new pelton::Connection" << std::endl;
    // create struct, initialize fields
    ConnectionC c_conn = {new pelton::Connection(), false};

    std::cout << "C-Wrapper: cpp_conn is stored at " << c_conn.cpp_conn << std::endl;
    return c_conn;
}

void destroy_conn(void *cpp_conn)
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

bool close_c(ConnectionC *c_conn)
{
    std::cout << "C-Wrapper: starting close_c" << std::endl;
    pelton::Connection *cpp_conn =
        reinterpret_cast<pelton::Connection *>(c_conn->cpp_conn);

    bool response = pelton::close(cpp_conn);
    if (response)
    {
        c_conn->connected = false;
        std::cout << "C-Wrapper: connection closed\n"
                  << std::endl;
        return true;
    }
    else
    {
        std::cout << "C-Wrapper: failed to close connection\n"
                  << std::endl;
        return false;
    }
}

bool exec_ddl(ConnectionC *c_conn, const char *query)
{
    pelton::Connection *cpp_conn =
        reinterpret_cast<pelton::Connection *>(c_conn->cpp_conn);
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

int exec_update(ConnectionC *c_conn, const char *query)
{
    pelton::Connection *cpp_conn =
        reinterpret_cast<pelton::Connection *>(c_conn->cpp_conn);
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

CResult *exec_select(ConnectionC *c_conn, const char *query)
{
    // std::vector<std::string> names = {"Col1", "Col2", "Col3"};
    // std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
    // std::unique_ptr<std::string> s1 = std::make_unique<std::string>("Hello!");
    // pelton::Record r1{schema, true, 0_u, std::move(s1), 10_s};
    // std::vector<Record> records;
    // records.push_back(r1.Copy());

    std::cout << "C-Wrapper: creating dummy pelton record and vector" << std::endl;
    std::vector<std::string> names2 = {"Col1"};
    std::vector<CType> types2 = {CType::UINT};
    // set col to be primary key (here column at index 0 is the primary key):
    std::vector<pelton::dataflow::ColumnID> keys2 = {0};

    pelton::dataflow::SchemaRef schema2 = pelton::dataflow::SchemaFactory::Create(names2, types2, keys2);
    // using pelton::operator""_u;
    using namespace pelton::literals;
    pelton::Record r2{schema2, true, 66_u};
    std::vector<pelton::Record> pelton_records;
    pelton_records.push_back(r2.Copy());

    std::cout << "C-Wrapper: converting vector of pelton records to vector of CRecord" << std::endl;
    // flexible array members only defined in C, not c++ so need malloc
    struct CResult *c_result = (CResult *)malloc(sizeof(struct CResult) + sizeof(struct CRecord) * pelton_records.size());

    for (int i = 0; i < pelton_records.size(); i++)
    {
        // have to do this for every column, so should really be nested loop
        c_result->records[i].record_data.UINT = pelton_records[i].GetUInt(i);
        std::cout << "C-Wrapper: iteration is : " << i << std::endl;
        std::cout << "C-Wrapper: value for this column in std::vector<pelton::Record> pelton_records is: " << pelton_records[i].GetUInt(0) << std::endl;
        std::cout << "C-Wrapper: value for this column in CRecord c_result is: " << c_result->records[i].record_data.UINT << std::endl;
    }
    return c_result;
}

void destroy_select(CResult *c_result)
{
    std::cout << "C-Wrapper: destroy_select to delete CResult" << std::endl;
    delete c_result;
}