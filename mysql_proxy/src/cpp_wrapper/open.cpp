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
#include "../../../pelton/sqlast/ast_schema.h"

namespace pelton
{
    inline namespace literals
    {
        uint64_t operator"" _u(unsigned long long x)
        {
            return static_cast<uint64_t>(x);
        }
        int64_t operator"" _s(unsigned long long x)
        {
            return static_cast<int64_t>(static_cast<long long>(x));
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

bool close_conn(ConnectionC *c_conn)
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
    std::cout << "C-Wrapper: creating dummy pelton record and vector" << std::endl;

    std::vector<std::string> names2 = {"Col1", "Col2", "Col3", "Col4"};
    std::vector<CType> types2 = {CType::UINT, CType::INT, CType::TEXT, CType::DATETIME};
    // set col to be primary key (here column at index 0 is the primary key):
    std::vector<pelton::dataflow::ColumnID> keys2 = {0};
    pelton::dataflow::SchemaRef schema2 = pelton::dataflow::SchemaFactory::Create(names2, types2, keys2);
    std::unique_ptr<std::string> s = std::make_unique<std::string>("I'm a string");
    std::unique_ptr<std::string> d = std::make_unique<std::string>("I'm a datetime");
    using namespace pelton::literals;
    pelton::Record r2{schema2, true, 66_u, -66_s, std::move(s), std::move(d)};
    std::vector<pelton::Record> pelton_records2;
    pelton_records2.push_back(r2.Copy());

    std::cout << "C-Wrapper: converting vector of pelton records to vector of CRecord" << std::endl;
    // allocate memory for CResult struct and the flexible array of union Record pointers (# pointers = # rows/pelton records) according the the number of pelton records
    struct CResult *c_result = (CResult *)malloc(sizeof(*c_result) + sizeof(*(c_result->records)) * pelton_records2.size());
    //                                           |-- CResult* --|    | ----  RecordData[]  ---- |   | number of records (rows) |

    c_result->num_rows = pelton_records2.size();
    c_result->num_cols = names2.size();
    // then allocate memory for the underlying union RecordData that lies behind each RecordData[] pointer (# unions = # of cols)
    for (int r = 0; r < c_result->num_rows; r++)
    {
        // allocate mem for all the cols of this row (# unions = # cols) | all the union arrays at each ptr
        c_result->records[r] = (CResult::RecordData *)malloc(sizeof(*(c_result->records[r])) * c_result->num_cols);
        //                                                  | ----     RecordData    ---- |   | number of columns |
    }

    // * Populate CResult schema

    std::cout << "C-Wrapper: converting pelton schema to CResult" << std::endl;
    // set number of columns
    // c_result->num_cols = schema2.GetSchema().size();
    c_result->num_cols = schema2.size();
    std::cout << "C-Wrapper: CResult schema size is : " << c_result->num_cols << std::endl;
    // for each column
    for (int i = 0; i < schema2.size(); i++)
    {
        // set column name
        const char *col_name = schema2.NameOf(i).c_str();
        strcpy(c_result->col_names[i], col_name);
        std::cout << "C-Wrapper: CResult col_name at index " << i << " is " << c_result->col_names[i] << std::endl;

        // set column type
        CType col_type = schema2.TypeOf(i); // .GetSchema().TypeOf(i)
        c_result->col_types[i] = col_type;
        std::cout << "C-Wrapper: CResult col_type at index " << i << " is " << c_result->col_types[i] << std::endl;
    }

    // * Populate CResult *records[]
    // for final version use while(sqlresult.HasNext() for outermost loop and the records_vector.size() as inner loop

    // for every record (row)
    for (int i = 0; i < pelton_records2.size(); i++)
    {
        std::cout << "C-Wrapper: row is : " << i << std::endl;
        // for every RecordData (col)
        for (int j = 0; j < names2.size(); j++)
        {
            std::cout << "C-Wrapper: col is : " << j << std::endl;

            // [i] row, [j] col
            if (c_result->col_types[j] == CType::UINT)
            {
                c_result->records[i][j].UINT = pelton_records2[i].GetUInt(j);
                std::cout << "C-Wrapper: row value for this column in std::vector<pelton::Record> pelton_records is: " << pelton_records2[i].GetUInt(j) << std::endl;
                std::cout << "C-Wrapper: row value for this column in CRecord c_result is: " << c_result->records[i][j].UINT << std::endl;
            }
            else if (c_result->col_types[j] == CType::INT)
            {
                c_result->records[i][j].INT = pelton_records2[i].GetInt(j);
                std::cout << "C-Wrapper: row value for this column in std::vector<pelton::Record> pelton_records is: " << pelton_records2[i].GetInt(j) << std::endl;
                std::cout << "C-Wrapper: row value for this column in CRecord c_result is: " << c_result->records[i][j].INT << std::endl;
            }
            else if (c_result->col_types[j] == CType::TEXT)
            {
                std::string cpp_val = pelton_records2[i].GetString(j);
                char *val = (char *)malloc(cpp_val.size() + 1);
                strcpy(val, cpp_val.c_str());
                c_result->records[i][j].TEXT = val;
                std::cout << "C-Wrapper: row value for this column in std::vector<pelton::Record> pelton_records is: " << pelton_records2[i].GetString(j) << std::endl;
                std::cout << "C-Wrapper: row value for this column in CRecord c_result is: " << c_result->records[i][j].TEXT << std::endl;
            }
            else if (c_result->col_types[j] == CType::DATETIME)
            {
                std::string cpp_val = pelton_records2[i].GetDateTime(j);
                char *val = (char *)malloc(cpp_val.size() + 1);
                strcpy(val, cpp_val.c_str());
                c_result->records[i][j].DATETIME = val;
                std::cout << "C-Wrapper: row value for this column in std::vector<pelton::Record> pelton_records is: " << pelton_records2[i].GetDateTime(j) << std::endl;
                std::cout << "C-Wrapper: row value for this column in CRecord c_result is: " << c_result->records[i][j].DATETIME << std::endl;
            }
            else {
                std::cout << "C-Wrapper: Invalid col_type" << std::endl;
            }
        }
    }
    return c_result;
}

void destroy_select(CResult *c_result)
{
    std::cout << "C-Wrapper: destroy_select to delete CResult" << std::endl;
    for (int r = 0; r < c_result->num_rows; r++)
    {
        free(c_result->records[r]);
    }
    free(c_result);
}