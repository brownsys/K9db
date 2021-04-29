#include "pelton/planner/planner.h"
#include <iostream>
#include <limits>
#include <string>
#include <utility>
#include <vector>
#include <fstream>

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "pelton/pelton.h"
#include "pelton/util/perf.h"
#include <sstream>

DEFINE_string(db_username, "pelton", "MYSQL username to connect with");
DEFINE_string(db_password, "pelton", "MYSQL pwd to connect with");

namespace
{
    // Expects that two vectors are equal regardless of order
    inline void EXPECT_EQ_MSET(std::vector<std::string> expected,
                               std::vector<std::string> actual,
                               std::string query)
    {
        // sort to remove differences in order
        std::sort(actual.begin(), actual.end());
        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(expected, actual) << "failed query was: " << query;
    }

    template <typename T>
    std::string tostring(const T &x)
    {
        std::ostringstream os;
        os << x;
        return os.str();
    }

    // CREATE TABLE queries.
    std::vector<std::string> CREATES{};

    // Inserts
    std::vector<std::string> INSERTS{};

    // Updates
    std::vector<std::string> UPDATES{};

    // Deletes
    std::vector<std::string> DELETES{};

    // Flows.
    std::vector<std::pair<std::string, std::string>> FLOWS{};

    // Flow reads (and queries)
    std::vector<std::string> FLOW_READS_AND_QUERIES{};

    // Expected query results
    std::vector<std::vector<std::string>> EXPECTED = {};

    
}; // namespace

int main(int argc, char **argv)
{
    // Command line arguments and help message
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Initialize Google’s logging library.
    google::InitGoogleLogging("correctness");

    // Read MySql configurations.
    const std::string &db_username = FLAGS_db_username;
    const std::string &db_password = FLAGS_db_password;

    // * process schema (input file 1)
    // std::ifstream schema("bin/data/trivial-schema.sql");
    std::ifstream schema(argv[1]);
    std::string line;
    if (schema.is_open())
    {
        std::cout << "schema file opened" << std::endl;
        std::string table = "";
        while (std::getline(schema, line))
        {
            // if empty line, skip
            if (line == "")
                continue;

            // if not empty line, process SQL
            // append to string until find ; indicating end of
            // CREATE TABLE statement (done to separate element in vector)
            table.append(line);

            // if end of create statement, push back to vector
            if (line.find(";") != std::string::npos)
            {
                CREATES.push_back(table);
                table = "";
            }
        }
        schema.close();
    }
    else
    {
        std::cout << "schema file closed" << std::endl;
    }

    // * process queries (input file 2)
    // std::ifstream queries("bin/data/trivial-queries.sql");
    std::ifstream queries(argv[2]);
    if (queries.is_open())
    {
        std::cout << "queries file opened" << std::endl;
        while (std::getline(queries, line))
        {
            if (line == "")
                continue;
            if (line.find("VIEW") != std::string::npos)
            {
                FLOWS.push_back(std::make_pair("", line));
            }
            else if (line.find("INSERT") != std::string::npos)
            {
                INSERTS.push_back(line);
            }
            else if (line.find("SELECT") != std::string::npos)
            {
                FLOW_READS_AND_QUERIES.push_back(line);
            }
            else if (line.find("DELETE") != std::string::npos)
            {
                DELETES.push_back(line);
            }
            else if (line.find("UPDATE") != std::string::npos)
            {
                UPDATES.push_back(line);
            }
            // std::string token = strtok(queries, " ");
            // while (token != NULL)
            // {
            //     std::string view = "VIEW";
            //     if (token == view)
            //     {
            //         token = strtok(NULL, " ");
            //         FLOWS.push_back(std::make_pair(token, queries));
            //     }
            //     token = strtok(NULL, " ");
        }
        queries.close();
    }
    else
    {
        std::cout << "queries file closed" << std::endl;
    }

    // * process expected results
    std::ifstream expected(argv[3]);
    if (expected.is_open())
    {
        std::cout << "expected results file open" << std::endl;
        // vector containing results for one query
        std::vector<std::string> query_results;
        while (std::getline(expected, line))
        {
            if (line == "")
                continue;
            // if ; found, end of query results
            if (line.find(";") != std::string::npos)
            {
                // push_back the vector of query results (prepend | for string formatting)
                EXPECTED.push_back(query_results);
                // clear vector
                query_results.clear();
            }
            query_results.push_back(line.insert(0, 1, '|'));
        }
        expected.close();
    }
    else
    {
        std::cout << "expected results file closed" << std::endl;
    }

    // * run schema and queries in pelton

    pelton::perf::Start("all");

    // Open connection to sharder.
    pelton::Connection connection;
    pelton::open("", db_username, db_password, &connection);
    CHECK(pelton::exec(&connection, "SET echo;").ok());

    // Create all the tables.
    std::cout << "Create the tables ... " << std::endl;
    for (std::string &create : CREATES)
    {
        std::cout << std::endl;
        CHECK(pelton::exec(&connection, create).ok());
    }
    std::cout << std::endl;

    // Add flows.
    std::cout << "Installing flows ... " << std::endl;
    for (const auto &[name, query] : FLOWS)
    {
        std::cout << name << std::endl;
        CHECK(pelton::exec(&connection, query).ok());
    }
    pelton::shutdown_planner();
    std::cout << std::endl;

    // Insert data into the tables.
    std::cout << "Insert data into tables ... " << std::endl;
    for (std::string &insert : INSERTS)
    {
        std::cout << std::endl;
        CHECK(pelton::exec(&connection, insert).ok());
    }
    std::cout << std::endl;

    // Updates
    std::cout << "Update data in tables ... " << std::endl;
    for (std::string &update : UPDATES)
    {
        std::cout << std::endl;
        CHECK(pelton::exec(&connection, update).ok());
    }
    std::cout << std::endl;

    // Deletes
    std::cout << "Delete data from tables ... " << std::endl;
    for (std::string &del : DELETES)
    {
        std::cout << std::endl;
        CHECK(pelton::exec(&connection, del).ok());
    }
    std::cout << std::endl;

    // * check flows from pelton to expected results

    std::cout << "Check flows and queries... " << std::endl;
    // run each query
    long unsigned int i = 0;
    for (const auto &query : FLOW_READS_AND_QUERIES)
    {
        std::cout << std::endl;
        auto status = pelton::exec(&connection, query);
        CHECK(status.ok());

        // Print(std::move(status.value()));
        pelton::SqlResult &result = status.value();

        // vector to hold query results.
        std::vector<std::string> query_actual;

        // add schema e.g. |id(INT, KEY)|
        query_actual.push_back(tostring(result.GetSchema()));

        // add records to query_results
        for (const pelton::Record &record : result)
        {
            // reverses the order due to appending to the end?
            query_actual.push_back(tostring(record));
        }

        if (i >= EXPECTED.size()) {
            break;
        }
        // orderless comparison via google test
        EXPECT_EQ_MSET(EXPECTED[i], query_actual, tostring(query));
        i++;
    }
    std::cout << std::endl;

    // Close connection.
    pelton::close(&connection);

    // Print performance profile.
    pelton::perf::End("all");
    pelton::perf::PrintAll();

    // Done.
    std::cout << "exit" << std::endl;
    return 0;
}