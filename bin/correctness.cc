#include "pelton/planner/planner.h"
#include <iostream>
#include <fstream>

#include "gtest/gtest.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "pelton/pelton.h"
#include "pelton/util/perf.h"

DEFINE_string(db_username, "pelton", "MYSQL username to connect with");
DEFINE_string(db_password, "pelton", "MYSQL pwd to connect with");
DEFINE_string(schema, "bin/data/lobster_schema_simplified.sql", "SQL schema input file");
DEFINE_string(queries, "bin/data/lobster_queries.sql", "SQL queries input file");
DEFINE_string(inserts, "bin/data/lobster_inserts.sql", "SQL insert statement input file");
DEFINE_string(expected_output, "bin/data/lobster_expected.txt", "File containing expected output");

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
    std::ifstream schema(FLAGS_schema);
    std::string line;
    if (schema.is_open())
    {
        std::cout << "schema file opened" << std::endl;
        std::string table = "";
        while (std::getline(schema, line))
        {
            // if line is empty or commented out, skip it
            if (line == "" || line.find("--skip--") != std::string::npos)
                continue;

            // append to string until ';' marking the end of a CREATE statement
            table.append(line);

            // if we've reached the end of a CREATE statement, add to CREATES
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
        std::cout << "schema file is closed" << std::endl;
    }

    // * process queries (input file 2)
    // int num_views = 5;
    // int num_queries = 5;

    std::ifstream queries(FLAGS_queries);
    if (queries.is_open())
    {
        std::cout << "queries file opened" << std::endl;
        while (std::getline(queries, line))
        {
            if (line == "" || line.find("--skip--") != std::string::npos)
                continue;
            if (line.find("VIEW") != std::string::npos)
            {
                // if (num_views == 0) continue;
                std::vector<std::string> split_on_space;
                std::istringstream iss(line);
                for (std::string s; iss >> s;)
                    split_on_space.push_back(s);
                // key of this pair is the name of the view at index 2
                FLOWS.push_back(std::make_pair(split_on_space[2], line));
                // num_views--;
            }
            else if (line.find("INSERT") != std::string::npos)
            {
                INSERTS.push_back(line);
            }
            else if (line.find("SELECT") != std::string::npos)
            {
                // if (num_queries == 0) continue;
                FLOW_READS_AND_QUERIES.push_back(line);
                // num_queries--;
            }
            else if (line.find("DELETE") != std::string::npos)
            {
                DELETES.push_back(line);
            }
            else if (line.find("UPDATE") != std::string::npos)
            {
                UPDATES.push_back(line);
            }
        }
        queries.close();
    }
    else
    {
        std::cout << "queries file is closed" << std::endl;
    }

    // * process expected results (input file 3)

    std::ifstream expected(FLAGS_expected_output);
    if (expected.is_open())
    {
        std::cout << "expected results file open" << std::endl;
        // vector containing result for one query
        std::vector<std::string> expected_result;
        while (std::getline(expected, line))
        {
            if (line == "" || line.find("--skip--") != std::string::npos)
                continue;
            // if ';' is found, we've reached the end of a single query's results
            if (line.find(";") != std::string::npos)
            {
                // add to the 2D vector of all query results
                EXPECTED.push_back(expected_result);
                // clear vector
                expected_result.clear();
                continue;
            }
            // '|' pre- and appended to match string formatting of pelton
            std::string format = "|";
            line.append(format);
            line.insert(0, 1, '|');
            expected_result.push_back(line);
        }
        expected.close();
    }
    else
    {
        std::cout << "expected results file is closed" << std::endl;
    }

    // * process inserts (input file 4)

    std::ifstream inserts(FLAGS_inserts);
    if (inserts.is_open())
    {
        std::cout << "inserts file opened" << std::endl;
        while (std::getline(inserts, line))
        {
            if (line == "" || line.find("--skip--") != std::string::npos)
                continue;
            if (line.find("INSERT") != std::string::npos)
            {
                INSERTS.push_back(line);
            }
        }
        inserts.close();
    }
    else
    {
        std::cout << "inserts file is closed" << std::endl;
    }

    // * run schema and queries using pelton

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

    // * compare pelton to expected results

    std::cout << "Check flows and queries... " << std::endl;
    // run each query
    long unsigned int i = 0;
    for (const auto &query : FLOW_READS_AND_QUERIES)
    {
        // if we exceed the number of expected results
        if (i >= EXPECTED.size())
        {
            break;
        }
        std::cout << std::endl;
        auto status = pelton::exec(&connection, query);
        CHECK(status.ok());

        // Print(std::move(status.value()));
        pelton::SqlResult &result = status.value();

        // vector to hold query results.
        std::vector<std::string> query_actual;

        // add records to query_results
        for (const pelton::Record &record : result)
        {
            query_actual.push_back(tostring(record));
        }

        // add schema e.g. |id(INT, KEY)| to actual & expected
        // query_actual.push_back(tostring(result.GetSchema()));
        // EXPECTED[i].push_back(tostring(result.GetSchema()));

        if (query.find("ORDER BY") != std::string::npos)
        {
            // ordered comparison
            EXPECT_EQ(EXPECTED[i], query_actual) << "failed query was: " << query;
        }
        else
        {
            // orderless comparison
            EXPECT_EQ_MSET(EXPECTED[i], query_actual, tostring(query));
        }
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
