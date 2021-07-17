#include "proxy.h"
#include "../../../pelton/pelton.h"

using CType = pelton::sqlast::ColumnDefinition::Type;

ConnectionC create() {
  std::cout << "C-Wrapper: creating new pelton::Connection" << std::endl;
  // create struct, initialize fields
  ConnectionC c_conn = {new pelton::Connection(), false};

  std::cout << "C-Wrapper: cpp_conn is stored at " << c_conn.cpp_conn
            << std::endl;
  return c_conn;
}

void destroy_conn(void *cpp_conn) {
  std::cout << "C-Wrapper: destroy" << std::endl;
  delete reinterpret_cast<pelton::Connection *>(cpp_conn);
}

ConnectionC open_c(const char *db_dir, const char *db_username,
                   const char *db_password) {
  std::cout << "C-Wrapper: starting open_c" << std::endl;
  std::cout << "C-Wrapper: db_dir is: " << std::string(db_dir) << std::endl;
  std::cout << "C-Wrapper: db_username is: " << std::string(db_username)
            << std::endl;
  std::cout << "C-Wrapper: db_passwored is: " << std::string(db_password)
            << std::endl;

  ConnectionC c_conn = create();

  // convert void pointer of ConnectionC struct into c++ class instance
  // Connection
  pelton::Connection *cpp_conn =
      reinterpret_cast<pelton::Connection *>(c_conn.cpp_conn);

  // convert char* to const std::string
  const std::string c_db_dir(db_dir);
  const std::string c_db_username(db_username);
  const std::string c_db_password(db_password);

  // call c++ function from C with converted types
  std::cout << "C-Wrapper: running pelton::open" << std::endl;
  bool response =
      pelton::open(c_db_dir, c_db_username, c_db_password, cpp_conn);
  if (response) {
    std::cout << "C-Wrapper: connection opened\n" << std::endl;
  } else {
    std::cout << "C-Wrapper: failed to open connection\n" << std::endl;
  }

  // set boolean for C++ response (connected or not)
  c_conn.connected = response;

  return c_conn;
}

bool close_conn(ConnectionC *c_conn) {
  std::cout << "C-Wrapper: starting close_c" << std::endl;
  pelton::Connection *cpp_conn =
      reinterpret_cast<pelton::Connection *>(c_conn->cpp_conn);

  bool response = pelton::close(cpp_conn);
  if (response) {
    c_conn->connected = false;
    std::cout << "C-Wrapper: connection closed\n" << std::endl;
    return true;
  } else {
    std::cout << "C-Wrapper: failed to close connection\n" << std::endl;
    return false;
  }
}

bool exec_ddl(ConnectionC *c_conn, const char *query) {
  pelton::Connection *cpp_conn =
      reinterpret_cast<pelton::Connection *>(c_conn->cpp_conn);
  absl::StatusOr<pelton::SqlResult> result = pelton::exec(cpp_conn, query);
  return result.ok() && result.value().IsStatement() &&
         result.value().Success();
}

int exec_update(ConnectionC *c_conn, const char *query) {
  pelton::Connection *cpp_conn =
      reinterpret_cast<pelton::Connection *>(c_conn->cpp_conn);
  absl::StatusOr<pelton::SqlResult> result = pelton::exec(cpp_conn, query);

  if (result.ok() && result.value().IsUpdate()) {
    return result.value().UpdateCount();
  } else {
    return -1;
  }
}

void populate_column_types(CResult *c_result, pelton::SqlResult &sql_result) {

  // for each column
  for (int i = 0; i < c_result->num_cols; i++) {
    const std::string &cpp_col_name = sql_result.GetSchema().NameOf(i);
    char *col_name = (char *)malloc(cpp_col_name.size() + 1);
    strcpy(col_name, cpp_col_name.c_str());
    c_result->col_names[i] = col_name;

    // set column type
    CType col_type = sql_result.GetSchema().TypeOf(i);
    switch (col_type) {
    case CType::INT:
      c_result->col_types[i] = ColumnDefinitionTypeEnum::INT;
      break;
    case CType::UINT:
      c_result->col_types[i] = ColumnDefinitionTypeEnum::UINT;
      break;
    case CType::TEXT:
      c_result->col_types[i] = ColumnDefinitionTypeEnum::TEXT;
      break;
    case CType::DATETIME:
      c_result->col_types[i] = ColumnDefinitionTypeEnum::DATETIME;
      break;
    }
    std::cout << "C-Wrapper: CResult col_type at index " << i << " is "
              << c_result->col_types[i] << std::endl;
  }
}

void populate_records(CResult *c_result,
                      std::vector<pelton::dataflow::Record> &records) {
  size_t num_rows = c_result->num_rows;
  size_t num_cols = c_result->num_cols;
  // for every record (row)
  for (int i = 0; i < num_rows; i++) {
    std::cout << "C-Wrapper: row index is : " << i << std::endl;
    // for every col in this row
    for (int j = 0; j < num_cols; j++) {
      std::cout << "C-Wrapper: col index is : " << j << std::endl;
      if (c_result->col_types[j] == ColumnDefinitionTypeEnum::UINT) {
        // current row index * num cols per row + current col index
        c_result->records[i * num_cols + j].UINT = records[i].GetUInt(j);
      } else if (c_result->col_types[j] == ColumnDefinitionTypeEnum::INT) {
        c_result->records[i * num_cols + j].INT = records[i].GetInt(j);
      } else if (c_result->col_types[j] == ColumnDefinitionTypeEnum::TEXT) {
        std::string cpp_val = records[i].GetString(j);
        char *val = (char *)malloc(cpp_val.size() + 1);
        strcpy(val, cpp_val.c_str());
        c_result->records[i * num_cols + j].TEXT = val;
      } else if (c_result->col_types[j] == ColumnDefinitionTypeEnum::DATETIME) {
        std::string cpp_val = records[i].GetDateTime(j);
        char *val = (char *)malloc(cpp_val.size() + 1);
        strcpy(val, cpp_val.c_str());
        c_result->records[i * num_cols + j].DATETIME = val;
      } else {
        std::cout << "C-Wrapper: Invalid col_type" << std::endl;
      }
    }
  }
}

CResult *exec_select(ConnectionC *c_conn, const char *query) {
  pelton::Connection *cpp_conn =
      reinterpret_cast<pelton::Connection *>(c_conn->cpp_conn);
  absl::StatusOr<pelton::SqlResult> result = pelton::exec(cpp_conn, query);

  if (result.ok() && result.value().IsQuery()) {
    pelton::SqlResult &sql_result = result.value();
    std::vector<pelton::dataflow::Record> records = sql_result.Vectorize();
    size_t num_rows = records.size();
    size_t num_cols = sql_result.GetSchema().size();

    std::cout << "C-Wrapper: malloc CResult" << std::endl;
    // allocate memory for CResult struct and the flexible array of RecordData
    // unions
    struct CResult *c_result = (CResult *)malloc(
        sizeof(CResult) + sizeof(records) * num_rows * num_cols);
    //  |- CResult*-|   |union RecordData| |num of records (rows) and cols |

    // set number of columns
    c_result->num_cols = num_cols;
    c_result->num_rows = num_rows;
    std::cout << "C-Wrapper: CResult number of columns is : "
              << c_result->num_cols << std::endl;
    std::cout << "C-Wrapper: CResult number of rows is : " << c_result->num_rows
              << std::endl;
    std::cout << "C-Wrapper: populating CResult col_types" << std::endl;
    populate_column_types(c_result, sql_result);
    std::cout << "C-Wrapper: populating CResult *records[]" << std::endl;
    populate_records(c_result, records);
    return c_result;
  }
  std::cout << "C-Wrapper: Result.ok() or result.value().IsQuery() failed"
            << std::endl;
  return NULL;
}

void destroy_select(CResult *c_result) {
  std::cout << "C-Wrapper: starting destroy_select to delete CResult"
            << std::endl;
  free(c_result);
}