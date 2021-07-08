#include "open.h"

int main(int argc, char **argv)
{
	// pointer to c_conn is stored on the stack of this function, so not valid
	// outside
	ConnectionC c_conn = open_c("", "root", "password");
	bool response_ddl = exec_ddl(&c_conn, "CREATE TABLE test3 (my_column int);");
	int response_insert = exec_update(&c_conn, "INSERT INTO test3 VALUES (1);");
	// int response_update = exec_update(&c_conn, "UPDATE test3 SET my_column=2 WHERE my_column=1;");
	// int response_delete = exec_update(&c_conn, "DELETE FROM test3 WHERE my_column=2;");
	CResult* response_select = exec_select(&c_conn, "SELECT * FROM test3;");
	destroy_select(response_select);
	bool close = close_c(&c_conn);
	destroy_conn(c_conn.cpp_conn);
	return 0;
}
