#include "open.h"

int main(int argc, char **argv)
{
	// pointer to c_conn is stored on the stack of this function, so not valid
	// outside
	ConnectionC c_conn = open_c("", "root", "password");
	ConnectionC response_ddl = exec_c(c_conn, "CREATE TABLE test3 (my_column int);");
	ConnectionC response_insert = exec_c(c_conn, "INSERT INTO test3 VALUES (1);");
	ConnectionC response_update = exec_c(c_conn, "UPDATE test3 SET my_column=2 WHERE my_column=1;");
	ConnectionC response_delete = exec_c(c_conn, "DELETE FROM test3 WHERE my_column=2;");
	ConnectionC close = close_c(response_delete);
	destroy(close.cpp_conn);
	return 0;
}
