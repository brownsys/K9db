#include "open.h"

int main(int argc, char **argv)
{
	// pointer to c_conn is stored on the stack of this function, so not valid
	// outside
	ConnectionC c_conn = open_c("", "root", "password");
	bool close = close_c(c_conn);
	// destroy(c_conn.cpp_conn);
	return 0;
}
