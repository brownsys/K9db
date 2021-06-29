#include "open.h"

int main(int argc, char **argv)
{
	ConnectionC c_conn = open_c("", "root", "password");
	bool close = close_c(c_conn);
	destroy(c_conn.cpp_conn);
	return 0;
}
