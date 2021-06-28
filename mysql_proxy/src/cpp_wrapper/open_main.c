#include "open.h"

int main(int argc, char **argv)
{
	ConnectionC c_conn = open_c("", "root", "password");
	destroy(c_conn.cpp_conn);
	return 0;
}
