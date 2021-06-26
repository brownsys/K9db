#include "open.h"

int main(int argc, char **argv)
{
	ConnectionC c_conn = create();
	open_c("", "root", "password", c_conn);
	destroy(c_conn);
	return 0;
}
