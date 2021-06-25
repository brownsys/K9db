#include "open.h"

int main(int argc, char **argv)
{
	ConnectionC_t *c_conn = create();
    open_c("query", "db_username", "db_password", c_conn);
	destroy(c_conn);
	return 0;
}