#include <stdlib.h>
#include <cstdio>
#include "simple.h"
#include "../cpp/cppsimple.h"

int query_pelton_c(int query)
{
    printf("Query received from rust proxy is: %d\n", query);
    fflush(stdout);
    // convert to C++ types, then call C++ function
    int response = query_pelton(query);
    return response;
}