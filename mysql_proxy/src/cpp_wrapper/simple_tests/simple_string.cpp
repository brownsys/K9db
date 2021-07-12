#include <stdio.h>
#include <string.h>
#include "simple_string.h"
#include "../cpp/simple_cppstring.h"

char* query_pelton_c(char* query)
{
    printf("C-Wrapper: query received from rust proxy is: %s\n", query);
    std::string cpp_query(query);
    std::string cpp_response = query_pelton(cpp_query);

    // create heap allocated buffer for size of response string + 1 for terminator
    char * c_response = (char*)malloc(cpp_query.size() + 1);
    strcpy(c_response, cpp_response.c_str());
    return c_response;
}