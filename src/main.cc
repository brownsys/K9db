#include <iostream>
#include <string>
#include <vector>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

ABSL_FLAG(bool, verbose, true, "Verbose output");

int main(int argc, char* argv[]) {
  absl::ParseCommandLine(argc, argv);

  std::cout << "Hello world!" << std::endl;

  return 0;
}
