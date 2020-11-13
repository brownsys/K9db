#include <iostream>
#include <string>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"

DEFINE_bool(verbose, false, "Verbose output");

int main(int argc, char* argv[]) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_verbose) {
    LOG(INFO) << "Hello world log";
    std::cout << "Hello world!" << std::endl;
  }

  // ServeGraph();

  return 0;
}
