// This file contain utility functions for interactions with the file system.

#include "pelton/util/fs.h"

#include <sys/stat.h>

namespace pelton {
namespace util {

bool FileExists(const std::string &file) {
  struct stat buffer;
  return stat(file.c_str(), &buffer) == 0;
}

}  // namespace util
}  // namespace pelton
