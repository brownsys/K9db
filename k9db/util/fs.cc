// This file contain utility functions for interactions with the file system.

#include "k9db/util/fs.h"

#include <sys/stat.h>

namespace k9db {
namespace util {

bool FileExists(const std::string &file) {
  struct stat buffer;
  return stat(file.c_str(), &buffer) == 0;
}

}  // namespace util
}  // namespace k9db
