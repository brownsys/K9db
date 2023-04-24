// This file contain utility functions for interactions with the file system.

#ifndef K9DB_UTIL_FS_H_
#define K9DB_UTIL_FS_H_

#include <fstream>
#include <string>

namespace k9db {
namespace util {

bool FileExists(const std::string &file);

inline void OpenRead(std::ifstream *file, const std::string &path) {
  file->open(path, std::ios::in);
}

inline void OpenWrite(std::ofstream *file, const std::string &path) {
  file->open(path, std::ios::out | std::ios::trunc);
}

}  // namespace util
}  // namespace k9db

#endif  // K9DB_UTIL_FS_H_
