#ifndef PELTON_SQLAST_HACKY_UTIL_H_
#define PELTON_SQLAST_HACKY_UTIL_H_

#include <string>

namespace pelton {
namespace sqlast {

inline bool StartsWith(const char **ptr, size_t *sz1, const char *s2,
                       size_t sz2) {
  if (*sz1 < sz2) {
    return false;
  }

  const char *s1 = *ptr;
  size_t i;
  for (i = 0; i < sz2; i++) {
    if (s1[i] != s2[i] && s1[i] != (s2[i] + 32)) {
      return false;
    }
  }

  *ptr += i;
  *sz1 -= i;
  return true;
}

inline void ConsumeWhiteSpace(const char **ptr, size_t *size) {
  const char *s = *ptr;
  size_t i;
  for (i = 0; i < *size; i++) {
    if (s[i] != ' ') {
      break;
    }
  }
  *ptr += i;
  *size -= i;
}

inline std::string ExtractIdentifier(const char **ptr, size_t *size) {
  const char *s = *ptr;
  size_t i;
  for (i = 0; i < *size; i++) {
    if (s[i] == ' ') {
      break;
    }
  }

  if (i > *size) {
    return "";
  }

  std::string idn(s, i);
  *ptr += i;
  *size -= i;
  return idn;
}

inline std::string ExtractValue(const char **ptr, size_t *size) {
  ConsumeWhiteSpace(ptr, size);
  if (size == 0) {
    return "";
  }

  const char *str = *ptr;
  bool squote = str[0] == '\'';
  bool dquote = str[0] == '"';
  bool escape = false;
  size_t i;
  for (i = squote || dquote ? 1 : 0; i < *size; i++) {
    if (escape) {
      escape = false;
      continue;
    }
    if (str[i] == '\\') {
      escape = true;
      continue;
    }
    if (squote && str[i] == '\'') {
      i++;
      break;
    } else if (dquote && str[i] == '"') {
      break;
      i++;
    } else if (!squote && !dquote && (str[i] < '0' || str[i] > '9')) {
      break;
    }
  }

  if (i > *size) {
    return "";
  }

  std::string idn(str, i);
  *ptr += i;
  *size -= i;
  return idn;
}

}  // namespace sqlast
}  // namespace pelton

#endif  // PELTON_SQLAST_HACKY_UTIL_H_
