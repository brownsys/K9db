#ifndef K9DB_SQLAST_HACKY_UTIL_H_
#define K9DB_SQLAST_HACKY_UTIL_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace k9db {
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
  size_t pos = 0;
  size_t i;
  for (i = 0; i < *size; i++) {
    if (s[i] == ' ' || s[i] == '>' || s[i] == '=' || s[i] == ';' ||
        s[i] == ')' || s[i] == '(' || s[i] == ',') {
      break;
    }
    if (s[i] == '.') {
      pos = i + 1;
    }
  }

  if (i > *size) {
    return "";
  }

  std::string idn(s + pos, i - pos);
  *ptr += i;
  *size -= i;
  return idn;
}

inline std::string ExtractValue(const char **ptr, size_t *size,
                                const std::vector<std::string> &args) {
  ConsumeWhiteSpace(ptr, size);
  if (size == 0) {
    return "";
  }
  if (StartsWith(ptr, size, "NULL", 4)) {
    return "NULL";
  }

  // bind parameter.
  if (StartsWith(ptr, size, "$_", 2)) {
    size_t i = 0;
    const char *str = *ptr;
    for (; str[i] >= '0' && str[i] <= '9'; i++) {
    }
    size_t arg_index = std::stoi(std::string(str, i));
    *ptr += i;
    *size -= i;
    return args.at(arg_index);
  }

  const char *str = *ptr;
  bool squote = str[0] == '\'';
  bool dquote = str[0] == '"';
  bool quote = squote || dquote;
  bool escape = false;
  size_t i;
  for (i = quote ? 1 : 0; i < *size; i++) {
    if (escape) {
      escape = false;
      continue;
    }
    if (quote && str[i] == '\\') {
      escape = true;
      continue;
    }
    if (squote && str[i] == '\'') {
      i++;
      break;
    } else if (dquote && str[i] == '"') {
      i++;
      break;
    } else if (!quote && !(str[i] >= '0' && str[i] <= '9') && str[i] != '?') {
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

inline bool EqualIgnoreCase(const std::string &str1, const std::string &upper) {
  if (str1.size() != upper.size()) {
    return false;
  }
  for (size_t i = 0; i < str1.size(); i++) {
    if (str1[i] != upper[i] && str1[i] != (upper[i] + 32)) {
      return false;
    }
  }
  return true;
}

std::unique_ptr<BinaryExpression> HackyCondition(
    const char **ptr, size_t *size, const std::vector<std::string> &args) {
  std::vector<std::unique_ptr<BinaryExpression>> conditions;

  bool has_and = false;
  do {
    // <column_name>
    std::string column_name = ExtractIdentifier(ptr, size);
    if (column_name.size() == 0) {
      return nullptr;
    }
    ConsumeWhiteSpace(ptr, size);
    std::unique_ptr<Expression> left =
        std::make_unique<ColumnExpression>(column_name);

    // =, >, or IN.
    bool in = StartsWith(ptr, size, "IN", 2);
    bool eq = StartsWith(ptr, size, "=", 1);
    bool gt = StartsWith(ptr, size, ">", 1);
    bool is = StartsWith(ptr, size, "IS", 2);
    if (!eq && !gt && !in && !is) {
      return nullptr;
    }
    ConsumeWhiteSpace(ptr, size);

    // Value.
    std::unique_ptr<Expression> right;
    if (gt) {
      std::string val = ExtractValue(ptr, size, args);
      if (val.size() == 0) {
        return nullptr;
      }
      right = std::make_unique<LiteralExpression>(Value::FromSQLString(val));
    } else if (eq || is) {
      // value or column
      std::string val = ExtractValue(ptr, size, args);
      if (val.size() > 0) {
        right = std::make_unique<LiteralExpression>(Value::FromSQLString(val));
      } else {
        std::string column = ExtractIdentifier(ptr, size);
        if (column.size() == 0) {
          return nullptr;
        }
        right = std::make_unique<ColumnExpression>(column);
      }
    } else {
      // in
      if (!StartsWith(ptr, size, "(", 1)) {
        return nullptr;
      }
      ConsumeWhiteSpace(ptr, size);

      std::vector<Value> values;
      while (true) {
        std::string val = ExtractValue(ptr, size, args);
        if (val.size() == 0) {
          break;
        }
        values.push_back(Value::FromSQLString(val));
        ConsumeWhiteSpace(ptr, size);
        if (!StartsWith(ptr, size, ",", 1)) {
          break;
        }
        ConsumeWhiteSpace(ptr, size);
      }

      ConsumeWhiteSpace(ptr, size);
      if (!StartsWith(ptr, size, ")", 1)) {
        return nullptr;
      }
      right = std::make_unique<LiteralListExpression>(values);
    }

    std::unique_ptr<BinaryExpression> binary;
    if (eq) {
      binary = std::make_unique<BinaryExpression>(Expression::Type::EQ);
    } else if (gt) {
      binary =
          std::make_unique<BinaryExpression>(Expression::Type::GREATER_THAN);
    } else if (in) {
      binary = std::make_unique<BinaryExpression>(Expression::Type::IN);
    } else if (is) {
      binary = std::make_unique<BinaryExpression>(Expression::Type::IS);
    }
    binary->SetLeft(std::move(left));
    binary->SetRight(std::move(right));
    conditions.push_back(std::move(binary));

    ConsumeWhiteSpace(ptr, size);
    if (StartsWith(ptr, size, "AND", 3)) {
      has_and = true;
      ConsumeWhiteSpace(ptr, size);
    } else {
      has_and = false;
    }
  } while (has_and);

  if (conditions.size() == 1) {
    return std::move(conditions.front());
  }

  // Combine ands.
  std::unique_ptr<BinaryExpression> left = std::move(conditions.front());
  for (size_t i = 1; i < conditions.size(); i++) {
    std::unique_ptr<BinaryExpression> expr =
        std::make_unique<BinaryExpression>(Expression::Type::AND);
    expr->SetLeft(std::move(left));
    expr->SetRight(std::move(conditions.at(i)));
    left = std::move(expr);
  }
  return left;
}

}  // namespace sqlast
}  // namespace k9db

#endif  // K9DB_SQLAST_HACKY_UTIL_H_
