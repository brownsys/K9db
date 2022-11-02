#ifndef PELTON_SQLAST_AST_VALUE_H_
#define PELTON_SQLAST_AST_VALUE_H_

#include <cassert>
#include <ostream>
#include <string>
#include <utility>
// NOLINTNEXTLINE
#include <variant>

#include "pelton/sqlast/ast_schema_enums.h"

namespace pelton {
namespace sqlast {

class Value {
 public:
  enum Type { _NULL = 0, UINT = 1, INT = 2, TEXT = 3 };

  // Actual value constructors.
  Value() : data_(static_cast<void *>(nullptr)) {}
  explicit Value(uint64_t v) : data_(v) {}
  explicit Value(int64_t v) : data_(v) {}
  explicit Value(const std::string &v) : data_(v) {}
  explicit Value(std::string &&v) : data_(std::move(v)) {}

  // Parse from SQL string.
  static Value FromSQLString(const std::string &str);

  // Comparisons.
  bool operator==(const Value &other) const = default;
  bool operator<(const Value &other) const;

  // Hash to use as a key in absl hash tables.
  template <typename H>
  friend H AbslHashValue(H h, const Value &k) {
    switch (k.data_.index()) {
      case Type::_NULL:
        return H::combine(std::move(h), "[NULL]");
      case Type::UINT:
        return H::combine(std::move(h), std::get<uint64_t>(k.data_));
      case Type::INT:
        return H::combine(std::move(h), std::get<int64_t>(k.data_));
      case Type::TEXT: {
        const std::string &str = std::get<std::string>(k.data_);
        return H::combine_contiguous(std::move(h), str.data(), str.size());
      }
      default:
        assert(false);
    }
    return h;
  }

  // Deterministic hash.
  size_t Hash() const;

  // Data access.
  uint64_t GetUInt() const;
  int64_t GetInt() const;
  const std::string &GetString() const;
  bool IsNull() const { return this->data_.index() == 0; }

  std::string AsUnquotedString() const;
  std::string AsSQLString() const;

  // Access the underlying type.
  Type type() const { return static_cast<Type>(this->data_.index()); }

  // Type conversions.
  bool TypeCompatible(ColumnDefinitionTypeEnum type) const;
  ColumnDefinitionTypeEnum ConvertType() const;

  // For logging and printing...
  friend std::ostream &operator<<(std::ostream &os, const Value &k);

  // The size in memory.
  size_t SizeInMemory() const;

 private:
  std::variant<void *, uint64_t, int64_t, std::string> data_;
};

// Printing a record to an output stream (e.g. std::cout).
std::ostream &operator<<(std::ostream &os, const pelton::sqlast::Value &v);

}  // namespace sqlast
}  // namespace pelton

#endif  // PELTON_SQLAST_AST_VALUE_H_
