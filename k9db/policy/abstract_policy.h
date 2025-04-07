#ifndef K9DB_POLICY_ABSTRACT_POLICY_H_
#define K9DB_POLICY_ABSTRACT_POLICY_H_

#include <memory>
#include <string>
#include <vector>

#include "k9db/dataflow/operator_type.h"

namespace k9db {
namespace policy {

// Policy state (per table).
class AbstractPolicy {
 public:
  // Constructor and destructor.
  AbstractPolicy() {}
  virtual ~AbstractPolicy() {}

  // Virtual functions.
  virtual std::string Name() const = 0;
  virtual bool Check() const;
  virtual std::unique_ptr<AbstractPolicy> Combine(AbstractPolicy *other);
  virtual bool Allows(dataflow::OperatorType type) const;

  // Copying.
  virtual std::unique_ptr<AbstractPolicy> Copy() const = 0;

  // Serializing.
  virtual std::vector<std::string> ColumnNames(const std::string &col) const {
    return {std::string("$_P__" + col + "__" + this->Name())};
  }

  virtual std::vector<std::string> SerializeValues() const = 0;
};

}  // namespace policy
}  // namespace k9db

#endif  // K9DB_POLICY_ABSTRACT_POLICY_H_
