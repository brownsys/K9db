#ifndef K9DB_POLICY_POLICIES_POLICIES_H_
#define K9DB_POLICY_POLICIES_POLICIES_H_

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "k9db/dataflow/operator_type.h"
#include "k9db/policy/abstract_policy.h"
#include "k9db/sqlast/ast_value.h"

namespace k9db {
namespace policy {
namespace policies {

std::unique_ptr<AbstractPolicy> MakePolicy(const std::string &policy_name,
                                           std::vector<sqlast::Value> &&values);

// AccessControl.
class AccessControl : public AbstractPolicy {
 public:
  explicit AccessControl(std::unordered_set<std::string> &&users)
      : AbstractPolicy(), users_(std::move(users)) {}

  // Factory to create from policy args when reading records from DB.
  static std::unique_ptr<AbstractPolicy> Factory(
      std::vector<sqlast::Value> &&values);

  // For debugging.
  std::string Debug() const override;

  // Combine.
  std::unique_ptr<AbstractPolicy> Combine(
      const std::unique_ptr<AbstractPolicy> &other) const override;

  // Copying.
  std::unique_ptr<AbstractPolicy> Copy() const override {
    return std::make_unique<AccessControl>(std::unordered_set(this->users_));
  }

  // Serializing.
  std::string Name() const override { return "AccessControl"; }
  std::vector<std::string> SerializeValues() const override;

 private:
  std::unordered_set<std::string> users_;
};

// MinK aggregation.
class Aggregate : public AbstractPolicy {
 public:
  Aggregate(size_t k, size_t min_k, std::string &&distinct)
      : AbstractPolicy(), k_(k), min_k_(min_k), distinct_() {
    this->distinct_.insert(std::move(distinct));
  }
  Aggregate(size_t k, size_t min_k, std::unordered_set<std::string> &&dist)
      : AbstractPolicy(), k_(k), min_k_(min_k), distinct_(std::move(dist)) {}

  // Factory to create from policy args when reading records from DB.
  static std::unique_ptr<AbstractPolicy> Factory(
      std::vector<sqlast::Value> &&values);

  // For debugging.
  std::string Debug() const override;

  // Combine.
  std::unique_ptr<AbstractPolicy> Combine(
      const std::unique_ptr<AbstractPolicy> &other) const override;

  // Copying.
  std::unique_ptr<AbstractPolicy> Copy() const override {
    return std::make_unique<Aggregate>(this->k_, this->min_k_,
                                       std::unordered_set(this->distinct_));
  }

  // Serializing.
  std::string Name() const override { return "Aggregate"; }
  std::vector<std::string> SerializeValues() const override;

 private:
  size_t k_;
  size_t min_k_;
  std::unordered_set<std::string> distinct_;
};

// User consent for purpose.
class Consent : public AbstractPolicy {
 public:
  Consent(bool consent, const std::string &purpose)
      : AbstractPolicy(), consent_(consent), purpose_(purpose) {}

  Consent(bool consent, std::string &&purpose)
      : AbstractPolicy(), consent_(consent), purpose_(std::move(purpose)) {}

  // Factory to create from policy args when reading records from DB.
  static std::unique_ptr<AbstractPolicy> Factory(
      std::vector<sqlast::Value> &&values);

  // For debugging.
  std::string Debug() const override;

  // Combine.
  std::unique_ptr<AbstractPolicy> Combine(
      const std::unique_ptr<AbstractPolicy> &other) const override;

  // Copying.
  std::unique_ptr<AbstractPolicy> Copy() const override {
    return std::make_unique<Consent>(this->consent_, this->purpose_);
  }

  // Serializing.
  std::string Name() const override { return "Consent"; }
  std::vector<std::string> SerializeValues() const override;

 private:
  bool consent_;
  std::string purpose_;
};

// Disallows all aggregation.
class NoAggregate : public AbstractPolicy {
 public:
  explicit NoAggregate(bool ok) : AbstractPolicy(), ok_(ok) {}

  // Factory to create from policy args when reading records from DB.
  static std::unique_ptr<AbstractPolicy> Factory(
      std::vector<sqlast::Value> &&values);

  // For debugging.
  std::string Debug() const override;

  // Combine.
  std::unique_ptr<AbstractPolicy> Combine(
      const std::unique_ptr<AbstractPolicy> &other) const override;

  // Disallow aggregation.
  bool Allows(dataflow::OperatorType type) const override {
    return type != dataflow::OperatorType::AGGREGATE;
  }

  // Copying.
  std::unique_ptr<AbstractPolicy> Copy() const override {
    return std::make_unique<NoAggregate>(this->ok_);
  }

  // Serializing.
  std::string Name() const override { return "NoAggregate"; }
  std::vector<std::string> SerializeValues() const override;

 private:
  bool ok_;
};

class And : public AbstractPolicy {
 public:
  explicit And(std::vector<std::unique_ptr<AbstractPolicy>> &&policies)
      : policies_(std::move(policies)) {}

  // For debugging.
  std::string Debug() const override;

  // Combine.
  std::unique_ptr<AbstractPolicy> Combine(
      const std::unique_ptr<AbstractPolicy> &other) const override;

  // Disallow aggregation for member policies.
  bool Allows(dataflow::OperatorType type) const override {
    for (const auto &policy : this->policies_) {
      if (!policy->Allows(type)) {
        return false;
      }
    }
    return true;
  }

  // Copying.
  std::unique_ptr<AbstractPolicy> Copy() const override {
    std::vector<std::unique_ptr<AbstractPolicy>> policies;
    policies.reserve(this->policies_.size());
    for (const auto &policy : this->policies_) {
      policies.push_back(policy->Copy());
    }
    return std::make_unique<And>(std::move(policies));
  }

  // Serializing.
  std::string Name() const override { return "And"; }
  std::vector<std::string> ColumnNames(const std::string &col) const override;
  std::vector<std::string> SerializeValues() const override;

 private:
  std::vector<std::unique_ptr<AbstractPolicy>> policies_;
};

class Or : public AbstractPolicy {
 public:
  explicit Or(std::vector<std::unique_ptr<AbstractPolicy>> &&policies)
      : policies_(std::move(policies)) {}

  // For debugging.
  std::string Debug() const override;

  // Combine.
  std::unique_ptr<AbstractPolicy> Combine(
      const std::unique_ptr<AbstractPolicy> &other) const override;

  // Disallow aggregation for member policies.
  bool Allows(dataflow::OperatorType type) const override {
    for (const auto &policy : this->policies_) {
      if (policy->Allows(type)) {
        return true;
      }
    }
    return false;
  }

  // Copying.
  std::unique_ptr<AbstractPolicy> Copy() const override {
    std::vector<std::unique_ptr<AbstractPolicy>> policies;
    policies.reserve(this->policies_.size());
    for (const auto &policy : this->policies_) {
      policies.push_back(policy->Copy());
    }
    return std::make_unique<Or>(std::move(policies));
  }

  // Serializing.
  std::string Name() const override { return "Or"; }
  std::vector<std::string> ColumnNames(const std::string &col) const override;
  std::vector<std::string> SerializeValues() const override;

 private:
  std::vector<std::unique_ptr<AbstractPolicy>> policies_;
};

}  // namespace policies
}  // namespace policy
}  // namespace k9db

#endif  // K9DB_POLICY_POLICIES_POLICIES_H_
