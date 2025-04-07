#include "k9db/policy/policies/policies.h"

#include "absl/strings/str_join.h"
#include "glog/logging.h"

namespace k9db {
namespace policy {
namespace policies {

std::unique_ptr<AbstractPolicy> MakePolicy(
    const std::string &policy_name, std::vector<sqlast::Value> &&values) {
  if (policy_name == "AccessControl") {
    return AccessControl::Factory(std::move(values));
  } else if (policy_name == "Aggregate") {
    return Aggregate::Factory(std::move(values));
  } else if (policy_name == "Consent") {
    return Consent::Factory(std::move(values));
  } else if (policy_name == "NoAggregate") {
    return NoAggregate::Factory(std::move(values));
  }

  LOG(FATAL) << "Unknown policy name '" << policy_name << "'";
}

// Factories.
std::unique_ptr<AbstractPolicy> AccessControl::Factory(
    std::vector<sqlast::Value> &&values) {
  std::unordered_set<std::string> users;
  for (const auto &value : values) {
    users.insert(value.AsUnquotedString());
  }
  return std::make_unique<AccessControl>(std::move(users));
}

std::unique_ptr<AbstractPolicy> Aggregate::Factory(
    std::vector<sqlast::Value> &&values) {
  return std::make_unique<Aggregate>(values[0].GetInt(), values[1].GetInt());
}

std::unique_ptr<AbstractPolicy> Consent::Factory(
    std::vector<sqlast::Value> &&values) {
  bool consent = values[0].GetInt() >= 1;
  const std::string &purpose = values[1].GetString();
  return std::make_unique<Consent>(consent, purpose);
}

std::unique_ptr<AbstractPolicy> NoAggregate::Factory(
    std::vector<sqlast::Value> &&values) {
  return std::make_unique<NoAggregate>(true);
}

// Serializing via MySQL protocol.

// AccessControl.
std::vector<std::string> AccessControl::SerializeValues() const {
  return {absl::StrJoin(this->users_, ";")};
}

// Aggregate.
std::vector<std::string> Aggregate::SerializeValues() const {
  return {std::to_string(this->k_) + ";" + std::to_string(this->min_k_)};
}

// Consent.
std::vector<std::string> Consent::SerializeValues() const {
  return {std::to_string(this->consent_ ? 1 : 0) + ";" + this->purpose_};
}

// NoAggregate.
std::vector<std::string> NoAggregate::SerializeValues() const {
  return {std::to_string(this->ok_ ? 1 : 0)};
}

// And.
std::vector<std::string> And::ColumnNames(const std::string &col) const {
  std::vector<std::string> result;
  result.reserve(this->policies_.size());
  for (const auto &policy : this->policies_) {
    for (auto &c : policy->ColumnNames(col)) {
      result.push_back(std::move(c));
    }
  }
  CHECK_GT(result.size(), 1);
  result[0][2] = '&';
  result[result.size() - 1][2] = ')';
  return result;
}
std::vector<std::string> And::SerializeValues() const {
  std::vector<std::string> result;
  result.reserve(this->policies_.size());
  for (auto &policy : this->policies_) {
    for (auto &c : policy->SerializeValues()) {
      result.push_back(std::move(c));
    }
  }
  return result;
}

// Or.
std::vector<std::string> Or::ColumnNames(const std::string &col) const {
  std::vector<std::string> result;
  result.reserve(this->policies_.size());
  for (const auto &policy : this->policies_) {
    for (auto &c : policy->ColumnNames(col)) {
      result.push_back(std::move(c));
    }
  }
  CHECK_GT(result.size(), 1);
  result[0][2] = '|';
  result[result.size() - 1][2] = ')';
  return result;
}
std::vector<std::string> Or::SerializeValues() const {
  std::vector<std::string> result;
  result.reserve(this->policies_.size());
  for (auto &policy : this->policies_) {
    for (auto &c : policy->SerializeValues()) {
      result.push_back(std::move(c));
    }
  }
  return result;
}

}  // namespace policies
}  // namespace policy
}  // namespace k9db
