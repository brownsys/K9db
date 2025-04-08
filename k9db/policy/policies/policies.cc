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
  return std::make_unique<Aggregate>(values[0].GetInt(), values[1].GetInt(),
                                     values[2].AsUnquotedString());
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

// Debugging.
std::string AccessControl::Debug() const {
  return "AccessControl(" + absl::StrJoin(this->users_, ";") + ")";
}
std::string Aggregate::Debug() const {
  return "Aggregate(" + std::to_string(this->k_) + ";" +
         std::to_string(this->min_k_) + ";" +
         absl::StrJoin(this->distinct_, ";") + ")";
}
std::string Consent::Debug() const {
  return "Consent(" + std::to_string(this->consent_) + ";" + this->purpose_ +
         ")";
}
std::string NoAggregate::Debug() const { return "NoAggregate()"; }
std::string And::Debug() const {
  std::vector<std::string> debugs;
  for (const auto &p : this->policies_) {
    debugs.push_back(p->Debug());
  }
  return absl::StrJoin(debugs, " && ");
}
std::string Or::Debug() const {
  std::vector<std::string> debugs;
  for (const auto &p : this->policies_) {
    debugs.push_back(p->Debug());
  }
  return absl::StrJoin(debugs, " || ");
}

// Policy combination.
std::unique_ptr<AbstractPolicy> AccessControl::Combine(
    const std::unique_ptr<AbstractPolicy> &other) const {
  if (other == nullptr) return this->Copy();

  CHECK_EQ(this->Name(), other->Name());
  const AccessControl *o = reinterpret_cast<const AccessControl *>(other.get());

  std::unordered_set<std::string> acl;
  for (const std::string &user : this->users_) {
    if (o->users_.contains(user)) {
      acl.insert(user);
    }
  }
  return std::make_unique<AccessControl>(std::move(acl));
}

std::unique_ptr<AbstractPolicy> Aggregate::Combine(
    const std::unique_ptr<AbstractPolicy> &other) const {
  if (other == nullptr) return this->Copy();

  CHECK_EQ(this->Name(), other->Name());
  const Aggregate *o = reinterpret_cast<const Aggregate *>(other.get());

  size_t k = this->k_;
  CHECK_EQ(this->min_k_, o->min_k_);
  std::unordered_set<std::string> distinct = this->distinct_;
  for (const std::string &ostr : o->distinct_) {
    if (!distinct.contains(ostr)) {
      distinct.insert(ostr);
      k++;
    }
  }
  return std::make_unique<Aggregate>(k, this->min_k_, std::move(distinct));
}

std::unique_ptr<AbstractPolicy> Consent::Combine(
    const std::unique_ptr<AbstractPolicy> &other) const {
  if (other == nullptr) return this->Copy();

  CHECK_EQ(this->Name(), other->Name());
  const Consent *o = reinterpret_cast<const Consent *>(other.get());

  bool consent = this->consent_ && o->consent_;
  CHECK_EQ(this->purpose_, o->purpose_);
  return std::make_unique<Consent>(consent, this->purpose_);
}

std::unique_ptr<AbstractPolicy> NoAggregate::Combine(
    const std::unique_ptr<AbstractPolicy> &other) const {
  if (other == nullptr) return this->Copy();

  CHECK_EQ(this->Name(), other->Name());
  const NoAggregate *o = reinterpret_cast<const NoAggregate *>(other.get());

  return std::make_unique<NoAggregate>(this->ok_ && o->ok_);
}

std::unique_ptr<AbstractPolicy> And::Combine(
    const std::unique_ptr<AbstractPolicy> &other) const {
  if (other == nullptr) return this->Copy();

  CHECK_EQ(this->Name(), other->Name());
  const And *o = reinterpret_cast<const And *>(other.get());

  std::vector<std::unique_ptr<AbstractPolicy>> policies;
  policies.reserve(this->policies_.size());
  CHECK_EQ(this->policies_.size(), o->policies_.size());
  for (size_t i = 0; i < this->policies_.size(); i++) {
    policies.push_back(this->policies_.at(i)->Combine(o->policies_.at(i)));
  }
  return std::make_unique<And>(std::move(policies));
}

std::unique_ptr<AbstractPolicy> Or::Combine(
    const std::unique_ptr<AbstractPolicy> &other) const {
  if (other == nullptr) return this->Copy();

  CHECK_EQ(this->Name(), other->Name());
  const Or *o = reinterpret_cast<const Or *>(other.get());

  std::vector<std::unique_ptr<AbstractPolicy>> policies;
  policies.reserve(this->policies_.size());
  CHECK_EQ(this->policies_.size(), o->policies_.size());
  for (size_t i = 0; i < this->policies_.size(); i++) {
    policies.push_back(this->policies_.at(i)->Combine(o->policies_.at(i)));
  }
  return std::make_unique<Or>(std::move(policies));
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
