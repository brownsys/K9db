#include "pelton/ctx.h"

#include <iostream>

#include "pelton/dataflow/record.h"
#include "pelton/util/shard_name.h"
#include "pelton/util/status.h"

namespace pelton {

ComplianceTransaction::~ComplianceTransaction() {
  if (this->in_ctx_) {
    PANIC(this->Commit());
  }
}

absl::Status ComplianceTransaction::Start() {
  ASSERT_RET(!this->in_ctx_, Internal, "Already in a compliance txn");
  ASSERT_RET(this->db_ != nullptr, Internal, "CTX DB not initalized");
  ASSERT_RET(!this->checkpoint_.has_value(), Internal,
             "CTX has leftover checkpoint before starting");
  this->in_ctx_ = true;
  this->map_.clear();
  return absl::OkStatus();
}

absl::Status ComplianceTransaction::Commit() {
  ASSERT_RET(!this->checkpoint_.has_value(), Internal,
             "CTX has leftover checkpoint before committing");

  // Ensure all records inserted into default shard by this transaction has been
  // removed.
  bool orphans_exist = false;
  this->db_->BeginTransaction(false);
  for (const auto &[table_name, pks] : this->map_) {
    std::vector<sql::KeyPair> pairs;
    pairs.reserve(pks.size());
    for (const sqlast::Value &pk : pks) {
      pairs.emplace_back(util::ShardName(DEFAULT_SHARD, DEFAULT_SHARD),
                         sqlast::Value(pk));
    }

    std::vector<dataflow::Record> orphans =
        this->db_->GetDirect(table_name, pairs);
    if (orphans.size() > 0) {
      std::cout << "Table " << table_name << " has orphans: " << std::endl;
      orphans_exist = true;
      for (const dataflow::Record &record : orphans) {
        std::cout << "    " << record << std::endl;
      }
      std::cout << std::endl;
    }
  }

  this->db_->RollbackTransaction();
  this->in_ctx_ = false;
  this->map_.clear();
  this->checkpoint_ = {};

  ASSERT_RET(!orphans_exist, Internal, "Orphaned data detected");
  return absl::OkStatus();
}

absl::Status ComplianceTransaction::Discard() {
  ASSERT_RET(this->in_ctx_, Internal, "No compliance txn rollback");
  ASSERT_RET(!this->checkpoint_.has_value(), Internal,
             "CTX has leftover checkpoint before rolling back");
  this->in_ctx_ = false;
  this->map_.clear();
  this->checkpoint_ = {};
  return absl::OkStatus();
}

// Checkpoints.
absl::Status ComplianceTransaction::AddCheckpoint() {
  ASSERT_RET(!this->checkpoint_.has_value(), Internal,
             "Already in a checkpoint");
  if (this->in_ctx_) {
    this->checkpoint_ = std::vector<Node>();
  }
  return absl::OkStatus();
}

absl::Status ComplianceTransaction::RollbackCheckpoint() {
  if (this->in_ctx_) {
    ASSERT_RET(this->checkpoint_.has_value(), Internal,
               "No checkpoint to rollback");
  }
  this->checkpoint_ = {};
  return absl::OkStatus();
}
absl::Status ComplianceTransaction::CommitCheckpoint() {
  if (this->in_ctx_) {
    ASSERT_RET(this->checkpoint_.has_value(), Internal,
               "No checkpoint to commit");
    for (auto &[table_name, pks] : this->checkpoint_.value()) {
      auto it = this->map_.find(table_name);
      if (it == this->map_.end()) {
        this->map_.emplace(table_name, std::move(pks));
      } else {
        for (const sqlast::Value &pk : pks) {
          it->second.insert(pk);
        }
      }
    }
  }
  this->checkpoint_ = {};
  return absl::OkStatus();
}

// Track orphaned data.
absl::Status ComplianceTransaction::AddOrphan(const std::string &table_name,
                                              const sqlast::Value &pk) {
  ASSERT_RET(this->in_ctx_, Internal,
             "Orphaned data created outside of compliance transaction!");
  ASSERT_RET(this->checkpoint_.has_value(), Internal, "No checkpoint to add");
  std::vector<Node> &vec = this->checkpoint_.value();
  vec.emplace_back();
  vec.back().first = table_name;
  vec.back().second.insert(pk);
  return absl::OkStatus();
}

absl::Status ComplianceTransaction::AddOrphans(
    const std::string &table_name,
    const std::unordered_set<sqlast::Value> &pks) {
  if (pks.size() > 0) {
    ASSERT_RET(this->in_ctx_, Internal,
               "Orphaned data created outside of compliance transaction!");
    ASSERT_RET(this->checkpoint_.has_value(), Internal, "No checkpoint to add");
    std::vector<Node> &vec = this->checkpoint_.value();
    vec.emplace_back();
    vec.back().first = table_name;
    vec.back().second = pks;
  }
  return absl::OkStatus();
}

}  // namespace pelton
