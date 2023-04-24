#ifndef K9DB_CTX_H_
#define K9DB_CTX_H_

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "k9db/sql/connection.h"
#include "k9db/sqlast/ast.h"

namespace k9db {

class ComplianceTransaction {
 public:
  explicit ComplianceTransaction(sql::Session *db)
      : db_(db), in_ctx_(false), map_(), checkpoint_() {}
  ~ComplianceTransaction();

  absl::Status Start();
  absl::Status Commit();
  absl::Status Discard();

  // Checkpoints.
  absl::Status AddCheckpoint();
  absl::Status RollbackCheckpoint();
  absl::Status CommitCheckpoint();

  // Track orphaned data.
  absl::Status AddOrphan(const std::string &table_name,
                         const sqlast::Value &pk);
  absl::Status AddOrphans(const std::string &table_name,
                          const std::unordered_set<sqlast::Value> &pks);

  // TODO(babman): optimization: allow client code to also untrack orphans
  //               it moved out of default shard.

 private:
  sql::Session *db_;
  bool in_ctx_;
  std::unordered_map<std::string, std::unordered_set<sqlast::Value>> map_;

  // Stage checkpoint here.
  using Node = std::pair<std::string, std::unordered_set<sqlast::Value>>;
  std::optional<std::vector<Node>> checkpoint_;
};

}  // namespace k9db

#endif  // K9DB_CTX_H_
