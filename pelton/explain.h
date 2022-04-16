// EXPLAIN PRIVACY command.
#ifndef PELTON_EXPLAIN_H_
#define PELTON_EXPLAIN_H_

#include <iomanip>
#include <iostream>
#include <vector>

#include "glog/logging.h"
#include "pelton/shards/state.h"
#include "pelton/shards/types.h"

namespace pelton {
namespace explain {

void PrintTransitivityChain(
    std::ostream &out, const shards::ShardingInformation &info,
    const shards::SharderState &state, int indent,
    std::vector<const shards::UnshardedTableName *> *varshards) {
  const auto &transitive_infos =
      state.GetShardingInformationFor(info.next_table, info.shard_kind);
  const shards::ShardingInformation *tinfo_ptr = nullptr;
  if (transitive_infos.size() == 0) {
    for (const auto &info : state.GetShardingInformation(info.next_table)) {
      std::cout << info << std::endl;
    }
    LOG(FATAL) << (transitive_infos.size() == 0 ? "No" : "Too many")
               << " next shardings for table: " << info.next_table
               << ", shard kind: " << info.shard_kind
               << ", column: " << info.next_column << " found.";
  } else if (transitive_infos.size() == 1) {
    tinfo_ptr = transitive_infos.front();
  } else {
    bool duplicate_found = false;
    for (const auto *tinfo : transitive_infos) {
      if (tinfo->distance_from_shard == info.distance_from_shard - 1) {
        duplicate_found = tinfo_ptr != nullptr;
        tinfo_ptr = tinfo;
      }
    }
    if (tinfo_ptr == nullptr) {
      LOG(FATAL) << "Tried to disambiguate multiple candidate shardings for "
                 << info.next_table
                 << " using distance from shard but did not find a match, this "
                    "is concerning!";
    } else if (duplicate_found) {
      LOG(WARNING)
          << "Tried to disambiguate mutliple candidate shardings for "
          << info.next_table
          << " using distance from shard but found more than one match. The "
             "following information may therefore be incorrect!";
    }
  }

  const auto &tinfo = *tinfo_ptr;
  if (tinfo.is_varowned()) varshards->push_back(&tinfo.next_table);
  out << std::setw(indent) << ""
      << "via " << info.next_table << "(" << tinfo.shard_by
      << ") resolved with index " << info.next_index_name << std::endl;

  if (tinfo.IsTransitive())
    PrintTransitivityChain(out, tinfo, state, indent, varshards);
}

void WarningsFromSchema(std::ostream &out, const sqlast::CreateTable &schema) {
  
}

using ShardingChainInfo = 
    std::vector<std::vector<const shards::UnshardedTableName *>>;

ShardingChainInfo GetAndReportShardingInformation(std::ostream &out, const shards::UnshardedTableName table_name, const std::list<shards::ShardingInformation> &sharding_infos, const shards::SharderState &state) {
  ShardingChainInfo varown_chains {};
  for (const auto &info : sharding_infos) {
    std::vector<const shards::UnshardedTableName *> varown_chain{};
    if (info.is_varowned()) varown_chain.push_back(&info.next_table);
    out << "  " << info.shard_by << " shards to " << info.shard_kind
        << std::endl;
    if (info.IsTransitive()) {
      PrintTransitivityChain(out, info, state, 4, &varown_chain);
      out << "    total transitive distance is " << info.distance_from_shard
          << std::endl;
    }
    if (varown_chain.size() > 0) varown_chains.push_back(varown_chain);
  }
  return varown_chains;
}

void ClassifyAndWarnAboutSharding(std::ostream &out, const ShardingChainInfo &varown_chains, const std::list<shards::ShardingInformation> &sharding_infos) {
  const std::vector<const shards::UnshardedTableName *> *longest_varown_chain =
      nullptr;

  for (auto &varown_chain : varown_chains) {
    if (!longest_varown_chain ||
        longest_varown_chain->size() < varown_chain.size())
      longest_varown_chain = &varown_chain;
  }

  int regular_shardings = sharding_infos.size() - varown_chains.size();
  int longest_varown_chain_size =
      longest_varown_chain ? longest_varown_chain->size() : 0;

  // Feedback section

  if (longest_varown_chain_size > 1) {
    out << "  [SEVERE] This table is variably sharded "
        << longest_varown_chain->size()
        << " times in sequence. This will create ";
    bool put_sym = false;
    for (const auto &varown_table : *longest_varown_chain) {
      if (put_sym)
        out << '*';
      else
        put_sym = true;
      out << *varown_table;
    }
    out << " copies of records inserted into this table. This is likely not "
            "desired behavior, I suggest checking your `OWNS` annotations."
        << std::endl;
  }
  if (varown_chains.size() > 1) {
    out << "  [Warning] This table is variably owned in multiple ways (via ";
    bool put_sym = false;
    for (const auto &varown_tables : varown_chains) {
      if (put_sym)
        out << " and ";
      else
        put_sym = true;
      out << *varown_tables.front();
    }
    out << "). This may not be desired behavior." << std::endl;
  } else if (varown_chains.size() == 1) {
    out << "  [Info] this table is variably owned (via "
        << *varown_chains.front().front() << ")" << std::endl;
  }
  if (longest_varown_chain_size == 1 && regular_shardings > 1) {
    out << "  [Warning] This table is variably owned and also copied an "
            "additional "
        << regular_shardings << " times." << std::endl;
  } else if (regular_shardings > 5) {
    out << "  [Warning] This table is sharded " << regular_shardings
        << " times. This seem excessive, you may want to check your `OWNER` "
            "annotations."
        << std::endl;
  } else if (regular_shardings > 2) {
    out << "  [Info] This table is copied " << regular_shardings << " times"
        << std::endl;
  }
}

void ExplainPrivacy(const Connection &connection) {
  const shards::SharderState &state = *connection.state->sharder_state();
  auto &out = std::cout;

  out << std::endl;
  out << std::endl;
  out << "############# EXPLAIN PRIVACY #############" << std::endl;

  // explain accessors

  // eventually maybe number of active shards

  for (const auto &[table_name, schema] : state.tables()) {
    // Eventually also include if there are sharded tables that have a default
    // table which is non-empty, shich could be a privacy violation
    out << table_name << ": ";
    if (state.IsPII(table_name)) {
      out << "is PII" << std::endl << std::endl;
      continue;
    } else if (!state.IsSharded(table_name)) {
      out << "unsharded" << std::endl << std::endl;
      continue;
    } else {
      out << "sharded with" << std::endl;
    }

    const auto &sharding_infos = state.GetShardingInformation(table_name);
    const auto &varown_chains = GetAndReportShardingInformation(out, table_name, sharding_infos, state);

    ClassifyAndWarnAboutSharding(out, varown_chains, sharding_infos);
    WarningsFromSchema(out, schema);
    out << std::endl;
  }
  out << "############# END EXPLAIN PRIVACY #############" << std::endl;
  out << std::endl;
  out << std::endl;
}

}  // namespace explain
}  // namespace pelton

#endif  // PELTON_EXPLAIN_H_
