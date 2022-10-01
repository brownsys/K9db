// EXPLAIN PRIVACY command.
#include "pelton/explain.h"

#include <iostream>

#include "pelton/shards/state.h"

namespace pelton {
namespace explain {

/*
namespace {

bool IsNullableSharding(const shards::UnshardedTableName &tbl,
                        const shards::ShardingInformation &info,
                        const shards::SharderState &state) {
  const shards::UnshardedTableName &table_name =
      info.is_varowned() ? info.next_table : tbl;
  const shards::ColumnName &column_name =
      info.is_varowned() ? info.next_column : info.shard_by;
  const auto &schema = state.GetSchema(table_name);
  const auto &col = schema.GetColumn(column_name);
  bool ret = !col.HasConstraint(sqlast::ColumnConstraint::Type::NOT_NULL);
  return ret;
}

bool PrintTransitivityChain(
    std::ostream &out, const shards::ShardingInformation &info,
    const shards::SharderState &state, int indent,
    std::vector<const shards::UnshardedTableName *> *varshards) {
  const auto &transitive_infos =
      state.GetShardingInformationFor(info.next_table, info.shard_kind);
  const shards::ShardingInformation *tinfo_ptr = nullptr;
  if (transitive_infos.size() == 0) {
    LOG(FATAL) << "No next shardings for table: " << info.next_table
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
  bool is_nullable = IsNullableSharding(info.next_table, tinfo, state);
  if (tinfo.is_varowned()) varshards->push_back(&tinfo.next_table);
  out << std::setw(indent) << ""
      << "via " << info.next_table << "(" << tinfo.shard_by
      << ") resolved with index " << info.next_index_name << std::endl;

  if (tinfo.IsTransitive())
    is_nullable = is_nullable ||
                  PrintTransitivityChain(out, tinfo, state, indent, varshards);
  return is_nullable;
}

const std::regex SUSPICIOUS_COLUMN_NAME_INDICATORS(
    "email|password|(first|last|middle|user)[-_]?name|gender",
    std::regex_constants::ECMAScript | std::regex_constants::icase |
        std::regex_constants::nosubs | std::regex_constants::optimize);

void WarningsFromSchema(std::ostream &out, const sqlast::CreateTable &schema,
                        const bool is_sharded, const bool is_pii) {
  using col_name_match_size_t = std::string::const_iterator::difference_type;
  if (is_sharded || is_pii) return;
  std::vector<std::tuple<const std::string *, const col_name_match_size_t,
                         const col_name_match_size_t>>
      suspicious_column_names{};
  for (const auto &col : schema.GetColumns()) {
    std::smatch match;
    bool matched = std::regex_search(col.column_name(), match,
                                     SUSPICIOUS_COLUMN_NAME_INDICATORS);
    if (matched) {
      CHECK(!match.empty());
      CHECK(match.ready());
      suspicious_column_names.emplace_back(&col.column_name(), match.position(),
                                           match.length());
    }
  }
  if (!suspicious_column_names.empty()) {
    const bool is_just_one = suspicious_column_names.size() == 1;
    out << "  [Warning] The column" << (is_just_one ? "" : "s") << " ";
    bool put_comma = false;
    for (const auto &[name, from, to] : suspicious_column_names) {
      out << (put_comma ? ", " : "") << std::quoted(*name);
      put_comma = true;
    }
    out << " sound" << (is_just_one ? "s" : "") << " like "
        << (is_just_one ? "it" : "they")
        << " may belong to a data subject, but the table is not sharded. You "
           "may want to review your annotations."
        << std::endl;
  }
}

using ShardingChainInfo =
    std::vector<std::vector<const shards::UnshardedTableName *>>;

std::pair<ShardingChainInfo, bool> GetAndReportShardingInformation(
    std::ostream &out, const shards::UnshardedTableName &table_name,
    const std::list<shards::ShardingInformation> &sharding_infos,
    const shards::SharderState &state) {
  ShardingChainInfo varown_chains{};
  bool is_nullable = true;
  for (const auto &info : sharding_infos) {
    bool this_sharding_is_nullable =
        IsNullableSharding(table_name, info, state);
    std::vector<const shards::UnshardedTableName *> varown_chain{};
    if (info.is_varowned()) varown_chain.push_back(&info.next_table);
    out << "  " << info.shard_by << " shards to " << info.shard_kind
        << std::endl;
    if (info.IsTransitive()) {
      this_sharding_is_nullable =
          this_sharding_is_nullable ||
          PrintTransitivityChain(out, info, state, 4, &varown_chain);
      out << "    total transitive distance is " << info.distance_from_shard
          << std::endl;
    }
    if (varown_chain.size() > 0) varown_chains.push_back(varown_chain);
    is_nullable = is_nullable && this_sharding_is_nullable;
  }
  return std::make_pair(varown_chains, is_nullable);
}

void ClassifyAndWarnAboutSharding(
    std::ostream &out, const ShardingChainInfo &varown_chains,
    const std::list<shards::ShardingInformation> &sharding_infos,
    const bool is_nullable) {
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
  if (is_nullable) {
    out << "  [Warning] This table is sharded, but all sharding paths are "
           "nullable. This will lead to records being stored in a global table "
           "if those columns are NULL and could be a source of non-compliance."
        << std::endl;
  }
}

}  // namespace
*/

void ExplainPrivacy(const Connection &connection) {
  const shards::SharderState &state = connection.state->SharderState();

  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << "############# EXPLAIN PRIVACY #############" << std::endl;
  for (const auto &[table_name, table] : state.AllTables()) {
    // Eventually also include if there are sharded tables that have a default
    // table which is non-empty, shich could be a privacy violation
    std::cout << " ----------------------------------------- " << std::endl;
    std::cout << table_name << std::endl;
    if (state.ShardKindExists(table_name)) {
      std::cout << "  DATASUBJECT" << std::endl;
    }

    std::cout << "  OWNERS:" << std::endl;
    for (const auto &descriptor : table.owners) {
      std::cout << *descriptor << std::endl;
    }

    std::cout << "  ACCESSORS:" << std::endl;
    for (const auto &descriptor : table.accessors) {
      std::cout << *descriptor << std::endl;
    }
  }
  std::cout << "############# END EXPLAIN PRIVACY #############" << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;
}

}  // namespace explain
}  // namespace pelton
