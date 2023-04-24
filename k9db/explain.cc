// EXPLAIN COMPLIANCE command.
#include "k9db/explain.h"

#include <iomanip>
#include <iostream>
// NOLINTNEXTLINE
#include <regex>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "k9db/shards/state.h"

namespace k9db {
namespace explain {

namespace {

auto &out = std::cout;

// Is the column that is used for this sharding (i.e. `info`) nullable. If so
// the sharding could be nullable and lead to orphaned data.
bool IsNullableSharding(const shards::TableName &tbl,
                        const shards::ShardDescriptor &info,
                        const shards::SharderState &state) {
  const shards::TableName &table_name =
      info.is_varowned() ? info.next_table() : tbl;
  const shards::ColumnName &column_name =
      info.is_varowned() ? info.upcolumn() : info.downcolumn();
  const auto &schema = state.GetTable(table_name).create_stmt;
  const auto &col = schema.GetColumn(column_name);
  bool ret = !col.HasConstraint(sqlast::ColumnConstraint::Type::NOT_NULL);
  return ret;
}

using ShardingChain =
    std::vector<std::reference_wrapper<const shards::ShardDescriptor>>;
using ShardingChainInfo = std::vector<ShardingChain>;

/*
 * Flatten out the tree of shards into individual paths. E.g.
 *
 *    A
 *    |
 *    B
 *   / \
 *  C   D
 *
 * Becomes the paths A->B->C and A->B->D
 *
 * Paths are stored in `results`. The last node in the results is a direct
 * sharding, not the data subject table.
 */
void BuildShardingChains(const shards::ShardDescriptor &desc,
                         const shards::SharderState &state,
                         const ShardingChain &running_chain,
                         ShardingChainInfo *results) {
  const auto &table = state.GetTable(desc.next_table());
  // The chain so far is taken by-ref from the prior call. Then we make a local
  // copy that pushes this sharding to the end and hand that by-ref to the
  // subsequewnt calls. We can't just mutably accumulate the chain because
  // ther's recursive fan-out that would contend for the chain. Also the vectors
  // are small and contain only references so copying is no big deal.
  ShardingChain local_chain = running_chain;
  local_chain.push_back(std::ref(desc));
  if (desc.is_transitive() || desc.is_varowned()) {
    for (const auto &tinfo : table.owners) {
      if (tinfo->shard_kind != desc.shard_kind) {
        continue;
      }
      BuildShardingChains(*tinfo, state, local_chain, results);
    }
  } else {
    // Desc is direct and so the next_table is the datasubject table.
    CHECK_EQ(table.table_name, desc.shard_kind)
        << "INVARIANT VIOLATED: the table " << table.table_name
        << " is being directly sharded to via " << desc
        << " (it should be a DATA_SUBJECT table)!";

    // We're done. This must be a direct sharding so no reason to recurse. Also
    // because the local chain is a local copy we can just move it.
    results->push_back(std::move(local_chain));
  }
}

// Matches column names for data that might be considered personal.
const std::regex SUSPICIOUS_COLUMN_NAME_INDICATORS(
    "email|password|(first|last|middle|user)[-_]?name|gender",
    std::regex_constants::ECMAScript | std::regex_constants::icase |
        std::regex_constants::nosubs | std::regex_constants::optimize);

// Emit warnigns that only relate to the schema (not the sharding). Currently
// just matches for column names that may indicate personal data and alerts if
// the table is not sharded and such columns have been found.
void WarningsFromSchema(const sqlast::CreateTable &schema,
                        const bool is_sharded, const bool is_pii) {
  using col_name_match_size_t = std::string::const_iterator::difference_type;
  if (is_sharded || is_pii) return;
  std::vector<std::tuple<const std::string *, const col_name_match_size_t,
                         const col_name_match_size_t>>
      suspicious_column_names;
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

// Print each sharding chain in human readable format.
void ReportShardingInformation(const shards::TableName &table_name,
                               const shards::SharderState &state,
                               const ShardingChainInfo &chains) {
  // The standard size of field to reserve for a column name
  const int std_col_size = 20;
  const auto &table_create_stmt = state.GetTable(table_name).create_stmt;
  for (const auto &chain : chains) {
    // We first print a subheader describing the shard kind and column in
    // original table.
    const auto &info = chain.front().get();
    const auto &column = table_create_stmt.GetColumn(info.downcolumn());
    std::string deduced = "implicitly deduced";
    if (info.is_varowned()) {
      // Variable ownerships are always explicit
      deduced = "explicit annotation";
    }
    using ColCons = sqlast::ColumnConstraint;
    if (column.HasConstraint(ColCons::Type::FOREIGN_KEY)) {
      const auto &cons = column.GetConstraintOfType(ColCons::Type::FOREIGN_KEY);
      if (std::get<2>(cons.ForeignKey()) == ColCons::FKType::OWNED_BY) {
        deduced = "explicit annotation";
      }
    }

    // Print sub-header.
    out << "  " << std::setiosflags(std::ios::left) << std::setw(std_col_size)
        << info.downcolumn() << " shards to "
        << std::setiosflags(std::ios::left) << std::setw(std_col_size)
        << info.shard_kind << " (" << deduced << ")" << std::endl;

    // Print the ownership chain.
    std::string last_table = table_name;
    for (const auto info_ref : chain) {
      const auto &info = info_ref.get();
      out << std::setw(6) << ""
          << "via   ";
      switch (info.type) {
        case shards::InfoType::DIRECT:
          out << last_table << "(" << info.downcolumn() << ") -> "
              << info.next_table() << "(" << info.upcolumn() << ")";
          break;
        case shards::InfoType::TRANSITIVE:
          out << last_table << "(" << info.downcolumn() << ") -> "
              << info.next_table() << "(" << info.upcolumn() << ")";
          break;
        case shards::InfoType::VARIABLE:
          out << last_table << "(" << info.downcolumn() << ") -> "
              << info.next_table() << "(" << info.upcolumn() << ")";
          break;
        default:
          LOG(FATAL) << "UNREACHABLE";
      }
      // << info->next_table() << "(" << tinfo.upcolumn() << ")";
      out << std::endl;
    }

    if (chain.size() > 1)
      out << "    with a total distance of " << chain.size() << std::endl;
  }
}

// Collate information about each sharding path (sharding chain) and the print
// warnings based on this information. Currently we try to figure out how many
// copies your annotations will cause to be stored and warn if either may be too
// many of them, either due to chained variable ownership or due to too many
// owner annotations on a table.
void ClassifyAndWarnAboutSharding(const shards::TableName table_name,
                                  const shards::SharderState &state,
                                  const ShardingChainInfo &sharding_chains) {
  using VarownChain =
      std::vector<std::reference_wrapper<const shards::TableName>>;
  bool is_always_nullable = true;

  std::vector<VarownChain> varown_chains;
  for (const auto &sharding_chain : sharding_chains) {
    // Tracks whether this chain has a nullable link
    bool is_nullable = false;
    // Tracks the variable ownership tables (the relationship tables) are in
    // this chain
    VarownChain varown_chain;
    // Reference to the table we're currently looking at so that we can check if
    // its sharding columns are nullable.
    const shards::TableName *current_table = &table_name;
    for (const auto &info : sharding_chain) {
      if (info.get().is_varowned())
        varown_chain.push_back(std::ref(info.get().next_table()));
      is_nullable =
          is_nullable || IsNullableSharding(*current_table, info, state);
      current_table = &info.get().next_table();
    }
    is_always_nullable = is_always_nullable && is_nullable;
    if (varown_chain.size() != 0) {
      varown_chains.push_back(std::move(varown_chain));
    }
  }

  const size_t varown_shardings = varown_chains.size();
  const size_t regular_shardings = sharding_chains.size() - varown_shardings;

  VarownChain empty_varown_chain;
  const auto &longest_chain_it =
      std::max_element(varown_chains.begin(), varown_chains.end(),
                       [](const auto &one, const auto &other) {
                         return one.size() < other.size();
                       });
  const VarownChain &longest_varown_chain =
      longest_chain_it == varown_chains.end() ? empty_varown_chain
                                              : *longest_chain_it;

  // Feedback section

  // At least one chain had multiple variable ownerships which can lead to a
  // copy explosion, so we warn about it and print (the variable ownership
  // tables in) the chain.
  if (longest_varown_chain.size() > 1) {
    out << "  [SEVERE]  This table is variably sharded "
        << longest_varown_chain.size()
        << " times in sequence. This will create ";
    bool put_sym = false;
    for (const auto &varown_table : longest_varown_chain) {
      if (put_sym)
        out << '*';
      else
        put_sym = true;
      out << "|" << varown_table.get() << "|";
    }
    out << " copies of records inserted into this table. This is likely not "
           "desired behavior, I suggest checking your `"
        << sqlast::ForeignKeyTypeEnum::OWNS << "` annotations." << std::endl;
  }
  // We detect there are multiple variable ownerships, but not necessarily in
  // the same chain. This is also a code smell so we warn about it and print in
  // each case the head of the variable ownership chain.
  if (varown_shardings > 1) {
    out << "  [Warning] This table is variably owned in multiple ways (via ";
    bool put_sym = false;
    for (const auto &varown_tables : varown_chains) {
      if (varown_tables.size() == 0) continue;
      if (put_sym)
        out << " and ";
      else
        put_sym = true;
      out << varown_tables.front().get();
    }
    out << "). This may not be desired behavior." << std::endl;
  }
  if (varown_shardings == 1 && longest_varown_chain.size() == 1) {
    // Only one variable ownership is detected, we report this not as a problem
    // but as perhaps useful information. Since there is only one we know that
    // also has to be the longest chain we found so we query that.
    //
    // We only report this if we haven't otherwise reported on variable
    // ownership.
    out << "  [Info]    This table is variably owned (via "
        << longest_varown_chain.front().get() << ")" << std::endl;
  }
  if (longest_varown_chain.size() == 1 && regular_shardings > 1) {
    // Copies in addition to variable ownership are a code smell so we report
    // how much it is copied in addition. The determination that
    // regular_shardings > 1 in addition to the varowns are where we draw the
    // line is arbitrary.
    out << "  [Warning] This table is variably owned and also copied an "
        << "additional " << regular_shardings << " times." << std::endl;
  } else if (regular_shardings > 5) {
    // Too many regular shardings are a code smell. The chosen threshold is
    // arbitrary.
    out << "  [Warning] This table is sharded " << regular_shardings
        << " times. This seem excessive, you may want to check your `"
        << sqlast::ForeignKeyTypeEnum::OWNED_BY << "`  annotations."
        << std::endl;
  } else if (regular_shardings > 2) {
    // Multiple copies of this table will be made. That could very well be
    // intended, but we report it as information in case it isn't.
    out << "  [Info]    This table is copied " << regular_shardings << " times"
        << std::endl;
  }
  if (is_always_nullable) {
    // All paths leading to this datum are nullable. If these foreign keys are
    // (accidentally) set to `NULL` this will store the data in the default
    // shard and either cause a compiance violation error (if enabled) or the
    // data may leak in this way so we always warn about it. If just one of the
    // paths is non-nullable that is enough because that means if all nullable
    // paths are set to `NULL` the data will still not go to the default shard,
    // but to the last non-nullable shard and thus if that user says `GDPR
    // FORGET` the data will be removed and not leak.
    out << "  [Warning] This table is sharded, but all sharding paths are "
           "nullable. This will lead to records being stored in a global table "
           "if those columns are NULL and could be a source of non-compliance."
        << std::endl;
  }
}

}  // namespace

void ExplainCompliance(const Connection &connection) {
  const shards::SharderState &state = connection.state->SharderState();

  out << std::endl;
  out << std::endl;
  out << "############# EXPLAIN COMPLIANCE #############" << std::endl;
  for (const auto &[table_name, table] : state.ReverseTables()) {
    out << "----------------------------------------- " << std::endl;
    out << table_name << ": ";

    bool is_sharded = table->owners.size() != 0;
    if (!is_sharded) {
      out << "UNSHARDED" << std::endl;
    } else {
      // Might be a datasubject table, or a table owned by other datasubjects,
      // or both.
      if (state.ShardKindExists(table_name)) {
        out << "DATASUBJECT";
        if (table->owners.size() > 1) {
          out << " AND SHARDED";
        }
        out << std::endl;
      } else {
        out << "SHARDED" << std::endl;
      }

      // Collect information about the chains starting from this table.
      ShardingChainInfo sharding_chains;
      for (const auto &own : table->owners) {
        if (own->shard_kind != table_name) {
          ShardingChain initial_chain;
          BuildShardingChains(*own, state, initial_chain, &sharding_chains);
        }
      }

      if (sharding_chains.size() > 0) {
        // Report statistics about the sharding itself
        ReportShardingInformation(table_name, state, sharding_chains);
        // Report violations arising from the sharding setup
        ClassifyAndWarnAboutSharding(table_name, state, sharding_chains);
      }
    }
    // Report possible vialations that we can infer from the schema alone
    WarningsFromSchema(table->create_stmt, is_sharded,
                       state.ShardKindExists(table_name));
  }
  out << "############# END EXPLAIN COMPLIANCE #############" << std::endl;
  out << std::endl;
  out << std::endl;
}

}  // namespace explain
}  // namespace k9db
