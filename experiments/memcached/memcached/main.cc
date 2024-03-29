#include <cassert>
#include <string>
#include <vector>
// NOLINTNEXTLINE
#include <regex>

#include "gflags/gflags.h"
#include "memcached/encode.h"
#include "memcached/mariadb.h"
#include "memcached/memcached.h"

// The IP of the MariaDB database server where the lobsters database is stored.
DEFINE_string(database, "127.0.0.1", "IP of MariaDB database with lobsters");

// Queries to cache.
static std::vector<std::string> QUERIES = {
    "SELECT comments.upvotes, comments.downvotes, comments.story_id FROM "
    "comments JOIN stories ON comments.story_id = stories.id WHERE "
    "comments.story_id = ? AND comments.user_id != stories.user_id",
    "SELECT comments.*, comments.upvotes - comments.downvotes AS saldo FROM "
    "comments WHERE comments.story_id = ? ORDER BY saldo ASC, confidence DESC",
    "SELECT tags.*, taggings.story_id FROM tags INNER JOIN taggings ON tags.id "
    "= taggings.tag_id WHERE taggings.story_id = ?",
    "SELECT stories.* FROM stories WHERE stories.merged_story_id IS NULL AND "
    "stories.is_expired = 0 AND stories.upvotes - stories.downvotes >= 0 ORDER "
    "BY hotness ASC LIMIT 51",
    "SELECT tags.id, stories.user_id, count(*) AS `count` FROM tags INNER JOIN "
    "taggings ON tags.id = taggings.tag_id INNER JOIN stories ON "
    "taggings.story_id = stories.id WHERE tags.inactive = 0 AND "
    "stories.user_id = ? GROUP BY tags.id, stories.user_id ORDER BY `count` "
    "DESC LIMIT 1",
    "SELECT comments.* FROM comments WHERE comments.is_deleted = 0 AND "
    "comments.is_moderated = 0 ORDER BY id DESC LIMIT 40",
    "SELECT stories.* FROM stories WHERE stories.merged_story_id IS NULL AND "
    "stories.is_expired = 0 AND stories.upvotes - stories.downvotes <= 5 ORDER "
    "BY stories.id DESC LIMIT 51",
    "SELECT read_ribbons.user_id, COUNT(*) FROM read_ribbons JOIN stories ON "
    "(read_ribbons.story_id = stories.id) JOIN comments ON "
    "(read_ribbons.story_id = comments.story_id) LEFT JOIN comments AS "
    "parent_comments ON (comments.parent_comment_id = parent_comments.id) "
    "WHERE read_ribbons.is_following = 1 AND comments.user_id <> "
    "read_ribbons.user_id AND comments.is_deleted = 0 AND "
    "comments.is_moderated = 0 AND ( comments.upvotes - comments.downvotes ) "
    ">= 0 AND read_ribbons.updated_at < comments.created_at AND ( ( "
    "parent_comments.user_id = read_ribbons.user_id AND ( "
    "parent_comments.upvotes - parent_comments.downvotes ) >= 0 ) OR ( "
    "parent_comments.id IS NULL AND stories.user_id = read_ribbons.user_id ) ) "
    "GROUP BY read_ribbons.user_id HAVING read_ribbons.user_id = ?"};

// Regex to find key column.
static std::regex REGEX_BEFORE_AND{"(.*) ([A-Za-z0-9\\._]+) \\= \\? AND(.*)"};
static std::regex REGEX_AFTER_AND{"(.*) AND ([A-Za-z0-9\\._]+) \\= \\?(.*)"};
static std::regex REGEX_WHERE{"(.*) WHERE ([A-Za-z0-9\\._]+) \\= \\?(.*)"};
static std::regex REGEX_HAVING{"(.*) HAVING ([A-Za-z0-9\\._]+) \\= \\?(.*)"};
static std::regex REGEX_COLUMN{"([A-Za-z_0-9]+\\.)?([A-Za-z0-9_]+)"};
static std::regex REGEX_LIMIT{"(.*) LIMIT ([0-9]+)"};

int main(int argc, char **argv) {
  uint64_t total_size = 0;

  // Parse command line flags.
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Connect to Mariadb and memcached.
  MariaDBConnection mariadb(FLAGS_database);
  MemcachedConnection memcached;

  // Loop over queries.
  for (size_t i = 0; i < QUERIES.size(); i++) {
    std::string query = QUERIES.at(i);
    // Find the name of the key column (if any).
    std::vector<std::string> key;
    std::smatch matches;
    while (std::regex_match(query, matches, REGEX_BEFORE_AND) ||
           std::regex_match(query, matches, REGEX_AFTER_AND) ||
           std::regex_match(query, matches, REGEX_WHERE) ||
           std::regex_match(query, matches, REGEX_HAVING)) {
      std::string key_col = matches[2];
      query = std::string(matches[1]) + std::string(matches[3]);
      if (std::regex_match(key_col, matches, REGEX_COLUMN)) {
        key.push_back(matches[2]);
      } else {
        std::cerr << "Query " << QUERIES.at(i) << " key " << key_col
                  << " did not match regex" << std::endl;
        assert(false);
      }
    }
    // Find the limit and remove it.
    int limit = -1;
    if (std::regex_match(query, matches, REGEX_LIMIT)) {
      query = std::string(matches[1]);
      limit = std::stoi(matches[2]);
    }

    // The query now has no ? and the key column is found in key.
    // Execute the query.
    std::unordered_map<MemcachedKey, std::vector<MemcachedRecord>> data =
        mariadb.Query(key, query, limit);

    // Write the data to memcached, keep track of memory.
    uint64_t query_size = 0;
    for (const auto &[key, records] : data) {
      query_size += memcached.Write(i, key, records);
    }
    std::cout << "_" << i << " in MB: " << (query_size / 1048576.0)
              << std::endl;
    total_size += query_size;
  }

  // Write results.
  std::cout << "Total size in bytes: " << total_size << std::endl;
  std::cout << "Total size in MB: " << (total_size / 1048576.0) << std::endl;
}
