// Ported from memcached/main.cc.
mod encode;
mod mariadb;
mod memcached;

use clap::{App, Arg};
use regex::Regex;

use mariadb::MariaDBConnection;
use memcached::MemcachedConnection;

// Queries to cache.
const QUERIES: &[&str] = &[
    "SELECT comments.upvotes, comments.downvotes, comments.story_id FROM \
     comments JOIN stories ON comments.story_id = stories.id WHERE \
     comments.story_id = ? AND comments.user_id != stories.user_id",
    "SELECT comments.*, comments.upvotes - comments.downvotes AS saldo FROM \
     comments WHERE comments.story_id = ? ORDER BY saldo ASC, confidence DESC",
    "SELECT tags.*, taggings.story_id FROM tags INNER JOIN taggings ON tags.id \
     = taggings.tag_id WHERE taggings.story_id = ?",
    "SELECT stories.* FROM stories WHERE stories.merged_story_id IS NULL AND \
     stories.is_expired = 0 AND stories.upvotes - stories.downvotes >= 0 ORDER \
     BY hotness ASC LIMIT 51",
    "SELECT tags.id, stories.user_id, count(*) AS `count` FROM tags INNER JOIN \
     taggings ON tags.id = taggings.tag_id INNER JOIN stories ON \
     taggings.story_id = stories.id WHERE tags.inactive = 0 AND \
     stories.user_id = ? GROUP BY tags.id, stories.user_id ORDER BY `count` \
     DESC LIMIT 1",
    "SELECT comments.* FROM comments WHERE comments.is_deleted = 0 AND \
     comments.is_moderated = 0 ORDER BY id DESC LIMIT 40",
    "SELECT stories.* FROM stories WHERE stories.merged_story_id IS NULL AND \
     stories.is_expired = 0 AND stories.upvotes - stories.downvotes <= 5 ORDER \
     BY stories.id DESC LIMIT 51",
    "SELECT read_ribbons.user_id, COUNT(*) FROM read_ribbons JOIN stories ON \
     (read_ribbons.story_id = stories.id) JOIN comments ON \
     (read_ribbons.story_id = comments.story_id) LEFT JOIN comments AS \
     parent_comments ON (comments.parent_comment_id = parent_comments.id) \
     WHERE read_ribbons.is_following = 1 AND comments.user_id <> \
     read_ribbons.user_id AND comments.is_deleted = 0 AND \
     comments.is_moderated = 0 AND ( comments.upvotes - comments.downvotes ) \
     >= 0 AND read_ribbons.updated_at < comments.created_at AND ( ( \
     parent_comments.user_id = read_ribbons.user_id AND ( \
     parent_comments.upvotes - parent_comments.downvotes ) >= 0 ) OR ( \
     parent_comments.id IS NULL AND stories.user_id = read_ribbons.user_id ) ) \
     GROUP BY read_ribbons.user_id HAVING read_ribbons.user_id = ?",
];

// Given a query and the "col = ? AND/WHERE/HAVING" patterns, strips the first
// matching clause and returns the rewritten query plus the stripped column.
fn strip_key_equals(query: &str, patterns: &[Regex]) -> Option<(String, String)> {
    for re in patterns {
        if let Some(caps) = re.captures(query) {
            let before = caps.get(1).unwrap().as_str();
            let key_col = caps.get(2).unwrap().as_str().to_string();
            let after = caps.get(3).unwrap().as_str();
            return Some((format!("{}{}", before, after), key_col));
        }
    }
    None
}

fn main() {
    // Regex to find key column.
    let regex_before_and = Regex::new(r"^(.*) ([A-Za-z0-9\._]+) \= \? AND(.*)$").unwrap();
    let regex_after_and = Regex::new(r"^(.*) AND ([A-Za-z0-9\._]+) \= \?(.*)$").unwrap();
    let regex_where = Regex::new(r"^(.*) WHERE ([A-Za-z0-9\._]+) \= \?(.*)$").unwrap();
    let regex_having = Regex::new(r"^(.*) HAVING ([A-Za-z0-9\._]+) \= \?(.*)$").unwrap();
    let regex_column = Regex::new(r"^([A-Za-z_0-9]+\.)?([A-Za-z0-9_]+)$").unwrap();
    let regex_limit = Regex::new(r"^(.*) LIMIT ([0-9]+)$").unwrap();
    let and_patterns = [regex_before_and, regex_after_and, regex_where, regex_having];

    // Parse command line flags.
    let matches = App::new("memcached-memory")
        .arg(
            Arg::with_name("database")
                .long("database")
                .takes_value(true)
                .default_value("127.0.0.1")
                .help("IP of MariaDB database with lobsters"),
        )
        .get_matches();

    // Connect to Mariadb and memcached.
    let mut mariadb = MariaDBConnection::new(matches.value_of("database").unwrap());
    let mut memcached = MemcachedConnection::new();

    let mut total_size: u64 = 0;

    // Loop over queries.
    for (i, &query) in QUERIES.iter().enumerate() {
        let mut query = query.to_string();

        // Find the name of the key column (if any).
        let mut key: Vec<String> = Vec::new();
        while let Some((new_query, key_col)) = strip_key_equals(&query, &and_patterns) {
            query = new_query;
            match regex_column.captures(&key_col) {
                Some(caps) => key.push(caps.get(2).unwrap().as_str().to_string()),
                None => panic!("Query {} key {} did not match regex", QUERIES[i], key_col),
            }
        }

        // Find the limit and remove it.
        let mut limit: i64 = -1;
        if let Some(caps) = regex_limit.captures(&query) {
            let stripped = caps.get(1).unwrap().as_str().to_string();
            limit = caps.get(2).unwrap().as_str().parse::<i64>().unwrap();
            query = stripped;
        }

        // The query now has no ? and the key column is found in key.
        // Execute the query.
        let data = mariadb.query(&key, &query, limit);

        // Write the data to memcached, keep track of memory.
        let mut query_size: u64 = 0;
        for (key, records) in &data {
            query_size += memcached.write(i as u64, key, records);
        }
        println!("_{} in MB: {}", i, (query_size as f64) / 1_048_576.0);
        total_size += query_size;
    }

    // Write results.
    println!("Total size in bytes: {}", total_size);
    println!("Total size in MB: {}", (total_size as f64) / 1_048_576.0);
}
