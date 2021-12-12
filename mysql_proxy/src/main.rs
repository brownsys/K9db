#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

#[macro_use]
extern crate slog;
extern crate ctrlc;
extern crate msql_srv;
extern crate proxy_ffi;
extern crate regex;
extern crate slog_term;
#[macro_use]
extern crate lazy_static;

use msql_srv::*;
use proxy_ffi::*;
use regex::Regex;
use slog::Drain;
use std::collections::HashMap;
use std::ffi::CString;
use std::io;
use std::net;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

const USAGE: &str = "
  Available options:
  --help (false): displays this help message.
  --workers (3): number of dataflow workers.
  --db_name (pelton): name of the database.
  --db_username (root): database user to connect with to mariadb.
  --db_password (password): password to connect with to mariadb.";

static NO_VIEWS: [&'static str; 22] = [
    // For ci tests.
    "SELECT * FROM tbl WHERE id = ?",
    "SELECT * FROM tbl WHERE id = ? AND age > ?",
    // TODO: add lobsters here.
    "SELECT 1 AS `one` FROM users WHERE users.PII_username = ?",
    "SELECT 1 AS `one`, short_id FROM stories WHERE stories.short_id = ?",
    "SELECT tags.* FROM tags WHERE tags.inactive = 0 AND tags.tag = ?",
    "SELECT keystores.* FROM keystores WHERE keystores.keyX = ?",
    "SELECT votes.* FROM votes WHERE votes.OWNER_user_id = ? AND votes.story_id = ? AND votes.comment_id IS NULL",
    "SELECT stories.* FROM stories WHERE stories.short_id = ?",
    "SELECT users.* FROM users WHERE users.id = ?",
    "SELECT 1 AS `one`, short_id FROM comments WHERE comments.short_id = ?",
    "SELECT votes.* FROM votes WHERE votes.OWNER_user_id = ? AND votes.story_id = ? AND votes.comment_id = ?",
    "SELECT comments.* FROM comments WHERE comments.story_id = ? AND comments.short_id = ?",
    "SELECT read_ribbons.* FROM read_ribbons WHERE read_ribbons.user_id = ? AND read_ribbons.story_id = ?",
    // NOTE: If a view is needed for this then the query in the harness has to be changed to
    // project the user_id column.
    "SELECT hidden_stories.story_id FROM hidden_stories WHERE hidden_stories.user_id = ?",
    "SELECT users.* FROM users WHERE users.PII_username = ?",
    "SELECT hidden_stories.* FROM hidden_stories WHERE hidden_stories.user_id = ? AND hidden_stories.story_id = ?",
    "SELECT tag_filters.* FROM tag_filters WHERE tag_filters.user_id = ?",
    "SELECT taggings.story_id FROM taggings WHERE taggings.story_id = ?",
    "SELECT saved_stories.* FROM saved_stories WHERE saved_stories.user_id = ? AND saved_stories.story_id = ?",
    "SELECT 1, user_id, story_id FROM hidden_stories WHERE hidden_stories.user_id = ? AND hidden_stories.story_id = ?",
    "SELECT votes.* FROM votes WHERE votes.OWNER_user_id = ? AND votes.comment_id = ?",
    "SELECT comments.* FROM comments WHERE comments.short_id = ?",
];

lazy_static! {
    static ref PARAMETER_REGEX : Regex = Regex::new(
        r"(?:\b|\s)(?P<expr>[A-Za-z_0-9`\.]+\s*[=><]\s*)\?").unwrap();
}

#[derive(Debug)]
struct Backend {
    rust_conn: FFIConnection,
    runtime: std::time::Duration,
    log: slog::Logger,
    stop: Arc<AtomicBool>,
    // 2D HashMap. HashMap1 maps actual prepared statement received from client or the
    // reduced/normalized form of a statement that contains a WHERE IN clause to Hashmap2.
    // And Hashmap2 maps the "metadata" to the statement_id of the prepared statement.
    // NOTE: semantics of "metadata" that is mentioned in above comment:
    // It is a clean mechanism of finding out whether a ?mark in the reduced form was literally a
    // ?mark in the original prepared statement or was it an IN clause and also it helps in
    // finding out that if it was a IN clause then what was it's parameter count.
    // Example: original prepared statement: "WHERE col1 = ? AND col2 IN (?,?,?) and col3 = ?"
    // metadata vector for above statement: [None, Some(3), None].
    // For INSERT, UPDATE and REPLACE statements the metadata will always consist of None(s).
    prepared_statements: Arc<RwLock<HashMap<String, HashMap<Vec<Option<u32>>, u32>>>>,
    // Maps statement_id to internal representation of the prepared statement.
    prepared_index: Arc<RwLock<HashMap<u32, Vec<String>>>>,
    // A structure for mapping the statement ids of WHERE IN parametrised statements with variable
    // ?marks to the corresponding (reduced_statement_id, metadata) tuple.
    wherein_view_index: Arc<RwLock<HashMap<u32, (u32, Vec<Option<u32>>)>>>,
    // Atomic prepared statement id generator.
    // WHERE IN prepared statements with different ids will have different statement ids, but those
    // statements never get prepared in reality. Their statement_id internally gets wired to the
    // reduced statement_id via the above data structures.
    id_generator: Arc<AtomicU32>,
}

impl Backend {
    fn create_view_stmt(&self, view_name: &str, prepared_statement: &str) -> String {
        // Form the query
        let mut create_view_stmt = String::from("CREATE VIEW ");
        create_view_stmt.push_str(&view_name);
        create_view_stmt.push_str(" AS '\"");
        create_view_stmt.push_str(&prepared_statement);
        create_view_stmt.push_str("\"';");
        return create_view_stmt;
    }

    // This function specifically handles the case where a prepared statement needs to be modified so
    // that it queries a planned view instead.
    fn construct_internal_format(&self, view_name: &str, prepared_statement: &str) -> Vec<String> {
        // Change query from "SELECT * FROM table WHERE col=?" to
        // "SELECT * FROM q0 WHERE col=?", where q0 is the view name
        // assigned to the prepared statement, makes parameterizing and
        // executing prepared statement later easier.
        // let re = Regex::new(r"\s(?P<expr>[A-Za-z_0-9`\.]+\s*[=><]\s*)\?").unwrap();
        // Expression used for extracting "table.column = ?" or "table.column IN"
        let re = Regex::new(r"\s(?P<expr>[A-Za-z_0-9`\.]+\s*([=><]|(\s+IN\s+))\s*)\?|(\s+IN\s+)")
            .unwrap();
        let mut view_query: Vec<String> = re
            .captures_iter(&prepared_statement)
            .map(|caps| caps["expr"].to_string())
            .collect();
        if view_query.len() == 0 {
            view_query.push(String::from("SELECT * FROM ") + &view_name + ";");
        } else {
            // Truncate the table name from the where clause
            let split = view_query[0].split(".");
            let split_result: Vec<&str> = split.collect();
            let column_name = match split_result.len() {
                1 => split_result[0],
                2 => split_result[1],
                _ => panic!("Rust proxy: The where clause contains more than one '.'"),
            };
            view_query[0] = String::from("SELECT * FROM ") + &view_name + " WHERE " + column_name;
            for i in 1..view_query.len() {
                // Truncate the table name from each where clause.
                let split = view_query[i].split(".");
                let split_result: Vec<&str> = split.collect();
                let column_name = match split_result.len() {
                    1 => split_result[0],
                    2 => split_result[1],
                    _ => panic!("Rust proxy: The where clause contains more than one '.'"),
                };
                view_query[i] = String::from(" AND ") + column_name;
            }
            view_query.push(String::from(";"));
        }
        return view_query;
    }

    // Helper function to infer metadata from select queries
    fn infer_metadata_select(&self, prepared_statement: &str) -> Vec<Option<u32>> {
        // Infer metadata from the query
        let mut metadata: Vec<Option<u32>> = Vec::new();
        // This regex captures " = ?" and "(?,?, ?)". And we iterate over them in order.
        let re_metadata =
            Regex::new(r"(?P<single>=\s*\?\s*)|(?P<in_clause>\s*\([\?\s,]+\)\s*)").unwrap();
        for cap in re_metadata.captures_iter(prepared_statement) {
            match cap.get(2) {
                Some(in_clause) => {
                    metadata.push(Some(in_clause.as_str().matches("?").count() as u32))
                }
                None => metadata.push(None),
            }
        }
        return metadata;
    }

    // Helper function to infer metadata for INSERT, UPDATE and REPLACE statements
    fn infer_metadata_rest(&self, prepared_statement: &str) -> Vec<Option<u32>> {
        // Infer metadata from the query
        let mut metadata: Vec<Option<u32>> = Vec::new();
        // Simply count the ?marks
        for _i in 0..prepared_statement.matches("?").count() {
            // Unlike selects, they do not contain count.
            metadata.push(None);
        }
        return metadata;
    }

    // Helper function that computes parameter count based on the supplied metadata.
    fn compute_param_count(&self, metadata: &Vec<Option<u32>>) -> u32 {
        let mut param_count = 0;
        for entry in metadata {
            match entry {
                None => param_count = param_count + 1,
                Some(count) => param_count = param_count + count,
            }
        }
        return param_count;
    }

    // Helper function that returns statement id of the reduced form.
    fn get_reduced_stmt_id(&self, map: &HashMap<Vec<Option<u32>>, u32>) -> u32 {
        // Get any entry
        let length = map.keys().next().unwrap().len();
        // Construct a metadata object that points to the reduced statment id.
        // NOTE: This statement id is mapped to the internal format of the prepared statement which
        // is accessed either directly or indirectly in on_execute.
        // NOTE: The "default" metadata object will always exist. This is the only way to retreive
        // statement id of the reduced form.
        let mut default_metadata: Vec<Option<u32>> = Vec::new();
        for _i in 0..length {
            default_metadata.push(None);
        }
        return map.get(&default_metadata).unwrap().clone();
    }

    fn handle_prepared_wherein(
        &mut self,
        prepared_statement: &str,
        metadata: Vec<Option<u32>>,
    ) -> (u32, u32) {
        debug!(
            self.log,
            "Rust proxy: handle_prepared_wherein {}", prepared_statement
        );
        // Construct the parametrised form of the statement that contains a single ?mark
        let reduced_form: String;
        let param_re = Regex::new(r"IN\s*\([,\s\?]*\)").unwrap();
        reduced_form = param_re.replace_all(prepared_statement, "= ?").into_owned();

        let prepared_read_guard = self.prepared_statements.read().unwrap();
        // Two harness threads could issue the same prepared statement twice hence if the prepared
        // statement has already been constructed then simply return.
        if prepared_read_guard.contains_key(&reduced_form) {
            if prepared_read_guard
                .get(&reduced_form)
                .unwrap()
                .contains_key(&metadata)
            {
                // The statement with the exact parameter count has already been inserted before.
                let stmt_id = prepared_read_guard
                    .get(&reduced_form)
                    .unwrap()
                    .get(&metadata)
                    .unwrap();
                return (stmt_id.clone(), self.compute_param_count(&metadata));
            } else {
                // View for this statement's reduced form has already been created. Just create
                // a new statement id (without creating a prepared statement) and wire it to the
                // reduced form's statement id.
                // Grab the write lock before doing any of it!
                drop(prepared_read_guard);
                let mut prepared_write_guard = self.prepared_statements.write().unwrap();
                if prepared_write_guard
                    .get(&reduced_form)
                    .unwrap()
                    .contains_key(&metadata)
                {
                    // Some other thread has already performed the insert while this thread was
                    // busy grabbing the write lock.
                    let stmt_id = prepared_write_guard
                        .get(prepared_statement)
                        .unwrap()
                        .get(&metadata)
                        .unwrap();
                    return (stmt_id.clone(), self.compute_param_count(&metadata));
                } // Else proceed to perform wiring.
                let reduced_stmt_id = self
                    .get_reduced_stmt_id(&prepared_write_guard.get(&reduced_form).unwrap());
                let mut wherein_write_guard = self.wherein_view_index.write().unwrap();
                let new_stmt_id = self.id_generator.fetch_add(1, Ordering::SeqCst);
                let param_count = self.compute_param_count(&metadata);
                // Update data structures
                prepared_write_guard
                    .get_mut(&reduced_form)
                    .unwrap()
                    .insert(metadata.clone(), new_stmt_id);
                wherein_write_guard.insert(new_stmt_id, (reduced_stmt_id, metadata));
                return (new_stmt_id, param_count);
            }
        }
        // Data structure does not contain reduced form => encountering this form for the first time.
        // Construct a view.
        // Protocol:
        // 1. Drop the current shared lock
        // 2. Acquire the unique lock
        // 3. Check if the prepared statment has not been added by another thread.
        // 4. If above condition is true then proceed to add the prepared statement.
        drop(prepared_read_guard);
        let mut prepared_write_guard = self.prepared_statements.write().unwrap();
        // Perform a similar check as above, since things could have changed while this thread
        // was acquiring the write lock.
        if prepared_write_guard.contains_key(&reduced_form) {
            if prepared_write_guard
                .get(&reduced_form)
                .unwrap()
                .contains_key(&metadata)
            {
                // The statement with the exact parameter count has already been inserted before.
                let stmt_id = prepared_write_guard
                    .get(prepared_statement)
                    .unwrap()
                    .get(&metadata)
                    .unwrap();
                return (stmt_id.clone(), self.compute_param_count(&metadata));
            } else {
                // View for this statement's reduced form has already been created. Just create
                // a new statement id (without creating a prepared statement) and wire it to the
                // reduced form's statement id.
                // Proceed to perform wiring.
                let reduced_stmt_id = self
                    .get_reduced_stmt_id(&prepared_write_guard.get(&reduced_form).unwrap());
                let mut wherein_write_guard = self.wherein_view_index.write().unwrap();
                let new_stmt_id = self.id_generator.fetch_add(1, Ordering::SeqCst);
                let param_count = self.compute_param_count(&metadata);
                // Update data structures
                prepared_write_guard
                    .get_mut(&reduced_form)
                    .unwrap()
                    .insert(metadata.clone(), new_stmt_id);
                wherein_write_guard.insert(new_stmt_id, (reduced_stmt_id, metadata));
                return (new_stmt_id, param_count);
            }
        } // Else proceed to create the view and do the wiring as usual.
        let reduced_stmt_id = self.id_generator.fetch_add(1, Ordering::SeqCst);
        let reduced_internal_format: Vec<String>;
        if !NO_VIEWS.contains(&reduced_form.as_str()) {
            let view_name = String::from("q") + &reduced_stmt_id.to_string();
            let create_view_stmt = self.create_view_stmt(&view_name, &reduced_form);
            // Execute create view statement in Pelton
            if !exec_ddl_ffi(&mut self.rust_conn, &create_view_stmt) {
                error!(
                    self.log,
                    "Rust proxy: Failed to execute create view {}", create_view_stmt
                );
            }
            reduced_internal_format = self.construct_internal_format(&view_name, &reduced_form);
        } else {
            reduced_internal_format = reduced_form.split("?").map(|s| s.to_string()).collect();
        }
        // Associate a unique id for the original prepared statement.
        let original_stmt_id = self.id_generator.fetch_add(1, Ordering::SeqCst);
        let original_param_count = self.compute_param_count(&metadata);
        // Construct default metadata will map to the reduced statement id.
        // This the only way the reduced statement id can be obtained later!
        let mut default_metadata: Vec<Option<u32>> = Vec::new();
        for _i in 0..metadata.len() {
            default_metadata.push(None);
        }

        // Update data structures.
        let mut wherein_write_guard = self.wherein_view_index.write().unwrap();
        let mut index_write_guard = self.prepared_index.write().unwrap();
        assert!(!prepared_write_guard.contains_key(&reduced_form));
        prepared_write_guard.insert(reduced_form.clone(), HashMap::new());
        prepared_write_guard
            .get_mut(&reduced_form)
            .unwrap()
            .insert(default_metadata, reduced_stmt_id);
        index_write_guard.insert(reduced_stmt_id, reduced_internal_format);
        // Wiring
        prepared_write_guard
            .get_mut(&reduced_form)
            .unwrap()
            .insert(metadata.clone(), original_stmt_id);
        wherein_write_guard.insert(original_stmt_id, (reduced_stmt_id, metadata));
        return (original_stmt_id, original_param_count);
    }

    fn handle_prepared_select_needs_view(
        &mut self,
        prepared_statement: &str,
        metadata: Vec<Option<u32>>,
    ) -> (u32, u32) {
        debug!(
            self.log,
            "Rust proxy: handle_prepared_select_needs_view {}", prepared_statement
        );
        let prepared_read_guard = self.prepared_statements.read().unwrap();
        // Two harness threads could issue the same prepared statement twice hence uf the prepared
        // statement has already been constructed then simply return.
        if prepared_read_guard.contains_key(prepared_statement)
            && prepared_read_guard
                .get(prepared_statement)
                .unwrap()
                .contains_key(&metadata)
        {
            let stmt_id = prepared_read_guard
                .get(prepared_statement)
                .unwrap()
                .get(&metadata)
                .unwrap();
            return (stmt_id.clone(), self.compute_param_count(&metadata));
        }

        // Data structure does not contain reduced form => encountering this form for the first time.
        // Construct a view.
        // Protocol:
        // 1. Drop the current shared lock
        // 2. Acquire the unique lock
        // 3. Check if the prepared statment has not been added by another thread.
        // 4. If above condition is true then proceed to add the prepared statement.
        drop(prepared_read_guard);
        let mut prepared_write_guard = self.prepared_statements.write().unwrap();
        if prepared_write_guard.contains_key(prepared_statement)
            && prepared_write_guard
                .get(prepared_statement)
                .unwrap()
                .contains_key(&metadata)
        {
            let stmt_id = prepared_write_guard
                .get(prepared_statement)
                .unwrap()
                .get(&metadata)
                .unwrap();
            return (stmt_id.clone(), self.compute_param_count(&metadata));
        } // Else proceceed to create the view and add the prepared statement.
        let stmt_id = self.id_generator.fetch_add(1, Ordering::SeqCst);
        let view_name = String::from("q") + &stmt_id.to_string();
        let create_view_stmt = self.create_view_stmt(&view_name, &prepared_statement);
        // Execute create view statement in Pelton
        if !exec_ddl_ffi(&mut self.rust_conn, &create_view_stmt) {
            error!(
                self.log,
                "Rust proxy: Failed to execute create view {}", create_view_stmt
            );
        }
        let internal_format =
            self.construct_internal_format(&view_name.as_str(), prepared_statement);
        let param_count = prepared_statement.matches("?").count() as u32;

        debug!(self.log, "Rust proxy: Prepared as: {:?}", internal_format);

        // Update data structures
        let mut index_write_guard = self.prepared_index.write().unwrap();
        if !prepared_write_guard.contains_key(prepared_statement) {
            prepared_write_guard.insert(prepared_statement.to_string(), HashMap::new());
        }
        prepared_write_guard
            .get_mut(prepared_statement)
            .unwrap()
            .insert(metadata, stmt_id);
        index_write_guard.insert(stmt_id, internal_format);
        return (stmt_id, param_count);
    }

    // Handles INSERT, REPLACE, UPDATE and SELECT (with no view requirement)
    fn handle_prepared_rest(
        &mut self,
        prepared_statement: &str,
        metadata: Vec<Option<u32>>,
    ) -> (u32, u32) {
        debug!(
            self.log,
            "Rust proxy: handle_prepared_rest: {}", prepared_statement
        );
        let prepared_read_guard = self.prepared_statements.read().unwrap();
        // Two harness threads could issue the same prepared statement twice hence uf the prepared
        // statement has already been constructed then simply return.
        if prepared_read_guard.contains_key(prepared_statement)
            && prepared_read_guard
                .get(prepared_statement)
                .unwrap()
                .contains_key(&metadata)
        {
            let stmt_id = prepared_read_guard
                .get(prepared_statement)
                .unwrap()
                .get(&metadata)
                .unwrap();
            return (stmt_id.clone(), self.compute_param_count(&metadata));
        } // Else proceed to add the prepared statement.

        // Data structure does not contain reduced form => encountering this form for the first time.
        // Construct a view.
        // Protocol:
        // 1. Drop the current shared lock
        // 2. Acquire the unique lock
        // 3. Check if the prepared statment has not been added by another thread.
        // 4. If above condition is true then proceed to add the prepared statement.
        drop(prepared_read_guard);
        let mut prepared_write_guard = self.prepared_statements.write().unwrap();
        if prepared_write_guard.contains_key(prepared_statement)
            && prepared_write_guard
                .get(prepared_statement)
                .unwrap()
                .contains_key(&metadata)
        {
            // Some other thread has already added the entry when this thread was busy getting
            // the writer lock.
            let stmt_id = prepared_write_guard
                .get(prepared_statement)
                .unwrap()
                .get(&metadata)
                .unwrap();
            return (stmt_id.clone(), self.compute_param_count(&metadata));
        } // Else proceceed to create the view and add the prepared statement.
        let internal_format: Vec<String> = prepared_statement
            .split("?")
            .map(|s| s.to_string())
            .collect();
        let stmt_id = self.id_generator.fetch_add(1, Ordering::SeqCst);
        let param_count = prepared_statement.matches("?").count() as u32;

        debug!(self.log, "Rust proxy: Prepared as: {:?}", internal_format);

        // Update data structures
        let mut index_write_guard = self.prepared_index.write().unwrap();
        if !prepared_write_guard.contains_key(prepared_statement) {
            prepared_write_guard.insert(prepared_statement.to_string(), HashMap::new());
        }
        prepared_write_guard
            .get_mut(prepared_statement)
            .unwrap()
            .insert(metadata, stmt_id);
        index_write_guard.insert(stmt_id, internal_format);
        return (stmt_id, param_count);
    }
}

impl<W: io::Write> MysqlShim<W> for Backend {
    type Error = io::Error;

    // called when client issues request to prepare query for later execution
    fn on_prepare(
        &mut self,
        prepared_statement: &str,
        info: StatementMetaWriter<W>,
    ) -> io::Result<()> {
        // Filter out supported statements.
        if !prepared_statement.starts_with("SELECT")
            && !prepared_statement.starts_with("INSERT")
            && !prepared_statement.starts_with("REPLACE")
            && !prepared_statement.starts_with("UPDATE")
        {
            error!(
                self.log,
                "Rust proxy: unsupported prepared statement {}", prepared_statement
            );
            return info.error(ErrorKind::ER_INTERNAL_ERROR, &[2]);
        }

        debug!(self.log, "Rust proxy: starting on_prepare");
        debug!(
            self.log,
            "Rust Proxy: prepared statement received is: {:?}", prepared_statement
        );

        let tuple: (u32, u32);
        if prepared_statement.contains(" IN ") {
            let metadata = self.infer_metadata_select(prepared_statement);
            tuple = self.handle_prepared_wherein(prepared_statement, metadata);
        } else if prepared_statement.starts_with("SELECT")
            && !NO_VIEWS.contains(&prepared_statement)
        {
            let metadata = self.infer_metadata_select(prepared_statement);
            tuple = self.handle_prepared_select_needs_view(prepared_statement, metadata);
        } else {
            let metadata = self.infer_metadata_rest(prepared_statement);
            tuple = self.handle_prepared_rest(prepared_statement, metadata);
        }
        let stmt_id = tuple.0;
        let param_count = tuple.1;

        // Construct and return param objects
        let mut params = Vec::new();
        for _i in 0..param_count {
            params.push(Column {
                table: "".to_string(),
                column: "".to_string(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            });
        }
        debug!(
            self.log,
            "[PREPARED] statement: {}, param_count: {}", prepared_statement, param_count
        );
        // Respond to client
        return info.reply(stmt_id, &params, &[]);
    }

    // called when client executes previously prepared statement
    // params: any params included with the client's command:
    // response to query given using QueryResultWriter.
    fn on_execute(
        &mut self,
        stmt_id: u32,
        params: ParamParser,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        debug!(self.log, "Rust proxy: starting on_execute");
        let index_read_guard = self.prepared_index.read().unwrap();
        let wherein_read_guard = self.wherein_view_index.read().unwrap();
        let mut query: String = String::new();
        let mut param_iter = params.into_iter();

        if wherein_read_guard.contains_key(&stmt_id) {
            let (reduced_stmt_id, metadata) = wherein_read_guard.get(&stmt_id).unwrap();
            let tokens = index_read_guard.get(&reduced_stmt_id).unwrap();
            let mut token_counter = 0;
            query.push_str(&tokens[token_counter]);
            token_counter = token_counter + 1;
            for entry in metadata {
                match entry {
                    None => {
                        let param = param_iter.next().unwrap();
                        if param.value.is_null() {
                            query.push_str("NULL");
                        } else {
                            let val = match param.coltype {
                                // If string, surround with \"
                                ColumnType::MYSQL_TYPE_STRING
                                | ColumnType::MYSQL_TYPE_VARCHAR
                                | ColumnType::MYSQL_TYPE_VAR_STRING => {
                                    // NOTE: For Lobsters harness, remove these single quotes.
                                    format!("\'{}\'", Into::<&str>::into(param.value))
                                }
                                ColumnType::MYSQL_TYPE_DECIMAL
                                | ColumnType::MYSQL_TYPE_NEWDECIMAL
                                | ColumnType::MYSQL_TYPE_TINY
                                | ColumnType::MYSQL_TYPE_SHORT
                                | ColumnType::MYSQL_TYPE_INT24
                                | ColumnType::MYSQL_TYPE_LONG
                                | ColumnType::MYSQL_TYPE_LONGLONG => match param.value.into_inner()
                                {
                                    ValueInner::Int(v) => v.to_string(),
                                    ValueInner::UInt(v) => v.to_string(),
                                    _ => unimplemented!("Rust proxy: unsupported numeric type"),
                                },
                                _ => unimplemented!("Rust proxy: unsupported parameter type"),
                            };
                            query.push_str(&val);
                        }
                    }
                    Some(count) => {
                        // let param = param_iter.next().unwrap();
                        // Replace the first "= ?" with " IN "
                        query = query[0..query.len() - 2].to_string();
                        query.push_str(" IN ");
                        query.push_str("(");
                        // Just add all the remaining parameters as usual
                        for _i in 0..count.clone() {
                            let param = param_iter.next().unwrap();
                            if param.value.is_null() {
                                query.push_str("NULL");
                            } else {
                                let val = match param.coltype {
                                    // If string, surround with \"
                                    ColumnType::MYSQL_TYPE_STRING
                                    | ColumnType::MYSQL_TYPE_VARCHAR
                                    | ColumnType::MYSQL_TYPE_VAR_STRING => {
                                        // NOTE: For Lobsters harness, remove these single quotes.
                                        format!("\'{}\'", Into::<&str>::into(param.value))
                                    }
                                    ColumnType::MYSQL_TYPE_DECIMAL
                                    | ColumnType::MYSQL_TYPE_NEWDECIMAL
                                    | ColumnType::MYSQL_TYPE_TINY
                                    | ColumnType::MYSQL_TYPE_SHORT
                                    | ColumnType::MYSQL_TYPE_INT24
                                    | ColumnType::MYSQL_TYPE_LONG
                                    | ColumnType::MYSQL_TYPE_LONGLONG => match param
                                        .value
                                        .into_inner()
                                    {
                                        ValueInner::Int(v) => v.to_string(),
                                        ValueInner::UInt(v) => v.to_string(),
                                        _ => unimplemented!("Rust proxy: unsupported numeric type"),
                                    },
                                    _ => unimplemented!("Rust proxy: unsupported parameter type"),
                                };
                                query.push_str(&val);
                            }
                            query.push_str(", ");
                        }
                        // Remove last ","
                        query = query[0..query.len() - 2].to_string();
                        // Terminate the IN clause
                        query.push_str(")");
                    }
                }
                query.push_str(&tokens[token_counter]);
                token_counter = token_counter + 1;
            }
            query.push_str(tokens.last().unwrap());
        } else {
            // No WHERE IN clause, hence there will be a 1 to 1 match with param values and
            // ?marks in the prepared statement.
            let tokens = index_read_guard.get(&stmt_id).unwrap();
            for i in 0..tokens.len() - 1 {
                query.push_str(&tokens[i]);
                let param = param_iter.next().unwrap();
                if param.value.is_null() {
                    query.push_str("NULL");
                } else {
                    let val = match param.coltype {
                        // If string, surround with \"
                        ColumnType::MYSQL_TYPE_STRING
                        | ColumnType::MYSQL_TYPE_VARCHAR
                        | ColumnType::MYSQL_TYPE_VAR_STRING => {
                            // NOTE: For Lobsters harness, remove these single quotes.
                            format!("\'{}\'", Into::<&str>::into(param.value))
                        }
                        ColumnType::MYSQL_TYPE_DECIMAL
                        | ColumnType::MYSQL_TYPE_NEWDECIMAL
                        | ColumnType::MYSQL_TYPE_TINY
                        | ColumnType::MYSQL_TYPE_SHORT
                        | ColumnType::MYSQL_TYPE_INT24
                        | ColumnType::MYSQL_TYPE_LONG
                        | ColumnType::MYSQL_TYPE_LONGLONG => match param.value.into_inner() {
                            ValueInner::Int(v) => v.to_string(),
                            ValueInner::UInt(v) => v.to_string(),
                            _ => unimplemented!("Rust proxy: unsupported numeric type"),
                        },
                        _ => unimplemented!("Rust proxy: unsupported parameter type"),
                    };
                    query.push_str(&val);
                }
            }
            query.push_str(tokens.last().unwrap());
        }

        drop(index_read_guard);
        drop(wherein_read_guard);
        self.on_query(&query, results)
    }

    // called when client wants to deallocate resources associated with a prev prepared statement
    fn on_close(&mut self, _: u32) {
        debug!(self.log, "Rust proxy: starting on_close");
    }

    // called when client switches databases
    fn on_init(&mut self, _: &str, writer: InitWriter<W>) -> io::Result<()> {
        // first statement in sql would be `use db` => acknowledgement
        debug!(self.log, "Rust proxy: starting on_init");
        // Tell client that database context has been changed
        // TODO(babman, benji): Figure out the DB name from the `use db`
        // statement and pass that to pelton.
        writer.ok()
    }

    // called when client issues query for immediate execution. Results returned via QueryResultWriter
    fn on_query(&mut self, q_string: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        // start measuring runtime
        let start = Instant::now();

        debug!(self.log, "Rust proxy: starting on_query");
        debug!(self.log, "Rust Proxy: query received is: {:?}", q_string);

        if q_string.starts_with("SET STOP") {
            self.stop.store(true, Ordering::Relaxed);
            results.completed(0, 0)
        } else if q_string.starts_with("SHOW VARIABLES") {
            // Hardcoded SHOW variables needed for mariadb java connectors.
            debug!(
                self.log,
                "Rust Proxy: SHOW statement simulated {}", q_string
            );

            let mut cols: Vec<Column> = Vec::with_capacity(2);
            cols.push(Column {
                table: "".to_string(),
                column: "Variable_name".to_string(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            });
            cols.push(Column {
                table: "".to_string(),
                column: "Value".to_string(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            });
            let mut rw = results.start(&cols)?;
            rw.write_col("auto_increment_increment")?;
            rw.write_col("1")?;
            rw.end_row()?;
            rw.write_col("max_allowed_packet")?;
            rw.write_col("16777216")?;
            rw.end_row()?;
            rw.write_col("system_time_zone")?;
            rw.write_col("EST")?;
            rw.end_row()?;
            rw.write_col("time_zone")?;
            rw.write_col("SYSTEM")?;
            rw.end_row()?;
            return rw.finish();
        } else if q_string.starts_with("CREATE TABLE")
            || q_string.starts_with("CREATE INDEX")
            || q_string.starts_with("CREATE VIEW")
            || q_string.starts_with("SET")
        {
            let ddl_response = exec_ddl_ffi(&mut self.rust_conn, q_string);
            debug!(self.log, "ddl_response is {:?}", ddl_response);

            // stop measuring runtime and add to total time for this connection
            self.runtime = self.runtime + start.elapsed();

            if ddl_response {
                results.completed(0, 0)
            } else {
                error!(self.log, "Rust Proxy: Failed to execute CREATE");
                results.error(ErrorKind::ER_INTERNAL_ERROR, &[2])
            }
        } else if q_string.starts_with("UPDATE")
            || q_string.starts_with("DELETE")
            || q_string.starts_with("INSERT")
            || q_string.starts_with("REPLACE")
            || q_string.starts_with("GDPR FORGET")
        {
            let update_response = exec_update_ffi(&mut self.rust_conn, q_string);

            // stop measuring runtime and add to total time for this connection
            self.runtime = self.runtime + start.elapsed();

            if update_response != -1 {
                results.completed(update_response as u64, 0)
            } else {
                error!(
                    self.log,
                    "Rust Proxy: Failed to execute UPDATE: {:?}", q_string
                );
                results.error(ErrorKind::ER_INTERNAL_ERROR, &[2])
            }
        } else if q_string.starts_with("SELECT")
            || q_string.starts_with("GDPR GET")
            || q_string.starts_with("SHOW SHARDS")
            || q_string.starts_with("SHOW VIEW")
            || q_string.starts_with("SHOW MEMORY")
        {
            let select_response = exec_select_ffi(&mut self.rust_conn, q_string);

            // stop measuring runtime and add to total time for this connection
            self.runtime = self.runtime + start.elapsed();

            let num_cols = unsafe { (*select_response).num_cols as usize };
            let num_rows = unsafe { (*select_response).num_rows as usize };
            let col_names = unsafe { (*select_response).col_names };
            let col_types = unsafe { (*select_response).col_types };

            debug!(
                self.log,
                "Rust Proxy: converting response schema to rust compatible types"
            );
            let cols = convert_columns(num_cols, col_types, col_names);
            let mut rw = results.start(&cols)?;

            debug!(
                self.log,
                "Rust Proxy: writing query response using RowWriter"
            );
            // convert incomplete array field (flexible array) to rust slice, starting with the outer array
            let rows_slice = unsafe { (*select_response).records.as_slice(num_rows * num_cols) };
            for r in 0..num_rows {
                for c in 0..num_cols {
                    if rows_slice[r * num_cols + c].is_null {
                        rw.write_col(None::<i32>)?
                    } else {
                        match col_types[c] {
                            // write a value to the next col of the current row (of this resultset)
                            FFIColumnType_UINT => unsafe {
                                rw.write_col(rows_slice[r * num_cols + c].record.UINT as i64)?
                            },
                            FFIColumnType_INT => unsafe {
                                rw.write_col(rows_slice[r * num_cols + c].record.INT)?
                            },
                            FFIColumnType_TEXT => unsafe {
                                rw.write_col(
                                    CString::from_raw(rows_slice[r * num_cols + c].record.TEXT)
                                        .into_string()
                                        .unwrap(),
                                )?
                            },
                            FFIColumnType_DATETIME => unsafe {
                                rw.write_col(
                                    CString::from_raw(rows_slice[r * num_cols + c].record.DATETIME)
                                        .into_string()
                                        .unwrap(),
                                )?
                            },
                            _ => error!(self.log, "Rust Proxy: Invalid column type"),
                        }
                    }
                }
                rw.end_row()?;
            }
            // call destructor for CResult (after results are written/copied via row writer)
            unsafe { std::ptr::drop_in_place(select_response) };

            // tell client no more rows coming. Returns an empty Ok to the proxy
            rw.finish()
        } else if q_string.starts_with("set") {
            debug!(self.log, "Ignoring set query {}", q_string);
            results.completed(0, 0)
        } else {
            error!(self.log, "Rust proxy: unsupported query type {}", q_string);
            results.error(ErrorKind::ER_INTERNAL_ERROR, &[2])
        }
    }
}

// destructor to close connection when Backend goes out of scope
impl Drop for Backend {
    fn drop(&mut self) {
        debug!(self.log, "Rust Proxy: Starting destructor for Backend");
        debug!(self.log, "Rust Proxy: Calling c-wrapper for pelton::close");
        info!(
            self.log,
            "Total time elapsed for this connection is: {:?}", self.runtime
        );
        let close_response: bool = close_ffi(&mut self.rust_conn);
        if close_response {
            debug!(self.log, "Rust Proxy: successfully closed connection");
        } else {
            debug!(self.log, "Rust Proxy: failed to close connection");
        }
    }
}

fn main() {
    // initialize rust logging
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let log: slog::Logger =
        slog::Logger::root(slog_term::FullFormat::new(plain).build().fuse(), o!());

    // Parse command line flags defined using rust's gflags.
    let flags = gflags_ffi(std::env::args(), USAGE);
    info!(
        log,
        "Rust proxy: running with args: {:?} {:?} {:?}",
        flags.workers,
        flags.consistent,
        flags.db_name
    );

    let global_open = initialize_ffi(flags.workers, flags.consistent);
    info!(
        log,
        "Rust Proxy: opening connection globally: {:?}", global_open
    );

    let listener = net::TcpListener::bind("127.0.0.1:10001").unwrap();
    info!(log, "Rust Proxy: Listening at: {:?}", listener);
    // set listener to non-blocking mode
    listener
        .set_nonblocking(true)
        .expect("Cannot set non-blocking");
    let stop = Arc::new(AtomicBool::new(false));

    // store client thread handles
    let mut threads = Vec::new();

    // detect listener status via shared boolean
    let stop_clone = Arc::clone(&stop);
    // detect SIGTERM to close listener
    let log_ctrlc = log.clone();
    ctrlc::set_handler(move || {
        info!(
            log_ctrlc,
            "Rust Proxy: received SIGTERM! Terminating listener once all client connections have closed..."
        );
        stop_clone.store(true, Ordering::Relaxed);
        debug!(
            log_ctrlc,
            "Rust Proxy: listener_terminated"
        );
    })
    .expect("Error setting Ctrl-C handler");

    let prepared_statements_lock = Arc::new(RwLock::new(HashMap::new()));
    let prepared_index_lock = Arc::new(RwLock::new(HashMap::new()));
    let wherein_view_index = Arc::new(RwLock::new(HashMap::new()));
    let id_generator = Arc::new(AtomicU32::new(0));
    // run listener until terminated with SIGTERM
    while !stop.load(Ordering::Relaxed) {
        while let Ok((stream, _addr)) = listener.accept() {
            let db_name = flags.db_name.clone();
            // clone log so that each client thread has an owned copy
            let log_client = log.clone();
            let stop_clone = stop.clone();
            let prepared_statements_clone = prepared_statements_lock.clone();
            let prepared_index_clone = prepared_index_lock.clone();
            let wherein_view_index_clone = wherein_view_index.clone();
            let id_generrator_clone = id_generator.clone();
            threads.push(std::thread::spawn(move || {
                info!(
                    log_client,
                    "Rust Proxy: Successfully connected to mysql proxy\nStream and address are: {:?}",
                    stream
                );
                let rust_conn = open_ffi(&db_name);
                info!(
                    log_client,
                    "Rust Proxy: connection status is: {:?}", rust_conn.connected
                );
                let backend = Backend {
                    rust_conn: rust_conn,
                    runtime: Duration::new(0, 0),
                    log: log_client,
                    stop: stop_clone,
                    prepared_statements: prepared_statements_clone,
                    prepared_index: prepared_index_clone,
                    wherein_view_index: wherein_view_index_clone,
                    id_generator: id_generrator_clone,
                };
                let _inter = MysqlIntermediary::run_on_tcp(backend, stream).unwrap();
            }));
        }
        // wait before checking listener status
        std::thread::sleep(Duration::from_secs(1));
    }

    // join all client threads
    for join_handle in threads {
        join_handle.join().unwrap();
    }
    let global_close = shutdown_ffi();
    info!(
        log,
        "Rust Proxy: Shutting down pelton, clearing global state: {:?}", global_close
    );
}
