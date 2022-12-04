extern crate memcached;
extern crate mysql;
use crate::backend::mysql::prelude::Queryable;

// src/db.rs
use super::db::{mariadb_connect, memcached_connect, pelton_connect};
// src/models.rs
use super::models::{File, Group, Share, User};
// src/workload.rs
use super::workload::Request;

// src/backend/{sql,hybrid}.rs
mod hybrid;
mod mariadb;
mod pelton;
mod simulate;

// Backend enum
pub enum Backend {
  Pelton(mysql::Conn),
  MariaDB(mysql::Conn),
  Memcached(mysql::Conn, memcached::client::Client),
  Simulate(simulate::SimulatedState),
}

// Functions for executing load.
impl Backend {
  // Shorthands to check type of backend.
  pub fn is_pelton(&self) -> bool {
    match self {
      Backend::Pelton(_) => true,
      _ => false,
    }
  }
  pub fn is_mariadb(&self) -> bool {
    match self {
      Backend::MariaDB(_) => true,
      _ => false,
    }
  }
  pub fn is_memcached(&self) -> bool {
    match self {
      Backend::Memcached(_, _) => true,
      _ => false,
    }
  }
  pub fn is_simulate(&self) -> bool {
    match self {
      Backend::Simulate(_) => true,
      _ => false,
    }
  }
  // Insert data (for priming).
  pub fn insert_user(&mut self, user: &User) {
    match self {
      Backend::Pelton(conn) => pelton::insert_user(conn, user),
      Backend::MariaDB(conn) => mariadb::insert_user(conn, user),
      Backend::Memcached(conn, cl) => hybrid::insert_user(conn, cl, user),
      Backend::Simulate(conn) => simulate::insert_user(conn, user),
    }
  }
  pub fn insert_group<'a>(&mut self, group: &Group<'a>) {
    match self {
      Backend::Pelton(conn) => pelton::insert_group(conn, group),
      Backend::MariaDB(conn) => mariadb::insert_group(conn, group),
      Backend::Memcached(conn, cl) => hybrid::insert_group(conn, cl, group),
      Backend::Simulate(conn) => simulate::insert_group(conn, group),
    }
  }
  pub fn insert_file<'a>(&mut self, file: &File<'a>) {
    match self {
      Backend::Pelton(conn) => pelton::insert_file(conn, file),
      Backend::MariaDB(conn) => mariadb::insert_file(conn, file),
      Backend::Memcached(conn, cl) => hybrid::insert_file(conn, cl, file),
      Backend::Simulate(conn) => simulate::insert_file(conn, file),
    }
  }
  pub fn insert_share<'a>(&mut self, share: &Share<'a>) {
    match self {
      Backend::Pelton(conn) => pelton::insert_share(conn, share),
      Backend::MariaDB(conn) => mariadb::insert_share(conn, share),
      Backend::Memcached(conn, cl) => hybrid::insert_share(conn, cl, share),
      Backend::Simulate(conn) => simulate::insert_share(conn, share),
    }
  }

  // Execute a workload.
  pub fn run(&mut self, request: &Request) -> u128 {
    match self {
      Backend::Pelton(conn) => match request {
        Request::Read(load, expected) => pelton::reads(conn, load, expected),
        Request::Direct(load) => pelton::direct(conn, load),
        Request::Indirect(load) => pelton::indirect(conn, load),
        Request::GetFilePK(load) => pelton::read_file_pk(conn, load),
        Request::UpdateFilePK(load, new_fn) => pelton::update_file_pk(conn, load, new_fn.to_string()),
      },
      Backend::MariaDB(conn) => match request {
        Request::Read(load, expected) => mariadb::reads(conn, load, expected),
        Request::Direct(load) => mariadb::direct(conn, load),
        Request::Indirect(load) => mariadb::indirect(conn, load),
        Request::GetFilePK(load) => mariadb::read_file_pk(conn, load),
        Request::UpdateFilePK(load, new_fn) => mariadb::update_file_pk(conn, load, new_fn.to_string()),
      },
      Backend::Memcached(conn, client) => match request {
        Request::Read(load, expected) => {
          hybrid::reads(conn, client, load, expected)
        }
        Request::Direct(load) => hybrid::direct(conn, client, load),
        Request::Indirect(load) => hybrid::indirect(conn, client, load),
        Request::GetFilePK(load) => hybrid::read_file_pk(conn, load),
        Request::UpdateFilePK(load, new_fn) => hybrid::update_file_pk(conn, load, new_fn.to_string()),
      },
      Backend::Simulate(conn) => match request {
        Request::Read(load, expected) => simulate::reads(conn, load, expected),
        Request::Direct(load) => simulate::direct(conn, load),
        Request::Indirect(load) => simulate::indirect(conn, load),
        Request::GetFilePK(load) => simulate::read_file_pk(conn, load),
        Request::UpdateFilePK(load, new_fn) => simulate::update_file_pk(conn, load, new_fn.to_string()),
      },
    }
  }

  pub fn execute(&mut self, sql: &str) {
    match self {
      Backend::Pelton(conn) => conn.query_drop(sql).unwrap(),
      Backend::MariaDB(conn) => conn.query_drop(sql).unwrap(),
      Backend::Memcached(conn, _) => conn.query_drop(sql).unwrap(),
      Backend::Simulate(_) => {},
    }
  }

  // Parse backend from string.
  pub fn from_str(s: &str, views: bool, accessors: bool, echo: bool) -> Self {
    match s {
      "pelton" => Backend::Pelton(pelton_connect(views, accessors, echo)),
      "mariadb" => Backend::MariaDB(mariadb_connect()),
      "memcached" => Backend::Memcached(mariadb_connect(), memcached_connect()),
      "simulate" => Backend::Simulate(simulate::SimulatedState::new()),
      _ => panic!("Unknown backend {}", s),
    }
  }
}

// Display backend as string.
impl std::fmt::Display for Backend {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.write_str(match self {
      Backend::Pelton(_) => "pelton",
      Backend::MariaDB(_) => "mariadb",
      Backend::Memcached(_, _) => "memcached",
      Backend::Simulate(_) => "simulate",
    })
  }
}
