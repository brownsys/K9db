extern crate mysql;
use mysql::{Conn, OptsBuilder};

// src/db.rs
use super::db::{mariadb_connect, pelton_connect};
// src/models.rs
use super::models::{File, Group, Share, User};
// src/workload.rs
use super::workload::Request;

// src/backend/{sql,hybrid}.rs
mod hybrid;
mod sql;

// Backend enum
#[derive(Debug)]
pub enum Backend {
  Pelton(mysql::Conn),
  MariaDB(mysql::Conn),
  Memcached(mysql::Conn),
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
      Backend::Memcached(_) => true,
      _ => false,
    }
  }
  // Insert data (for priming).
  pub fn insert_user(&mut self, user: &User) {
    match self {
      Backend::Pelton(conn) => sql::insert_user(conn, user),
      Backend::MariaDB(conn) => sql::insert_user(conn, user),
      Backend::Memcached(conn) => sql::insert_user(conn, user),
    }
  }
  pub fn insert_group<'a>(&mut self, group: &Group<'a>) {
    match self {
      Backend::Pelton(conn) => sql::insert_group(conn, group),
      Backend::MariaDB(conn) => sql::insert_group(conn, group),
      Backend::Memcached(conn) => sql::insert_group(conn, group),
    }
  }
  pub fn insert_file<'a>(&mut self, file: &File<'a>) {
    match self {
      Backend::Pelton(conn) => sql::insert_file(conn, file),
      Backend::MariaDB(conn) => sql::insert_file(conn, file),
      Backend::Memcached(conn) => sql::insert_file(conn, file),
    }
  }
  pub fn insert_share<'a>(&mut self, share: &Share<'a>) {
    match self {
      Backend::Pelton(conn) => sql::insert_share(conn, share),
      Backend::MariaDB(conn) => sql::insert_share(conn, share),
      Backend::Memcached(conn) => sql::insert_share(conn, share),
    }
  }

  // Execute a workload.
  pub fn run(&mut self, request: &Request) -> u128 {
    match (self, request) {
      (Backend::Pelton(conn), Request::Read(load)) => sql::reads(conn, load),
      (Backend::MariaDB(conn), Request::Read(load)) => sql::reads(conn, load),
      (Backend::Memcached(conn), Request::Read(load)) => sql::reads(conn, load),
      (Backend::Pelton(conn), Request::Direct(load)) => sql::direct(conn, load),
      (Backend::MariaDB(conn), Request::Direct(load)) => {
        sql::direct(conn, load)
      }
      (Backend::Memcached(conn), Request::Direct(load)) => {
        sql::direct(conn, load)
      }
      (Backend::Pelton(conn), Request::Indirect(load)) => {
        sql::indirect(conn, load)
      }
      (Backend::MariaDB(conn), Request::Indirect(load)) => {
        sql::indirect(conn, load)
      }
      (Backend::Memcached(conn), Request::Indirect(load)) => {
        sql::indirect(conn, load)
      }
    }
  }

  // Parse backend from string.
  pub fn from_str(s: &str) -> Self {
    match s {
      "pelton" => Backend::Pelton(pelton_connect()),
      "mariadb" => Backend::MariaDB(mariadb_connect()),
      "memcached" => Backend::Memcached(mariadb_connect()),
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
                  Backend::Memcached(_) => "memcached",
                })
  }
}
