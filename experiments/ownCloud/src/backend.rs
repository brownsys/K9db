extern crate memcached;
extern crate mysql;

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

// Backend enum
pub enum Backend {
  Pelton(mysql::Conn),
  MariaDB(mysql::Conn),
  Memcached(mysql::Conn, memcached::client::Client),
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
  // Insert data (for priming).
  pub fn insert_user(&mut self, user: &User) {
    match self {
      Backend::Pelton(conn) => pelton::insert_user(conn, user),
      Backend::MariaDB(conn) => mariadb::insert_user(conn, user),
      Backend::Memcached(conn, cl) => hybrid::insert_user(conn, cl, user),
    }
  }
  pub fn insert_group<'a>(&mut self, group: &Group<'a>) {
    match self {
      Backend::Pelton(conn) => pelton::insert_group(conn, group),
      Backend::MariaDB(conn) => mariadb::insert_group(conn, group),
      Backend::Memcached(conn, cl) => hybrid::insert_group(conn, cl, group),
    }
  }
  pub fn insert_file<'a>(&mut self, file: &File<'a>) {
    match self {
      Backend::Pelton(conn) => pelton::insert_file(conn, file),
      Backend::MariaDB(conn) => mariadb::insert_file(conn, file),
      Backend::Memcached(conn, cl) => hybrid::insert_file(conn, cl, file),
    }
  }
  pub fn insert_share<'a>(&mut self, share: &Share<'a>) {
    match self {
      Backend::Pelton(conn) => pelton::insert_share(conn, share),
      Backend::MariaDB(conn) => mariadb::insert_share(conn, share),
      Backend::Memcached(conn, cl) => hybrid::insert_share(conn, cl, share),
    }
  }

  // Execute a workload.
  pub fn run(&mut self, request: &Request) -> u128 {
    match self {
      Backend::Pelton(conn) => match request {
        Request::Read(load, expected) => pelton::reads(conn, load, expected),
        Request::Direct(load) => pelton::direct(conn, load),
        Request::Indirect(load) => pelton::indirect(conn, load),
      },
      Backend::MariaDB(conn) => match request {
        Request::Read(load, expected) => mariadb::reads(conn, load, expected),
        Request::Direct(load) => mariadb::direct(conn, load),
        Request::Indirect(load) => mariadb::indirect(conn, load),
      },
      Backend::Memcached(conn, client) => match request {
        Request::Read(load, expected) => {
          hybrid::reads(conn, client, load, expected)
        }
        Request::Direct(load) => hybrid::direct(conn, client, load),
        Request::Indirect(load) => hybrid::indirect(conn, client, load),
      },
    }
  }

  // Parse backend from string.
  pub fn from_str(s: &str) -> Self {
    match s {
      "pelton" => Backend::Pelton(pelton_connect()),
      "mariadb" => Backend::MariaDB(mariadb_connect()),
      "memcached" => Backend::Memcached(mariadb_connect(), memcached_connect()),
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
    })
  }
}
