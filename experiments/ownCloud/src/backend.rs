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
  pub fn insert_users(&mut self, users: Vec<User>) {
    match self {
      Backend::Pelton(conn) => pelton::insert_users(conn, users),
      Backend::MariaDB(conn) => mariadb::insert_users(conn, users),
      Backend::Memcached(conn, cl) => hybrid::insert_users(conn, cl, users),
      Backend::Simulate(conn) => simulate::insert_users(conn, users),
    }
  }
  pub fn insert_groups(&mut self, groups: Vec<Group>) {
    match self {
      Backend::Pelton(conn) => pelton::insert_groups(conn, groups),
      Backend::MariaDB(conn) => mariadb::insert_groups(conn, groups),
      Backend::Memcached(conn, cl) => hybrid::insert_groups(conn, cl, groups),
      Backend::Simulate(conn) => simulate::insert_groups(conn, groups),
    }
  }
  pub fn insert_files(&mut self, files: Vec<File>) {
    match self {
      Backend::Pelton(conn) => pelton::insert_files(conn, files),
      Backend::MariaDB(conn) => mariadb::insert_files(conn, files),
      Backend::Memcached(conn, cl) => hybrid::insert_files(conn, cl, files),
      Backend::Simulate(conn) => simulate::insert_files(conn, files),
    }
  }
  pub fn insert_shares(&mut self, shares: Vec<Share>) {
    match self {
      Backend::Pelton(conn) => pelton::insert_shares(conn, shares),
      Backend::MariaDB(conn) => mariadb::insert_shares(conn, shares),
      Backend::Memcached(conn, cl) => hybrid::insert_shares(conn, cl, shares),
      Backend::Simulate(conn) => simulate::insert_shares(conn, shares),
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

  pub fn warmup(&mut self) {
    match self {
      Backend::Pelton(conn) => {},
      Backend::MariaDB(conn) => {},
      Backend::Memcached(conn, cl) => hybrid::warmup(conn, cl),
      Backend::Simulate(conn) => {},
    }
  }

  // Parse backend from string.
  pub fn from_str(s: &str, ip: &str) -> Self {
    match s {
      "pelton" => Backend::Pelton(pelton_connect(ip)),
      "mariadb" => Backend::MariaDB(mariadb_connect(ip)),
      "memcached" => Backend::Memcached(mariadb_connect(ip), memcached_connect()),
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
