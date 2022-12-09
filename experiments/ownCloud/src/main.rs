#[macro_use]
extern crate clap;

use rand::Rng;
use std::io::Write;
use std::time::{Duration, Instant};
use std::collections::HashMap;

// src/*.rs
mod backend;
mod db;
mod generator;
mod models;
mod workload;
use backend::Backend;
use generator::GeneratorState;
use workload::{WorkloadGenerator, ZipfF};

pub struct Args {
  pub num_users: usize,
  pub files_per_user: usize,
  pub direct_shares_per_file: usize,
  pub group_shares_per_file: usize,
  pub users_per_group: usize,
  pub backend: String,
  pub in_size: usize,
  pub outfile: Option<String>,
  pub operations: usize,
  pub perf: bool,
  pub warmup: bool,
  pub distr: Option<ZipfF>,
}

fn main() {
  // Add distributions later
  let matches =
            clap_app!((crate_name!()) =>
                (version: (crate_version!()))
                (author: (crate_authors!()))
                (@arg num_users: --("num-users") ... +required +takes_value "Number of users (> 1)")
                (@arg files_per_user: --("files-per-user") +takes_value "Avg files per user (>1)")
                (@arg direct_shares_per_file: --("direct-shares-per-file") +takes_value "Avg direct shares per file")
                (@arg group_shares_per_file: --("group-shares-per-file") +takes_value "Avg group shares per file")
                (@arg users_per_group: --("users-per-group") +takes_value "Avg users per group")
                (@arg backend: --backend ... +required +takes_value "Backend type to use")
                (@arg in_size: --in_size +takes_value "Number of values in an IN clauses")
                (@arg outfile: -o --out +takes_value "File for writing output to (defaults to stdout")
                (@arg operations: --operations +takes_value "Number of operations in load")
                (@arg perf: --perf "Wait for user input before starting workload to attach perf")
                (@arg warmup: --warmup "Warmup the cache!")
                (@arg distr: --zipf [s] "Use zipf distribution with a frequency rank exponent of 's' (default to uniform)")
        ).get_matches();

  // Parse command line arguments.
  let args = Args {
    files_per_user: value_t!(matches, "files_per_user", usize).unwrap_or(1),
    direct_shares_per_file: value_t!(matches, "direct_shares_per_file", usize)
      .unwrap_or(1),
    group_shares_per_file: value_t!(matches, "group_shares_per_file", usize)
      .unwrap_or(1),
    users_per_group: value_t!(matches, "users_per_group", usize).unwrap_or(1),
    num_users: value_t_or_exit!(matches, "num_users", usize),
    backend: matches.value_of("backend").map(&str::to_string).unwrap(),
    in_size: value_t!(matches, "in_size", usize).unwrap_or(1),
    outfile: matches.value_of("outfile").map(&str::to_string),
    operations: value_t_or_exit!(matches, "operations", usize),
    perf: matches.is_present("perf"),
    warmup: matches.is_present("warmup"),
    distr: value_t!(matches, "distr", ZipfF).ok(),
  };

  // Output file.
  let mut f = if let Some(fname) = &args.outfile {
    Box::new(std::fs::File::create(fname).unwrap()) as Box<dyn Write>
  } else {
    Box::new(std::io::stdout()) as Box<dyn Write>
  };

  // Generate priming data.
  let mut st = GeneratorState::new();
  let users = st.generate_users(args.num_users);
  let files = st.generate_files(&users, args.files_per_user);
  let direct_shares =
    st.generate_direct_shares(&users, &files, args.direct_shares_per_file);
  let groups = st.generate_groups(&users, args.users_per_group);
  let group_shares =
    st.generate_group_shares(&groups, &files, args.group_shares_per_file);

  // Map user at index k to group at index v.
  let mut user_to_group_map: HashMap<usize, usize> = HashMap::new();
  let mut index_map: HashMap<String, usize> = HashMap::new();
  for (i, u) in users.iter().enumerate() {
    index_map.insert(u.uid.clone(), i);
  }
  for (gi, g) in groups.iter().enumerate() {
    for (_, uid) in &g.users {
      let i = index_map[uid];
      user_to_group_map.insert(i, gi);
    }
  }

  // Parameters.
  let in_size = args.in_size;
  let operations = args.operations;

  // Run the experiment for each provided backend.
  let mut backend = Backend::from_str(&args.backend);
  eprintln!("--> Starting backend {}", backend);

  // Insert load (priming).
  let instant = Instant::now();
  backend.insert_users(users.clone());
  backend.insert_groups(groups.clone());
  backend.insert_files(files.clone());
  backend.insert_shares(direct_shares.clone());
  backend.insert_shares(group_shares.clone());
  eprintln!("--> Priming done in {}ms", instant.elapsed().as_millis());

  // Wait for user input.
  if args.perf {
    eprintln!("# perf provided: waiting for user input before executing load");
    std::io::stdin().read_line(&mut String::new());
  }

  // Create a generator workload.
  let mut workload = WorkloadGenerator::new(st, args.distr.unwrap_or(0.0) /* s = 0 is uniform distr */);

  // Warmup.
  if args.warmup {
    let instant = Instant::now();
    backend.warmup();
    eprintln!("--> Warmup done in {}ms", instant.elapsed().as_millis());
  }

  // Run actual load.
  let mut last_write = 0; // 0 is direct, 1 is indirect, 2 is get file by pk
  let mut last_read_user = false;
  let mut reads = Vec::<u128>::new();
  let mut dwrites = Vec::<u128>::new();
  let mut gwrites = Vec::<u128>::new();
  let mut read_file_pk = Vec::<u128>::new();
  let mut update_file_pk = Vec::<u128>::new();

  let mut next_write = 0;
  for i in 0..(operations/3) {
    // Issue a direct/indirect write and a update by pk.
    if next_write % 3 == 0 {
      // do direct
      let request = workload.make_direct_share(&users, &files);
      dwrites.push(backend.run(&request));
    } else if next_write % 3 == 1 {
      // do indirect
      let request = workload.make_group_share(&users, &groups, &files, &user_to_group_map);
        gwrites.push(backend.run(&request));
    } else {
      // do update file by pk
      let request = workload.make_update_file_pk(&files);
      update_file_pk.push(backend.run(&request));  
    }
    next_write += 1;
    
    // read all of a user's files
    last_read_user = true;
    let request = workload.make_read(in_size, &users);
    reads.push(backend.run(&request));
    
    // read a specific file by pk
    last_read_user = false;
    let request = workload.make_get_file_pk(in_size, &files);
    read_file_pk.push(backend.run(&request));
  }

  // Report medians and tails.
  if reads.len() > 0 {
    reads.sort();
    writeln!(f, "Read: {}", reads.len());
    writeln!(f, "Read [50]: {}", reads[reads.len() / 2]);
    writeln!(f, "Read [90]: {}", reads[reads.len() * 90 / 100]);
    writeln!(f, "Read [95]: {}", reads[reads.len() * 95 / 100]);
    writeln!(f, "Read [99]: {}", reads[reads.len() * 99 / 100]);
  }

  if dwrites.len() > 0 {
    dwrites.sort();
    writeln!(f, "Direct: {}", dwrites.len());
    writeln!(f, "Direct [50]: {}", dwrites[dwrites.len() / 2]);
    writeln!(f, "Direct [90]: {}", dwrites[dwrites.len() * 90 / 100]);
    writeln!(f, "Direct [95]: {}", dwrites[dwrites.len() * 95 / 100]);
    writeln!(f, "Direct [99]: {}", dwrites[dwrites.len() * 99 / 100]);
  }

  if gwrites.len() > 0 {
    gwrites.sort();
    writeln!(f, "Group: {}", gwrites.len());
    writeln!(f, "Group [50]: {}", gwrites[gwrites.len() / 2]);
    writeln!(f, "Group [90]: {}", gwrites[gwrites.len() * 90 / 100]);
    writeln!(f, "Group [95]: {}", gwrites[gwrites.len() * 95 / 100]);
    writeln!(f, "Group [99]: {}", gwrites[gwrites.len() * 99 / 100]);
  }

  if read_file_pk.len() > 0 {
    read_file_pk.sort();
    writeln!(f, "Read File PK: {}", read_file_pk.len());
    writeln!(f, "Read File PK [50]: {}", read_file_pk[read_file_pk.len() / 2]);
    writeln!(f, "Read File PK [90]: {}", read_file_pk[read_file_pk.len() * 90 / 100]);
    writeln!(f, "Read File PK [95]: {}", read_file_pk[read_file_pk.len() * 95 / 100]);
    writeln!(f, "Read File PK [99]: {}", read_file_pk[read_file_pk.len() * 99 / 100]);
  }

  if update_file_pk.len() > 0 {
    update_file_pk.sort();
    writeln!(f, "Update File PK: {}", update_file_pk.len());
    writeln!(f, "Update File PK [50]: {}", update_file_pk[update_file_pk.len() / 2]);
    writeln!(f, "Update File PK [90]: {}", update_file_pk[update_file_pk.len() * 90 / 100]);
    writeln!(f, "Update File PK [95]: {}", update_file_pk[update_file_pk.len() * 95 / 100]);
    writeln!(f, "Update File PK [99]: {}", update_file_pk[update_file_pk.len() * 99 / 100]);
  }
}
