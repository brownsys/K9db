#[macro_use]
extern crate clap;

use rand::Rng;
use std::io::Write;
use std::time::{Duration, Instant};

// src/*.rs
mod backend;
mod db;
mod generator;
mod models;
mod workload;
use backend::Backend;
use generator::GeneratorState;

pub struct Args {
  pub num_users: usize,
  pub files_per_user: usize,
  pub direct_shares_per_file: usize,
  pub group_shares_per_file: usize,
  pub users_per_group: usize,
  pub backend: String,
  pub in_size: usize,
  pub outfile: Option<String>,
  pub write_every: usize,
  pub operations: usize,
  pub perf: bool,
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
                (@arg write_every: --write_every +takes_value "Issue a direct or group write every x operations")
                (@arg operations: --operations +takes_value "Number of operations in load")
                (@arg perf: --perf "Wait for user input before starting workload to attach perf")
        ).get_matches();

  // Parse command line arguments.
  let args =
    Args { files_per_user:
             value_t!(matches, "files_per_user", usize).unwrap_or(1),
           direct_shares_per_file: value_t!(matches,
                                            "direct_shares_per_file",
                                            usize).unwrap_or(1),
           group_shares_per_file: value_t!(matches,
                                           "group_shares_per_file",
                                           usize).unwrap_or(1),
           users_per_group:
             value_t!(matches, "users_per_group", usize).unwrap_or(1),
           num_users: value_t_or_exit!(matches, "num_users", usize),
           backend: matches.value_of("backend")
                           .map(&str::to_string)
                           .unwrap(),
           in_size: value_t!(matches, "in_size", usize).unwrap_or(1),
           outfile: matches.value_of("outfile").map(&str::to_string),
           write_every: value_t_or_exit!(matches, "write_every", usize),
           operations: value_t_or_exit!(matches, "operations", usize),
           perf: matches.is_present("perf") };

  // Output file.
  let mut f = if let Some(fname) = &args.outfile {
    Box::new(std::fs::File::create(fname).unwrap()) as Box<dyn Write>
  } else {
    Box::new(std::io::stdout()) as Box<dyn Write>
  };

  // Generate priming data.
  let ref mut st = GeneratorState::new();
  let users = st.generate_users(args.num_users);
  let files = st.generate_files(&users, args.files_per_user);
  let direct_shares =
    st.generate_direct_shares(&users, &files, args.direct_shares_per_file);
  let groups = st.generate_groups(&users, args.users_per_group);
  let group_shares =
    st.generate_group_shares(&groups, &files, args.group_shares_per_file);

  // Parameters.
  let in_size = args.in_size;
  let write_every = args.write_every;
  let operations = args.operations;

  // Run the experiment for each provided backend.
  let mut backend = Backend::from_str(&args.backend);
  eprintln!("--> Starting backend {}", backend);

  // Insert load (priming).
  let instant = Instant::now();
  users.iter().for_each(|user| backend.insert_user(user));
  groups.iter().for_each(|group| backend.insert_group(group));
  files.iter().for_each(|file| backend.insert_file(file));
  direct_shares.iter()
               .for_each(|share| backend.insert_share(share));
  group_shares.iter()
              .for_each(|share| backend.insert_share(share));
  eprintln!("--> Priming done in {}ms", instant.elapsed().as_millis());

  // Wait for user input.
  if args.perf {
    eprintln!("# perf provided: waiting for user input before executing load");
    std::io::stdin().read_line(&mut String::new());
  }

  // Start workload.
  let mut last_write_direct = false;
  let mut reads = Vec::<u128>::new();
  let mut dwrites = Vec::<u128>::new();
  let mut gwrites = Vec::<u128>::new();
  for i in 0..operations {
    if write_every > 0 && i > 0 && i % write_every == 0 {
      // Must issue a write.
      if !last_write_direct {
        last_write_direct = true;
        let request = workload::make_direct_share(st, &users, &files);
        dwrites.push(backend.run(&request));
      } else {
        last_write_direct = false;
        let request = workload::make_group_share(st, &groups, &files);
        gwrites.push(backend.run(&request));
      }
    } else {
      // Must issue a read.
      let request = workload::make_read(in_size, &users);
      reads.push(backend.run(&request));
    };
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
}
