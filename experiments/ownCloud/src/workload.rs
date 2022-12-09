extern crate rand_distr;

use rand_distr::Zipf;
use rand::distributions::Distribution;

use rand::rngs::ThreadRng;
use rand::Rng;

use std::collections::HashMap;

// src.models.rs
use super::generator::{EntityType, GeneratorState};
use super::models::{File, Group, Share, ShareType, User};

// Request
pub enum Request {
  Read(Vec<User>, Vec<usize>),
  Direct(Share),
  Indirect(Share),
  GetFilePK(Vec<File>),
  UpdateFilePK(File, String),
}

pub type ZipfF = f64;

pub struct WorkloadGenerator {
  rng: rand::rngs::ThreadRng,
  st: GeneratorState,
  zipf_f: ZipfF,
  fn_counter : u128,
}

impl WorkloadGenerator {
  // Create a new request.
  pub fn new(st: GeneratorState, zipf_f: ZipfF) -> Self {
    WorkloadGenerator {
      rng: rand::thread_rng(),
      st: st,
      zipf_f,
      fn_counter: 0
    }
  }

  // Create a read workloads with given samples.
  pub fn make_read(
    &mut self,
    num_samples: usize,
    users: &[User],
  ) -> Request {
    let mut rng = rand::thread_rng();
    let ulen = users.len();
    let mut samples: Vec<User> = Vec::with_capacity(num_samples);
    if users.len() < num_samples {
      panic!("Too few users, need at least as many as samples");
    }
    let distr = Zipf::new(users.len() as u64, self.zipf_f).unwrap();
    while samples.len() != num_samples {
      let s = distr.sample(&mut rng) - 1.0;
      let u: &User = &users[s as usize];
      if !samples.contains(&u) {
        samples.push(u.clone());
      }
    }
    let mut expected = Vec::new();
    for u in &samples {
      expected.append(&mut self.st.shared_with(&u.uid));
    }
    expected.sort();
    Request::Read(samples, expected)
  }

  // Create a write workload with direct shares.
  pub fn make_direct_share(
    &mut self,
    users: &[User],
    files: &[File],
  ) -> Request {
    // Select a user from zipf.
    let distr = Zipf::new(users.len() as u64, self.zipf_f).unwrap();
    let s = distr.sample(&mut self.rng) - 1.0;
    let u: &User = &users[s as usize];
    // Select some file randomly.
    let flen = files.len();
    let share = Share {
      id: self.st.new_id(EntityType::Share),
      share_with: ShareType::Direct(u.clone()),
      file: files[self.rng.gen_range(0..flen)].clone(),
    };
    self.st.track_share(&share);
    Request::Direct(share)
  }

  // Create a write workload with indirect shares.
  pub fn make_group_share(
    &mut self,
    users: &[User],
    groups: &[Group],
    files: &[File],
    user_to_group: &HashMap<usize, usize>,
  ) -> Request {
    // Select a user from zipf.
    let distr = Zipf::new(users.len() as u64, self.zipf_f).unwrap();
    let s = distr.sample(&mut self.rng) - 1.0;
    let u = s as usize;
    // Get group of the select users.
    let g = &groups[user_to_group[&u]];
    // Select some file randomly.
    let flen = files.len();
    let share = Share {
      id: self.st.new_id(EntityType::Share),
      share_with: ShareType::Group(g.clone()),
      file: files[self.rng.gen_range(0..flen)].clone(),
    };
    self.st.track_share(&share);
    Request::Indirect(share)
  }

  pub fn make_get_file_pk(
    &mut self,
    num_samples: usize,
    files: &[File],
  ) -> Request {
    let mut rng = rand::thread_rng();
    let flen = files.len();
    let mut samples: Vec<File> = Vec::with_capacity(1000);
    if flen < num_samples {
      panic!("Too few files, need at least as many as samples");
    }
    let distr = Zipf::new(flen as u64, self.zipf_f).unwrap();
    while samples.len() != num_samples {
      let s = distr.sample(&mut rng) - 1.0;
      let f: &File = &files[s as usize];
      if !samples.contains(&f) {
        samples.push(f.clone());
      }
    }
    Request::GetFilePK(samples)
  }

  pub fn make_update_file_pk(
    &mut self,
    files: &[File],
  ) -> Request {
    let flen = files.len();
    let file =  &files[self.rng.gen_range(0..flen)];
    self.fn_counter = self.fn_counter + 1;
    Request::UpdateFilePK(file.clone(), self.fn_counter.to_string())
  }
}
