extern crate rand_distr;

use rand_distr::Zipf;
use rand::distributions::Distribution;

use rand::rngs::ThreadRng;
use rand::Rng;

// src.models.rs
use super::generator::{EntityType, GeneratorState};
use super::models::{File, Group, Share, ShareType, User};

// Request
pub enum Request<'a> {
  Read(Vec<&'a User>, Vec<usize>),
  Direct(Share<'a>),
  Indirect(Share<'a>),
  GetFilePK(&'a File<'a>),
  UpdateFilePK(&'a File<'a>, String),
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
  pub fn make_read<'a>(
    &mut self,
    num_samples: usize,
    users: &'a [User],
  ) -> Request<'a> {
    let mut rng = rand::thread_rng();
    let ulen = users.len();
    let mut samples: Vec<&'a User> = Vec::with_capacity(num_samples);
    if users.len() < num_samples {
      panic!("Too few users, need at least as many as samples");
    }
    let distr = Zipf::new(users.len() as u64, self.zipf_f).unwrap();
    while samples.len() != num_samples {
      let s = distr.sample(&mut rng) - 1.0;
      let u: &'a User = &users[s as usize];
      if !samples.contains(&u) {
        samples.push(u);
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
  pub fn make_direct_share<'a>(
    &mut self,
    users: &'a [User],
    files: &'a [File<'a>],
  ) -> Request<'a> {
    let ulen = users.len();
    let flen = files.len();
    let share = Share {
      id: self.st.new_id(EntityType::Share),
      share_with: ShareType::Direct(&users[self.rng.gen_range(0..ulen)]),
      file: &files[self.rng.gen_range(0..flen)],
    };
    self.st.track_share(&share);
    Request::Direct(share)
  }

  // Create a write workload with indirect shares.
  pub fn make_group_share<'a>(
    &mut self,
    groups: &'a [Group<'a>],
    files: &'a [File<'a>],
  ) -> Request<'a> {
    let glen = groups.len();
    let flen = files.len();
    let share = Share {
      id: self.st.new_id(EntityType::Share),
      share_with: ShareType::Group(&groups[self.rng.gen_range(0..glen)]),
      file: &files[self.rng.gen_range(0..flen)],
    };
    self.st.track_share(&share);
    Request::Indirect(share)
  }

  pub fn make_get_file_pk<'a>(
    &mut self,
    files: &'a [File<'a>],
  ) -> Request<'a> {
    let flen = files.len();
    let file =  &files[self.rng.gen_range(0..flen)];
    Request::GetFilePK(file)
  }

  pub fn make_update_file_pk<'a>(
    &mut self,
    files: &'a [File<'a>],
  ) -> Request<'a> {
    let flen = files.len();
    let file =  &files[self.rng.gen_range(0..flen)];
    self.fn_counter = self.fn_counter + 1;
    Request::UpdateFilePK(file, self.fn_counter.to_string())
  }
}
