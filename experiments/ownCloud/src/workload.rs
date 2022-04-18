use rand::Rng;
use rand::rngs::ThreadRng;

// src.models.rs
use super::generator::{EntityType, GeneratorState};
use super::models::{File, Group, Share, ShareType, User};

// Request
pub enum Request<'a> {
  Read(Vec<&'a User>, Vec<usize>),
  Direct(Share<'a>),
  Indirect(Share<'a>),
}

pub struct WorkloadGenerator {
  rng: rand::rngs::ThreadRng,
  st: GeneratorState,
}

impl WorkloadGenerator {
  // Create a new request.
  pub fn new(st: GeneratorState) -> Self {
    WorkloadGenerator {
      rng: rand::thread_rng(),
      st: st,
    }
  }

  // Create a read workloads with given samples.
  pub fn make_read<'a>(&mut self, num_samples: usize, users: &'a [User]) -> Request<'a> {
    let mut rng = rand::thread_rng();
    let ulen = users.len();
    let mut samples: Vec<&'a User> = Vec::with_capacity(num_samples);
    if users.len() < num_samples {
      panic!("Too few users, need at least as many as samples");
    }
    while samples.len() != num_samples {
      let u: &'a User = &users[rng.gen_range(0..ulen)];
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
  pub fn make_direct_share<'a>(&mut self,
                               users: &'a [User],
                               files: &'a [File<'a>])
                               -> Request<'a> {
    let ulen = users.len();
    let flen = files.len();
    let share = Share { id: self.st.new_id(EntityType::Share),
                         share_with: ShareType::Direct(&users[self.rng.gen_range(0..ulen)]),
                         file: &files[self.rng.gen_range(0..flen)] };
    self.st.track_share(&share);
    Request::Direct(share)
  }

  // Create a write workload with indirect shares.
  pub fn make_group_share<'a>(&mut self,
                              groups: &'a [Group<'a>],
                              files: &'a [File<'a>])
                              -> Request<'a> {
    let glen = groups.len();
    let flen = files.len();
    let share = Share { id: self.st.new_id(EntityType::Share),
                        share_with: ShareType::Group(&groups[self.rng.gen_range(0..glen)]),
                        file: &files[self.rng.gen_range(0..flen)] };
    self.st.track_share(&share);
    Request::Indirect(share)
  }

}
