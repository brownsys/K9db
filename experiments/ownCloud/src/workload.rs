use rand::Rng;

// src.models.rs
use super::generator::{EntityType, GeneratorState};
use super::models::{File, Group, Share, ShareType, User};

// Request
pub enum Request<'a> {
  Read(Vec<&'a User>),
  Direct(Share<'a>),
  Indirect(Share<'a>),
}

// Create a read workloads with given samples.
pub fn make_read<'a>(num_samples: usize, users: &'a [User]) -> Request<'a> {
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
  Request::Read(samples)
}

// Create a write workload with direct shares.
pub fn make_direct_share<'a>(st: &mut GeneratorState,
                             users: &'a [User],
                             files: &'a [File<'a>])
                             -> Request<'a> {
  let mut rng = rand::thread_rng();
  let ulen = users.len();
  let flen = files.len();
  Request::Direct(Share { id: st.new_id(EntityType::Share),
                          share_with:
                            ShareType::Direct(&users[rng.gen_range(0..ulen)]),
                          file: &files[rng.gen_range(0..flen)] })
}

// Create a write workload with indirect shares.
pub fn make_group_share<'a>(st: &mut GeneratorState,
                            groups: &'a [Group<'a>],
                            files: &'a [File<'a>])
                            -> Request<'a> {
  let mut rng = rand::thread_rng();
  let glen = groups.len();
  let flen = files.len();
  Request::Indirect(Share { id: st.new_id(EntityType::Share),
                            share_with:
                              ShareType::Group(&groups[rng.gen_range(0..glen)]),
                            file: &files[rng.gen_range(0..flen)] })
}
