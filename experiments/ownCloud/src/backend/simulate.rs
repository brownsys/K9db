use std::collections::HashMap;

// src/models.rs
use super::super::models::{File, Group, Share, ShareType, User};

pub struct SimulatedState {
  simulated: bool,
  shares: usize,
  share_map: HashMap<String, Vec<usize>>,  // uid -> [shareid]
  group_map: HashMap<String, Vec<String>>,  // gid -> [uid]
}

impl SimulatedState {
  pub fn new() -> Self {
    SimulatedState {
      simulated: false,
      shares: 0,
      share_map: HashMap::new(),
      group_map: HashMap::new(),
    }
  }

  pub fn insert_user(&mut self, user: &User) {
    println!("{}", user.uid);
    self.share_map.insert(user.uid.to_string(), Vec::new());
  }

  pub fn insert_group<'a>(&mut self, group: &Group<'a>) {
    self.group_map.insert(group.gid.to_string(), Vec::new());
    for (_, user) in &group.users {
      self.group_map.get_mut(&group.gid).unwrap().push(user.uid.to_string());
    }
  }

  pub fn insert_share<'a>(&mut self, share: &Share<'a>) {
    match share.share_with {
      ShareType::Direct(u) => {
        self.shares += 1;
        self.share_map.get_mut(&u.uid).unwrap().push(share.id);
      }
      ShareType::Group(g) => {
        for u in self.group_map.get(&g.gid).unwrap() {
          self.shares += 1;
          self.share_map.get_mut(u).unwrap().push(share.id);
        }
      }
    };
  }
  
  pub fn print(&mut self) {
    if !self.simulated {
      self.simulated = true;
      let mut vec = Vec::new();
      for (k, v) in &self.share_map {
        vec.push(v.len());
      }
      vec.sort();
      let min = vec[0];
      let median = vec[vec.len() / 2];
      let max = vec[vec.len() - 1];
      let avg = vec.iter().fold(0, |acc, size| acc + size) / vec.len();
      println!("# shares = {}", self.shares);
      println!("expected = {}", self.shares / self.share_map.len());
      println!("min = {},", min);
      println!("median = {},", median);
      println!("max = {},", max);
      println!("avg = {},", avg);
    }
  }
}

// Workload execution.
pub fn reads<'a>(conn: &mut SimulatedState, sample: &Vec<&'a User>,
                 expected: &Vec<usize>) -> u128 {
  conn.print();
  0
}
pub fn direct<'a>(conn: &mut SimulatedState, share: &Share<'a>) -> u128 {
  0
}
pub fn indirect<'a>(conn: &mut SimulatedState, share: &Share<'a>) -> u128 {
  0
}

// Inserts.
pub fn insert_user(conn: &mut SimulatedState, user: &User) {
  conn.insert_user(user);
}

pub fn insert_group<'a>(conn: &mut SimulatedState, group: &Group<'a>) {
  conn.insert_group(group);
}

pub fn insert_file<'a>(conn: &mut SimulatedState, file: &File<'a>) {}

pub fn insert_share<'a>(conn: &mut SimulatedState, share: &Share<'a>) {
  conn.insert_share(share);
}
