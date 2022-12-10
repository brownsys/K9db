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

  pub fn insert_users(&mut self, users: &Vec<User>) {
    for user in users {
      self.share_map.insert(user.uid.to_string(), Vec::new());
    }
  }

  pub fn insert_groups(&mut self, groups: &Vec<Group>) {
    for group in groups {
      self.group_map.insert(group.gid.to_string(), Vec::new());
      for (_, uid) in &group.users {
        self.group_map.get_mut(&group.gid).unwrap().push(uid.to_string());
      }
    }
  }

  pub fn insert_shares(&mut self, shares: &Vec<Share>) {
    for share in shares {
      match &share.share_with {
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
pub fn reads(conn: &mut SimulatedState, sample: &Vec<User>,
                 expected: &Vec<usize>) -> u128 {
  conn.print();
  0
}
pub fn direct(conn: &mut SimulatedState, share: &Share) -> u128 {
  0
}
pub fn indirect(conn: &mut SimulatedState, share: &Share) -> u128 {
  0
}
pub fn read_file_pk(conn: &mut SimulatedState, files: &Vec<File>) -> u128 {
  0
}
pub fn update_file_pk(conn: &mut SimulatedState, file: &File, new_name: String) -> u128 {
  0
}

// Inserts.
pub fn insert_users(conn: &mut SimulatedState, users: Vec<User>) {
  conn.insert_users(&users);
}
pub fn insert_groups(conn: &mut SimulatedState, groups: Vec<Group>) {
  conn.insert_groups(&groups);
}
pub fn insert_files(conn: &mut SimulatedState, files: Vec<File>) {}
pub fn insert_shares(conn: &mut SimulatedState, shares: Vec<Share>) {
  conn.insert_shares(&shares);
}
