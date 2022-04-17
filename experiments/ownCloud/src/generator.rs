use rand::Rng;
use std::collections::HashMap;

// src/models.rs
use super::models::{File, Group, Share, ShareType, User};

// Entities that we can generate.
#[derive(Hash, PartialEq, Eq)]
pub enum EntityType {
  Group,
  User,
  Share,
  UserGroupAssoc,
  File,
}

// A generator needs to keep track of the last id/size of each entity to
// generate unique ids.
pub struct GeneratorState(HashMap<EntityType, usize>);

impl GeneratorState {
  pub fn new() -> Self {
    GeneratorState(HashMap::new())
  }

  // Generate a new unique id for given entity.
  pub fn new_id(&mut self, e: EntityType) -> usize {
    *self.0.entry(e).and_modify(|i| *i += 1).or_insert(0)
  }
  pub fn new_sid(&mut self, e: EntityType) -> String {
    self.0
        .entry(e)
        .and_modify(|i| *i += 1)
        .or_insert(0)
        .to_string()
  }

  // Generate users.
  pub fn generate_users(&mut self, num: usize) -> Vec<User> {
    std::iter::repeat_with(|| User { uid: self.new_sid(EntityType::User) }).take(num).collect()
  }

  // Generate groups and add users to them.
  pub fn generate_groups<'b>(&mut self,
                             users: &'b [User],
                             num: usize)
                             -> Vec<Group<'b>> {
    if num > users.len() {
      panic!("Number of users < number of users per group");
    }
    let num_groups = if num > 0 { users.len() / num } else { 0 };
    let ref mut rng = rand::thread_rng();
    let mut raw_groups: Vec<_> =
      std::iter::repeat_with(|| Group { gid:
                                          self.new_sid(EntityType::Group),
                                        users: vec![] }).take(num_groups)
                                                        .collect();
    let grp_len = raw_groups.len();
    if grp_len > 0 {
      users.iter().for_each(|u|
              raw_groups[rng.gen_range(0..grp_len)].users.push((self.new_id(EntityType::UserGroupAssoc), u))
          );
    }
    raw_groups
  }

  // Generate files with an owner, but no shares.
  pub fn generate_files<'b>(&mut self,
                            users: &'b [User],
                            num: usize)
                            -> Vec<File<'b>> {
    users.iter()
         .flat_map(|u| std::iter::repeat(u).take(num))
         .map(|u| File { id: self.new_id(EntityType::File),
                         owned_by: u })
         .collect()
  }

  // Generate direct shares.
  pub fn generate_direct_shares<'b>(&mut self,
                                    users: &'b [User],
                                    files: &'b [File<'b>],
                                    num: usize)
                                    -> Vec<Share<'b>> {
    let ref mut rng = rand::thread_rng();
    files.iter()
            .flat_map(|f| std::iter::repeat(f).take(num))
            .map(|f|
                Share {
                    id: self.new_id(EntityType::Share),
                    share_with: ShareType::Direct(&users[rng.gen_range(0..users.len())]),
                    file: f
                }
            )
            .collect()
  }

  // generate indirect shares.
  pub fn generate_group_shares<'b>(&mut self,
                                   groups: &'b [Group],
                                   files: &'b [File<'b>],
                                   num: usize)
                                   -> Vec<Share<'b>> {
    let ref mut rng = rand::thread_rng();
    files.iter()
            .flat_map(|f| std::iter::repeat(f).take(num))
            .map(|f|
                Share {
                    id: self.new_id(EntityType::Share),
                    share_with: ShareType::Group(&groups[rng.gen_range(0..groups.len())]),
                    file: f
                }
            )
            .collect()
  }
}
