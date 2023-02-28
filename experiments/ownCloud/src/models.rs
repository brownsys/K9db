// Owncloud user.
#[derive(Clone)]
pub struct User {
  pub uid: String,
}

#[derive(Clone)]
pub struct File {
  pub id: usize,
  pub owned_by: String,
}

// Group consists of several owning users.
#[derive(Clone)]
pub struct Group {
  pub gid: String,
  pub users: Vec<(usize, String)>,
}

// Share.
#[derive(Clone)]
pub enum ShareType {
  Direct(User),
  Group(Group),
}
#[derive(Clone)]
pub struct Share {
  pub id: usize,
  pub share_with: ShareType,
  pub file: File,
}

impl PartialEq for User {
  fn eq(&self, other: &Self) -> bool {
    self.uid == other.uid
  }
}

impl PartialEq for File {
  fn eq(&self, other: &Self) -> bool {
    self.id == other.id
  }
}

/*
impl <'a> ToValue for Group<'a> {
    fn to_value(&self) -> Value {
        self.gid.to_value()
    }
}
*/
