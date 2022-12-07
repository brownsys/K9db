// Owncloud user.
pub struct User {
  pub uid: String,
  pub gid: String,  // ID of the group this user is in.
}

pub struct File<'a> {
  pub id: usize,
  pub owned_by: &'a User,
}

// Group consists of several owning users.
pub struct Group<'a> {
  pub gid: String,
  pub users: Vec<(usize, &'a User)>,
}

// Share.
pub enum ShareType<'a> {
  Direct(&'a User),
  Group(&'a Group<'a>),
}
pub struct Share<'a> {
  pub id: usize,
  pub share_with: ShareType<'a>,
  pub file: &'a File<'a>,
}

impl PartialEq for User {
  fn eq(&self, other: &Self) -> bool {
    &self.uid == &other.uid
  }
}

impl<'a> PartialEq for &'a File<'a> {
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
