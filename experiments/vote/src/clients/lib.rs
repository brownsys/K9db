#![feature(min_type_alias_impl_trait)]

use clap;
use std::future::Future;

#[derive(Copy, Clone, Debug)]
pub struct Parameters {
    pub prime: bool,
    pub articles: usize,
}

pub struct WriteRequest(pub Vec<i32>);
pub struct ReadRequest(pub Vec<i32>);

pub trait VoteClient
where
    Self: Sized,
{
    type Future: Future<Output = Result<Self, failure::Error>> + Send + 'static;
    fn new(params: Parameters, args: clap::ArgMatches) -> <Self as VoteClient>::Future;
}

pub mod mysql;
