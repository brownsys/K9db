#[link(name = "simple")]
extern "C"  {
    pub fn query_pelton_c(
        query: cty::c_int
    ) -> cty::c_int;
}