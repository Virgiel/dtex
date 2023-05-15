use napi_derive::napi;

#[napi]
pub fn ex(path: String) {
    ::dtex::run(vec![::dtex::Open::File(path.into())], String::new());
}
