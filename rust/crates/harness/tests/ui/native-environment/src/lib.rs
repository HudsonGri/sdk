#[path = "../../../../src/native_environment.rs"]
pub mod real_seam;

#[cfg(feature = "negative")]
pub fn forbidden() {
    let _ = std::env::var_os("HOME");
    let _ = std::env::var_os("HOME",);
    let _ = std::env::var_os(r"USERPROFILE");
    use std::env::var_os as renamed;
    let _ = renamed(r#"APPDATA"#);
    use std::env as process;
    let _ = process::var("CODEX_HOME");
    let _ = std::env::vars();
    let _ = std::env::vars_os();
    let _ = std::env::home_dir();
}
