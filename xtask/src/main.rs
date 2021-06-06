/*!
Post build tasks for distribution and install.

This implementation provides the following tasks:
- `cargo xtask dist` to create a compressed binary for a target
- `cargo xtask install` to install tsdbperf as a binary in ~/.cargo/bin

See [cargo-xtask](https://github.com/matklad/cargo-xtask) for documentation
and examples.

For a more comprehensive example see the source for
[xtask in rust-analyzer](https://github.com/rust-analyzer/rust-analyzer/tree/master/xtask).
*/
#![deny(rust_2018_idioms)]

use std::{
    env,
    fs::File,
    path::{Path, PathBuf},
};

use anyhow::Result;
use flate2::{write::GzEncoder, Compression};
use structopt::StructOpt;
use xshell::{cmd, mkdir_p, rm_rf};

#[derive(Debug, StructOpt)]
#[structopt(name = "xtask")]
pub enum Opt {
    Dist,
    Install,
}

fn main() -> Result<()> {
    match Opt::from_args() {
        Opt::Dist => run_dist(),
        Opt::Install => run_install(),
    }
}

fn run_dist() -> Result<()> {
    let dist = project_root().join("dist");
    rm_rf(&dist)?;
    mkdir_p(&dist)?;

    let target = get_target();

    cmd!("cargo build --manifest-path ./tsdbperf/Cargo.toml --bin tsdbperf --target {target} --release").run()?;

    let suffix = exe_suffix(&target);
    let src = Path::new("target")
        .join(&target)
        .join("release")
        .join(format!("tsdbperf{}", suffix));
    let dst = Path::new("dist").join(format!("tsdbperf-{}{}", target, suffix));
    gzip(&src, &dst.with_extension("gz"))?;

    Ok(())
}

fn run_install() -> Result<()> {
    cmd!("cargo install --path tsdbperf --locked --force").run()?;
    Ok(())
}

fn get_target() -> String {
    match env::var("tsdbperf_TARGET") {
        Ok(target) => target,
        _ => {
            if cfg!(target_os = "linux") {
                "x86_64-unknown-linux-gnu".to_string()
            } else if cfg!(target_os = "windows") {
                "x86_64-pc-windows-msvc".to_string()
            } else if cfg!(target_os = "macos") {
                "x86_64-apple-darwin".to_string()
            } else {
                panic!("Unsupported OS, maybe try setting tsdbperf_TARGET")
            }
        }
    }
}

fn exe_suffix(target: &str) -> String {
    if target.contains("-windows-") {
        ".exe".into()
    } else {
        "".into()
    }
}

fn gzip(src_path: &Path, dest_path: &Path) -> Result<()> {
    let mut encoder = GzEncoder::new(File::create(dest_path)?, Compression::best());
    let mut input = std::io::BufReader::new(File::open(src_path)?);
    std::io::copy(&mut input, &mut encoder)?;
    encoder.finish()?;
    Ok(())
}

fn project_root() -> PathBuf {
    Path::new(
        &env::var("CARGO_MANIFEST_DIR")
            .unwrap_or_else(|_| env!("CARGO_MANIFEST_DIR").to_owned()),
    )
    .ancestors()
    .nth(1)
    .unwrap()
    .to_path_buf()
}
