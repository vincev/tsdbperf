#![deny(rust_2018_idioms)]

use anyhow::{anyhow, Result};
use chrono::Utc;
use futures::future::try_join_all;
use log::info;
use structopt::StructOpt;

mod db;
mod measurement;

#[derive(StructOpt, Debug)]
#[structopt(name = "tsdbperf")]
pub struct Opt {
    /// Number of measurements per device
    #[structopt(long = "measurements", default_value = "100000")]
    num_measurements: u32,

    /// Number of devices per worker
    #[structopt(long = "devices", default_value = "10")]
    num_devices: u32,

    /// Number of parallel workers [default: number of CPUs]
    #[structopt(long = "workers")]
    num_workers: Option<u32>,

    #[structopt(flatten)]
    dbopts: db::DbOpt,
}

#[tokio::main]
async fn main() -> Result<()> {
    let log_env = env_logger::Env::default().default_filter_or("info");
    env_logger::from_env(log_env).init();

    let mut opt = Opt::from_args();

    opt.num_workers = opt
        .num_workers
        .map(|n| n.max(1))
        .or_else(|| Some(num_cpus::get() as u32));

    if opt.num_workers.unwrap() > 64 {
        Err(anyhow!("Number of workers cannot be greater than 64"))
    } else if opt.num_devices > 10_000 {
        Err(anyhow!("Number of devices cannot be greater than 10,000"))
    } else if opt.dbopts.num_metrics > 100 {
        Err(anyhow!("Number of metrics cannot be greater than 100"))
    } else {
        info!("Number of workers:       {}", opt.num_workers.unwrap());
        info!("Devices per worker:      {}", opt.num_devices);
        info!("Metrics per device:      {}", opt.dbopts.num_metrics);
        info!("Measurements per device: {}", opt.num_measurements);

        run_workers(&opt).await
    }
}

async fn run_workers(opt: &Opt) -> Result<()> {
    db::init(&opt.dbopts).await?;
    let start_time = Utc::now();

    let handles = (1..=opt.num_workers.unwrap())
        .map(|worker_id| {
            let dbopts = opt.dbopts.clone();
            let (num_devices, num_measurements) = (opt.num_devices, opt.num_measurements);
            tokio::spawn(async move {
                db::run_worker(&dbopts, worker_id, num_devices, num_measurements).await
            })
        })
        .collect::<Vec<_>>();

    let num_written: usize = try_join_all(handles)
        .await?
        .into_iter()
        .collect::<Result<Vec<usize>>>()?
        .iter()
        .sum();

    let elapsed_secs = (Utc::now() - start_time).num_milliseconds() as f64 / 1000.0;

    info!(
        "Wrote {:9} measurements in {:.2} seconds",
        num_written, elapsed_secs
    );

    let measurements_per_sec = num_written as f64 / elapsed_secs;
    info!(
        "Wrote {:9.0} measurements per second",
        measurements_per_sec.round()
    );

    info!(
        "Wrote {:9.0} metrics per second",
        (measurements_per_sec * opt.dbopts.num_metrics as f64).round()
    );

    Ok(())
}
