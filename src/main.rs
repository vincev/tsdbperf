#![deny(rust_2018_idioms)]

use anyhow::Result;
use chrono::Utc;
use futures::future::try_join_all;
use log::info;
use structopt::StructOpt;

mod db;
mod measurement;

#[derive(StructOpt, Debug)]
#[structopt(name = "tsdbperf")]
struct Opt {
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

    let opt = Opt::from_args();

    let (num_written, elapsed_secs) = run_workers(&opt).await?;

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

async fn run_workers(opt: &Opt) -> Result<(usize, f64)> {
    db::init(&opt.dbopts).await?;

    let num_workers = opt
        .num_workers
        .unwrap_or_else(|| num_cpus::get() as u32)
        .max(1);
    info!("Number of workers:       {}", num_workers);
    info!("Devices per worker:      {}", opt.num_devices);
    info!("Metrics per device:      {}", opt.dbopts.num_metrics);
    info!("Measurements per device: {}", opt.num_measurements);

    let start_time = Utc::now();

    let handles = (1..=num_workers)
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

    Ok((num_written, elapsed_secs))
}
