use std::thread;

use anyhow::Result;
use log::error;
use structopt::StructOpt;
use tokio::sync::mpsc;
use tokio_postgres::{Client, Row};

use crate::measurement::{Measurement, MeasurementIterator};

mod command;

#[derive(Debug, Clone, StructOpt)]
pub struct DbOpt {
    /// Number of metrics per device
    #[structopt(long = "metrics", default_value = "10")]
    pub num_metrics: u32,
    /// Hypertable chunk interval in seconds
    #[structopt(long = "chunk-interval", default_value = "86400")]
    pub chunk_interval: usize,
    /// Number of measurements per insert
    #[structopt(long = "batch-size", default_value = "10000")]
    pub batch_size: usize,
    /// Skip DB inserts, report only data generation timings
    #[structopt(long = "dry-run")]
    pub dry_run: bool,
    /// Run the tests without creating hypertables
    #[structopt(long = "no-hypertables")]
    pub no_hypertables: bool,
    /// Run the tests with copy in upserts
    #[structopt(long, conflicts_with_all = &["with-upserts", "with-jsonb"])]
    pub with_copy_upserts: bool,
    /// Run the tests with upserts
    #[structopt(long, conflicts_with = "with-jsonb")]
    pub with_upserts: bool,
    /// Insert metrics as a JSONB column
    #[structopt(long)]
    pub with_jsonb: bool,
    /// Database host
    #[structopt(long = "db-host", default_value = "localhost")]
    pub db_host: String,
    /// Database user
    #[structopt(long = "db-user", default_value = "postgres")]
    pub db_user: String,
    /// Database password
    #[structopt(long = "db-password", default_value = "postgres")]
    pub db_password: String,
    /// Database name
    #[structopt(long = "db-name", default_value = "postgres")]
    pub db_name: String,
}

/// Initialize schema and close connection.
pub async fn init(opts: &DbOpt) -> Result<()> {
    let db = Db::connect(opts).await?;
    db.create_schema().await?;
    Ok(())
}

/// Run worker writes devices * measurements rows.
pub async fn run_worker(
    opt: &DbOpt,
    worker_id: u32,
    num_devices: u32,
    num_measurements: u32,
) -> Result<usize> {
    let batch_size = opt.batch_size;
    let num_metrics = opt.num_metrics;
    let (tx, mut rx) = mpsc::channel::<Vec<Measurement>>(100);

    thread::spawn(move || {
        let mut measurements =
            MeasurementIterator::new(worker_id, num_devices, num_metrics, num_measurements);
        let mut num_sent = 0;
        let send_until = num_devices * num_measurements;

        while num_sent < send_until {
            let mut data = Vec::with_capacity(batch_size);
            for m in &mut measurements {
                data.push(m);
                num_sent += 1;

                if data.len() == batch_size || num_sent == send_until {
                    break;
                }
            }

            if tx.blocking_send(data).is_err() {
                break; // Reader disconnected
            }
        }
    });

    let mut num_written = 0;
    let mut db = Db::connect(opt).await?;
    let command = command::Command::new(opt, &db.client).await?;

    while let Some(data) = rx.recv().await {
        num_written += if opt.dry_run {
            data.len()
        } else {
            command.execute(&mut db.client, data).await?
        };
    }

    Ok(num_written)
}

pub struct Db {
    client: Client,
    num_metrics: u32,
    chunk_interval: usize,
    use_hypertables: bool,
    with_jsonb: bool,
}

impl Db {
    pub async fn connect(opts: &DbOpt) -> Result<Self> {
        use tokio_postgres::{config::Config, NoTls};

        let (client, connection) = Config::new()
            .user(&opts.db_user)
            .password(&opts.db_password)
            .host(&opts.db_host)
            .dbname(&opts.db_name)
            .connect(NoTls)
            .await?;

        // The connection object performs the actual communication
        // with the database, so spawn it off to run on its own.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("connection error: {}", e);
            }
        });

        Ok(Db {
            client,
            num_metrics: opts.num_metrics,
            chunk_interval: opts.chunk_interval * 1_000_000,
            use_hypertables: !opts.no_hypertables,
            with_jsonb: opts.with_jsonb,
        })
    }

    pub async fn query(&self, query: &str) -> Result<Vec<Row>> {
        Ok(self.client.query(query, &[]).await?)
    }

    async fn create_schema(&self) -> Result<()> {
        let mut stms = vec![format!("DROP TABLE IF EXISTS measurement;")];

        if self.with_jsonb {
            stms.push(format!(
                "CREATE TABLE measurement(
                    time  TIMESTAMP WITH TIME ZONE NOT NULL,
                    device_id OID NOT NULL,
                    metrics JSONB NOT NULL);"
            ));
        } else {
            let columns = (1..=self.num_metrics)
                .map(|c| format!("m{} DOUBLE PRECISION", c))
                .collect::<Vec<_>>()
                .join(", ");

            stms.push(format!(
                "CREATE TABLE measurement(
                    time  TIMESTAMP WITH TIME ZONE NOT NULL,
                    device_id OID NOT NULL,
                    {});",
                columns
            ));
        }

        stms.push(format!(
            "CREATE INDEX ON measurement(time DESC);
             CREATE UNIQUE INDEX ON measurement(device_id, time DESC);"
        ));

        if self.use_hypertables {
            stms.push(format!(
                "SELECT create_hypertable(
                   'measurement',
                   'time',
                   chunk_time_interval => {});",
                self.chunk_interval
            ));
        }

        self.client.batch_execute(&stms.join("\n")).await?;
        Ok(())
    }
}
