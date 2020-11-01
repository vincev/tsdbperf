use std::iter;
use std::thread;

use anyhow::Result;
use futures::pin_mut;
use log::error;
use structopt::StructOpt;
use tokio::sync::mpsc;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::Client;

use crate::measurement::{Measurement, MeasurementIterator};

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
        let mut num_written = 0;
        while num_written < (num_devices * num_measurements) as usize {
            let mut data = Vec::with_capacity(batch_size);
            for m in &mut measurements {
                data.push(m);
                if data.len() == batch_size {
                    break;
                }
            }

            num_written += data.len();
            tx.blocking_send(data).unwrap();
        }
    });

    let mut num_written = 0;
    let db = Db::connect(opt).await?;
    while let Some(data) = rx.recv().await {
        if !opt.dry_run {
            db.insert(&data).await?;
        }
        num_written += data.len();
    }

    Ok(num_written)
}

struct Db {
    db_client: Client,
    num_metrics: u32,
    chunk_interval: usize,
    col_types: Vec<Type>,
    copy_stm: String,
}

impl Db {
    async fn connect(opts: &DbOpt) -> Result<Self> {
        use tokio_postgres::{config::Config, NoTls};

        let (db_client, connection) = Config::new()
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

        // Columns types are a timestamp, a device id, and a f64 for
        // each metric
        let mut col_types = vec![Type::TIMESTAMP, Type::OID];
        col_types.extend(iter::repeat(Type::FLOAT8).take(opts.num_metrics as usize));

        let columns = (1..=opts.num_metrics)
            .map(|c| format!("m{}", c))
            .collect::<Vec<_>>()
            .join(", ");

        let copy_stm = format!(
            "COPY measurement (time, device_id, {}) FROM STDIN BINARY",
            columns
        );

        Ok(Db {
            db_client,
            num_metrics: opts.num_metrics,
            chunk_interval: opts.chunk_interval * 1_000_000,
            col_types,
            copy_stm,
        })
    }

    async fn create_schema(&self) -> Result<()> {
        let columns = (1..=self.num_metrics)
            .map(|c| format!("m{} DOUBLE PRECISION", c))
            .collect::<Vec<_>>()
            .join(", ");

        let stm = format!(
            "DROP TABLE IF EXISTS measurement;

            CREATE TABLE measurement(
              time  TIMESTAMP WITH TIME ZONE NOT NULL,
              device_id OID,
              {});

            CREATE INDEX ON measurement(time DESC);
            CREATE INDEX ON measurement(device_id, time DESC);

            SELECT create_hypertable(
              'measurement',
              'time',
              chunk_time_interval => {});",
            columns, self.chunk_interval
        );

        self.db_client.batch_execute(&stm).await?;
        Ok(())
    }

    async fn insert(&self, measurements: &Vec<Measurement>) -> Result<()> {
        let sink = self.db_client.copy_in(self.copy_stm.as_str()).await?;
        let writer = BinaryCopyInWriter::new(sink, &self.col_types);

        pin_mut!(writer);

        let mut data: Vec<&'_ (dyn ToSql + Sync)> = Vec::new();
        for m in measurements {
            data.clear();
            data.push(&m.time);
            data.push(&m.device_id);
            data.extend(m.metrics.iter().map(|x| x as &(dyn ToSql + Sync)));
            writer.as_mut().write(&data).await?;
        }

        writer.finish().await?;

        Ok(())
    }
}
