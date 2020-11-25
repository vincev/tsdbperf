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
    /// Run the tests without creating hypertables
    #[structopt(long = "no-hypertables")]
    pub no_hypertables: bool,
    /// Run the tests with upserts
    #[structopt(long = "with-upserts")]
    pub do_upserts: bool,
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
    let mut db = Db::connect(opt).await?;
    while let Some(data) = rx.recv().await {
        if !opt.dry_run {
            num_written += db.insert(&data).await?;
        }
    }

    Ok(num_written)
}

struct Db {
    db_client: Client,
    num_metrics: u32,
    chunk_interval: usize,
    use_hypertables: bool,
    command: Command,
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

        let command = if opts.do_upserts {
            Command::Upsert(UpsertCommand::new(opts, &db_client).await?)
        } else {
            Command::Insert(InsertCommand::new(opts).await?)
        };

        Ok(Db {
            db_client,
            num_metrics: opts.num_metrics,
            chunk_interval: opts.chunk_interval * 1_000_000,
            use_hypertables: !opts.no_hypertables,
            command,
        })
    }

    async fn create_schema(&self) -> Result<()> {
        let columns = (1..=self.num_metrics)
            .map(|c| format!("m{} DOUBLE PRECISION", c))
            .collect::<Vec<_>>()
            .join(", ");

        let mut stms = vec![format!(
            "DROP TABLE IF EXISTS measurement;

            CREATE TABLE measurement(
              time  TIMESTAMP WITH TIME ZONE NOT NULL,
              device_id OID NOT NULL,
              {});

            CREATE INDEX ON measurement(time DESC);
            CREATE UNIQUE INDEX ON measurement(device_id, time DESC);",
            columns
        )];

        if self.use_hypertables {
            stms.push(format!(
                "SELECT create_hypertable(
                   'measurement',
                   'time',
                   chunk_time_interval => {});",
                self.chunk_interval
            ));
        }

        self.db_client.batch_execute(&stms.join("\n")).await?;
        Ok(())
    }

    async fn insert(&mut self, data: &Vec<Measurement>) -> Result<usize> {
        let num_written = match &self.command {
            Command::Insert(cmd) => cmd.execute(&mut self.db_client, data).await?,
            Command::Upsert(cmd) => cmd.execute(&mut self.db_client, data).await?,
        };

        Ok(num_written)
    }
}

enum Command {
    Insert(InsertCommand),
    Upsert(UpsertCommand),
}

struct InsertCommand {
    col_types: Vec<Type>,
    copy_stm: String,
}

impl InsertCommand {
    async fn new(opts: &DbOpt) -> Result<Self> {
        Ok(Self {
            col_types: get_col_types(opts.num_metrics),
            copy_stm: get_copy_statement("measurement", opts.num_metrics),
        })
    }

    async fn execute(&self, client: &mut Client, data: &Vec<Measurement>) -> Result<usize> {
        let tx = client.transaction().await?;
        let sink = tx.copy_in(self.copy_stm.as_str()).await?;
        let writer = BinaryCopyInWriter::new(sink, &self.col_types);
        let num_written = write(writer, data).await?;
        tx.commit().await?;
        Ok(num_written)
    }
}

// An upsert operation happens when we try insert a row that violates
// a unique constraint, in this case a row with the same time and
// device id. To keep ingestion rate high we use copy in binary on a
// temp table and then insert its data into the final table using an
// ON CONFLICT update statement.
struct UpsertCommand {
    col_types: Vec<Type>,
    copy_stm: String,
    insert_stm: String,
}

impl UpsertCommand {
    async fn new(opts: &DbOpt, client: &Client) -> Result<Self> {
        client
            .batch_execute(
                "CREATE TEMP TABLE upserts ON COMMIT DELETE ROWS \
                 AS TABLE measurement WITH NO DATA;",
            )
            .await?;

        let set_columns = (1..=opts.num_metrics)
            .map(|c| format!("m{} = EXCLUDED.m{}", c, c))
            .collect::<Vec<_>>()
            .join(", ");

        let insert_stm = format!(
            "INSERT INTO measurement
             SELECT * FROM upserts
             ON CONFLICT (device_id, time) DO UPDATE SET {}",
            set_columns
        );

        Ok(Self {
            col_types: get_col_types(opts.num_metrics),
            copy_stm: get_copy_statement("upserts", opts.num_metrics),
            insert_stm,
        })
    }

    async fn execute(&self, client: &mut Client, data: &Vec<Measurement>) -> Result<usize> {
        let tx = client.transaction().await?;

        let sink = tx.copy_in(self.copy_stm.as_str()).await?;
        let writer = BinaryCopyInWriter::new(sink, &self.col_types);
        let num_written = write(writer, data).await?;

        tx.batch_execute(self.insert_stm.as_str()).await?;

        tx.commit().await?;
        Ok(num_written)
    }
}

async fn write(writer: BinaryCopyInWriter, data: &Vec<Measurement>) -> Result<usize> {
    pin_mut!(writer);

    let mut row: Vec<&'_ (dyn ToSql + Sync)> = Vec::new();
    for m in data {
        row.clear();
        row.push(&m.time);
        row.push(&m.device_id);
        row.extend(m.metrics.iter().map(|x| x as &(dyn ToSql + Sync)));
        writer.as_mut().write(&row).await?;
    }

    writer.finish().await?;

    Ok(data.len())
}

fn get_col_types(num_metrics: u32) -> Vec<Type> {
    // Columns are a timestamp, a device id, and a f64 for each metric
    let mut col_types = vec![Type::TIMESTAMP, Type::OID];
    col_types.extend(iter::repeat(Type::FLOAT8).take(num_metrics as usize));
    col_types
}

fn get_copy_statement(table: &str, num_metrics: u32) -> String {
    let columns = (1..=num_metrics)
        .map(|c| format!("m{}", c))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "COPY {} (time, device_id, {}) FROM STDIN BINARY",
        table, columns
    )
}
