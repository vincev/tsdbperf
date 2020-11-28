use std::iter;

use anyhow::Result;
use futures::pin_mut;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Client, Statement, Transaction};

use super::DbOpt;
use crate::measurement::Measurement;

pub struct Command {
    command: CommandType,
}

impl Command {
    pub async fn new(opt: &DbOpt, client: &Client) -> Result<Self> {
        let command = if opt.do_upserts {
            CommandType::Upsert(Upsert::new(opt, client).await?)
        } else if opt.do_copy_upserts {
            CommandType::CopyInUpsert(CopyInUpsert::new(opt, client).await?)
        } else {
            CommandType::CopyIn(CopyIn::new(opt)?)
        };

        Ok(Command { command })
    }

    pub async fn execute(&self, client: &mut Client, data: Vec<Measurement>) -> Result<usize> {
        let tx = client.transaction().await?;

        let num_written = match &self.command {
            CommandType::CopyIn(cmd) => cmd.execute(&tx, data).await?,
            CommandType::CopyInUpsert(cmd) => cmd.execute(&tx, data).await?,
            CommandType::Upsert(cmd) => cmd.execute(&tx, data).await?,
        };

        tx.commit().await?;
        Ok(num_written)
    }
}

enum CommandType {
    CopyIn(CopyIn),
    CopyInUpsert(CopyInUpsert),
    Upsert(Upsert),
}

struct CopyIn {
    col_types: Vec<Type>,
    copy_stm: String,
}

impl CopyIn {
    fn new(opt: &DbOpt) -> Result<Self> {
        let mut col_types = vec![Type::TIMESTAMP, Type::OID];
        col_types.extend(iter::repeat(Type::FLOAT8).take(opt.num_metrics as usize));

        Ok(Self {
            col_types,
            copy_stm: get_copy_statement("measurement", opt.num_metrics),
        })
    }

    async fn execute(&self, tx: &Transaction<'_>, data: Vec<Measurement>) -> Result<usize> {
        let sink = tx.copy_in(self.copy_stm.as_str()).await?;
        let writer = BinaryCopyInWriter::new(sink, &self.col_types);
        write(writer, &data).await
    }
}

// An upsert operation happens when we try to insert a row that
// violates a unique constraint, in this case a row with the same time
// and device id. To keep ingestion rate high we use copy in binary on
// a temp table and then insert its data into the final table using an
// ON CONFLICT update statement.
struct CopyInUpsert {
    col_types: Vec<Type>,
    copy_stm: String,
    upsert_stm: String,
}

impl CopyInUpsert {
    async fn new(opt: &DbOpt, client: &Client) -> Result<Self> {
        client
            .batch_execute(
                "CREATE TEMP TABLE upserts ON COMMIT DELETE ROWS \
                 AS TABLE measurement WITH NO DATA;",
            )
            .await?;

        let set_columns = (1..=opt.num_metrics)
            .map(|c| format!("m{} = EXCLUDED.m{}", c, c))
            .collect::<Vec<_>>()
            .join(", ");

        let upsert_stm = format!(
            "INSERT INTO measurement
             SELECT * FROM upserts
             ON CONFLICT (device_id, time) DO UPDATE SET {}",
            set_columns
        );

        let mut col_types = vec![Type::TIMESTAMP, Type::OID];
        col_types.extend(iter::repeat(Type::FLOAT8).take(opt.num_metrics as usize));

        Ok(Self {
            col_types,
            copy_stm: get_copy_statement("upserts", opt.num_metrics),
            upsert_stm,
        })
    }

    async fn execute(&self, tx: &Transaction<'_>, data: Vec<Measurement>) -> Result<usize> {
        let sink = tx.copy_in(self.copy_stm.as_str()).await?;
        let writer = BinaryCopyInWriter::new(sink, &self.col_types);
        let num_written = write(writer, &data).await?;
        tx.batch_execute(self.upsert_stm.as_str()).await?;
        Ok(num_written)
    }
}

// The CopyInUpsert above uses PostgreSQL `copy-in` to send data to
// the server as fast as possible but to work around the limitation
// that copy-in doesn't support upserts we copy the data to a temp
// table and then do an upsert from the temp table to the destination
// table.
// Mat from Timescale suggested another approach, we run an insert
// with ON CONFLICT UPDATE but instead of inserting a single row
// we insert N rows by passing an array of N values for each column
// and then use unnest to insert them as multiple rows.
struct Upsert {
    statement: Statement,
    num_metrics: u32,
}

impl Upsert {
    async fn new(opt: &DbOpt, client: &Client) -> Result<Self> {
        let insert_cols = (1..=opt.num_metrics)
            .map(|c| format!("m{}", c))
            .collect::<Vec<_>>()
            .join(", ");

        let unnest_cols = (1..=opt.num_metrics)
            .map(|c| format!("${}", c + 2))
            .collect::<Vec<_>>()
            .join(", ");

        let conflict_cols = (1..=opt.num_metrics)
            .map(|c| format!("m{} = EXCLUDED.m{}", c, c))
            .collect::<Vec<_>>()
            .join(", ");

        let insert_stm = format!(
            "INSERT INTO measurement(time, device_id, {})
             SELECT * FROM unnest($1, $2, {}) as U
             ON CONFLICT (device_id, time) DO UPDATE SET {}",
            insert_cols, unnest_cols, conflict_cols
        );

        // When using unnest each column is an array
        let mut col_types = vec![Type::TIMESTAMP_ARRAY, Type::OID_ARRAY];
        col_types.extend(iter::repeat(Type::FLOAT8_ARRAY).take(opt.num_metrics as usize));

        Ok(Self {
            statement: client.prepare_typed(&insert_stm, &col_types).await?,
            num_metrics: opt.num_metrics,
        })
    }

    async fn execute(&self, tx: &Transaction<'_>, data: Vec<Measurement>) -> Result<usize> {
        let mut times = Vec::with_capacity(data.len());
        let mut device_ids = Vec::with_capacity(data.len());
        let mut metrics = (0..self.num_metrics)
            .map(|_| Vec::with_capacity(data.len()))
            .collect::<Vec<_>>();

        for m in &data {
            times.push(&m.time);
            device_ids.push(&m.device_id);
            m.metrics
                .iter()
                .zip(metrics.iter_mut())
                .for_each(|(m, v)| v.push(*m));
        }

        let mut cols: Vec<&'_ (dyn ToSql + Sync)> = Vec::new();
        cols.push(&times);
        cols.push(&device_ids);
        metrics.iter().for_each(|v| cols.push(v));

        tx.execute(&self.statement, &cols).await?;
        Ok(data.len())
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
