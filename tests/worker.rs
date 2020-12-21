use std::sync::Mutex;

use anyhow::Result;
use lazy_static::lazy_static;
use structopt::StructOpt;

use tsdbperf::db;

lazy_static! {
    static ref DB_LOCK: Mutex<()> = Mutex::new(());
}

struct Runner {
    devices: u32,
    measurements: u32,
    metrics: u32,
    copy_upserts: bool,
    upserts: bool,
    jsonb: bool,
}

impl Runner {
    fn new() -> Self {
        Self {
            devices: 15,
            measurements: 1000,
            metrics: 5,
            copy_upserts: false,
            upserts: false,
            jsonb: false,
        }
    }

    fn opt(&self) -> db::DbOpt {
        let metrics = self.metrics.to_string();
        let mut args = vec!["dbtest", "--metrics", &metrics];
        if self.jsonb {
            args.push("--with-jsonb");
        } else if self.upserts {
            args.push("--with-upserts");
        } else if self.copy_upserts {
            args.push("--with-copy-upserts");
        }

        db::DbOpt::from_iter(&args)
    }

    async fn run(&self) -> Result<()> {
        let (written, rows) = {
            // Each run recreates the schema so we need to serialize them.
            // Use a block to prevent asserts from poisoning the mutex.
            let _guard = DB_LOCK.lock().unwrap();

            let opt = self.opt();
            db::init(&opt).await?;

            let written = db::run_worker(&opt, 1, self.devices, self.measurements).await?;
            let db = db::Db::connect(&opt).await?;
            let rows = db.query("select * from measurement").await?;
            (written, rows)
        };

        assert_eq!(written as u32, self.devices * self.measurements);
        assert_eq!(written, rows.len());

        if self.jsonb {
            // Time, device_id and jsonb metrics
            assert_eq!(rows[0].len(), 3);
        } else {
            // Time, device_id and metrics columns
            assert_eq!(rows[0].len(), 2 + self.metrics as usize);
        }

        Ok(())
    }
}

#[tokio::test]
async fn copy_in() -> Result<()> {
    let runner = Runner::new();
    runner.run().await
}

#[tokio::test]
async fn copy_upsert() -> Result<()> {
    let runner = Runner {
        copy_upserts: true,
        ..Runner::new()
    };

    runner.run().await
}

#[tokio::test]
async fn upsert() -> Result<()> {
    let runner = Runner {
        upserts: true,
        ..Runner::new()
    };

    runner.run().await
}

#[tokio::test]
async fn jsonb() -> Result<()> {
    let runner = Runner {
        jsonb: true,
        ..Runner::new()
    };

    runner.run().await
}
