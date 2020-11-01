use chrono::{Duration, NaiveDateTime, Utc};
use rand::distributions::{Distribution, Uniform};
use rand_distr::StandardNormal;

#[derive(Debug, Clone)]
pub struct Measurement {
    pub time: NaiveDateTime,
    pub device_id: u32,
    pub metrics: Vec<f64>,
}

pub struct MeasurementIterator {
    worker_id: u32,
    num_devices: u32,
    metrics: Vec<f64>,
    time: NaiveDateTime,
    cur_device: u32,
    rng: rand::rngs::ThreadRng,
}

impl MeasurementIterator {
    pub fn new(worker_id: u32, num_devices: u32, num_metrics: u32, num_measurements: u32) -> Self {
        let time = Utc::now().naive_utc() - Duration::seconds(num_measurements as i64);
        let mut rng = rand::thread_rng();
        let dis = Uniform::new(100.0, 1000.0);
        let metrics = (0..num_metrics).map(|_| dis.sample(&mut rng)).collect();

        Self {
            worker_id,
            num_devices,
            metrics,
            time,
            cur_device: 0,
            rng,
        }
    }
}

impl Iterator for MeasurementIterator {
    type Item = Measurement;

    fn next(&mut self) -> Option<Self::Item> {
        use rand::prelude::*;

        let device_id = self.worker_id | self.cur_device << 8;
        let mut metrics = self.metrics.clone();
        metrics.rotate_left(self.cur_device as usize  % self.metrics.len());

        self.cur_device = (self.cur_device + 1) % self.num_devices;
        if self.cur_device == 0 {
            self.time += Duration::seconds(1);

            for m in self.metrics.iter_mut() {
                let v: f64 = self.rng.sample(StandardNormal);
                *m += *m * 0.0005 * v;
            }
        }

        Some(Measurement {
            time: self.time,
            device_id,
            metrics,
        })
    }
}
