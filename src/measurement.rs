use chrono::{Duration, NaiveDateTime, Utc};
use rand::distributions::{Distribution, Uniform};
use rand_distr::StandardNormal;

#[derive(Debug, Clone)]
pub struct Measurement {
    pub time: NaiveDateTime,
    pub device_id: i32,
    pub metrics: Vec<f64>,
}

pub struct MeasurementIterator {
    measurements: Vec<Measurement>,
    next_device: usize,
    rng: rand::rngs::ThreadRng,
}

impl MeasurementIterator {
    pub fn new(worker_id: u32, num_devices: u32, num_metrics: u32, num_measurements: u32) -> Self {
        let device_ids = (1..=num_devices)
            .map(|id| worker_id * 1000 + id)
            .collect::<Vec<_>>();

        let time = Utc::now().naive_utc() - Duration::seconds(num_measurements as i64);
        let mut rng = rand::thread_rng();
        let dis = Uniform::new(100.0, 1000.0);

        let measurements = device_ids
            .iter()
            .map(|did| {
                let metrics = (0..num_metrics).map(|_| dis.sample(&mut rng)).collect();
                Measurement {
                    time,
                    device_id: *did as i32,
                    metrics,
                }
            })
            .collect();

        Self {
            measurements,
            next_device: 0,
            rng,
        }
    }
}

impl Iterator for MeasurementIterator {
    type Item = Measurement;

    fn next(&mut self) -> Option<Self::Item> {
        use rand::prelude::*;

        let n = self.measurements.len();
        let m = &mut self.measurements[self.next_device];
        self.next_device = (self.next_device + 1) % n;

        m.time += Duration::seconds(1);

        // Choose one value to change
        let idx = self.rng.sample(Uniform::from(0..m.metrics.len()));

        // Simulate random walk
        let v: f64 = self.rng.sample(StandardNormal);
        m.metrics[idx] += m.metrics[idx] * 0.0005 * v;

        Some(m.clone())
    }
}
