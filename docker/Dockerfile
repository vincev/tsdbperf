FROM rust:latest as build

WORKDIR /app

COPY Cargo.lock Cargo.toml ./
COPY tsdbperf tsdbperf
COPY xtask xtask

RUN cargo build --manifest-path /app/tsdbperf/Cargo.toml --bin tsdbperf --release

FROM debian:buster-slim

WORKDIR /app
COPY --from=build /app/target/release/tsdbperf ./

ENTRYPOINT ["/app/tsdbperf"]
CMD ["--help"]
