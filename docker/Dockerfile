FROM rust:latest as build

WORKDIR /app

COPY Cargo.lock Cargo.toml ./
COPY src src

RUN cargo build --release --all

FROM debian:buster-slim

WORKDIR /app
COPY --from=build /app/target/release/tsdbperf ./

ENTRYPOINT ["/app/tsdbperf"]
CMD ["--help"]
