FROM rust:1.89-bookworm AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release --locked --bins

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /var/lib/turbovec \
    && chown 65532:65532 /var/lib/turbovec
COPY --from=build /src/target/release/turbovec-grpc /usr/local/bin/turbovec-grpc
COPY --from=build /src/target/release/turbovec-coordinator /usr/local/bin/turbovec-coordinator

USER 65532:65532
VOLUME ["/var/lib/turbovec"]
EXPOSE 50050 50051 9090
ENV TURBOVEC_DATA_DIR=/var/lib/turbovec
ENTRYPOINT ["/usr/local/bin/turbovec-grpc"]
