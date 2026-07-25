# Example Dockerfile for turbovec-grpc.
#
# This is a starting point for running the server in a container, not a
# hardened production image: it runs as root, sets no resource limits, and
# does no TLS termination. See docs/deployment.md before deploying for real.
#
# Build from the repo root (the workspace is the build context):
#   docker build -f turbovec-grpc/Dockerfile -t turbovec-grpc .
# Run:
#   docker run --rm -p 50051:50051 turbovec-grpc

FROM rust:1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
        protobuf-compiler libopenblas-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
# The repo's .cargo/config.toml targets x86-64-v3, so the resulting binary
# needs a Haswell-or-newer CPU at runtime — same as any local build.
RUN cargo build --release -p turbovec-grpc

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        libopenblas0 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/turbovec-grpc /usr/local/bin/turbovec-grpc
EXPOSE 50051
ENTRYPOINT ["turbovec-grpc"]
