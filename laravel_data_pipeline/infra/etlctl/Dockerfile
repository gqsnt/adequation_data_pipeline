# syntax=docker/dockerfile:1

ARG PUID=1000
ARG PGID=1000


############################
# Builder
############################
from rust:1.90-bullseye as builder
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates pkg-config build-essential clang git libssl-dev \
  && rm -rf /var/lib/apt/lists/*

# --- Build réel (on copie tout le code avant) ---
COPY etlctl/src ./src
COPY etlctl/Cargo.toml .
RUN cargo build

############################
# Runtime

############################
FROM debian:bullseye-slim
WORKDIR /app

ARG PUID
ARG PGID

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl  libc6 libstdc++6 libgcc-s1 \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/debug/etlctl /app/etlctl
RUN curl https://install.duckdb.org | sh;



# User non-root (robuste)
RUN set -eux; \
    if getent group "${PGID}" >/dev/null; then GNAME="$(getent group "${PGID}" | cut -d: -f1)"; \
    else groupadd -g "${PGID}" app; GNAME="app"; fi; \
    if id -u "${PUID}" >/dev/null 2>&1; then UNAME="$(getent passwd "${PUID}" | cut -d: -f1)"; \
      usermod -l app "${UNAME}" || true; usermod -g "${GNAME}" -u "${PUID}" app || true; \
    else useradd -u "${PUID}" -g "${GNAME}" -m app; fi

USER app

EXPOSE 8080
ENV RUST_LOG=info
# Ton main n'a pas de sous-commande -> on lance juste le binaire
CMD ["/app/etlctl"]
