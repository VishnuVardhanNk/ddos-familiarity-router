#!/usr/bin/env bash
cd "$(dirname "$0")/.."
docker compose up -d
docker compose restart spark-master spark-worker spark-history
docker compose ps
