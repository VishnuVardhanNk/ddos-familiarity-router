#!/usr/bin/env bash
set -u
cd "$(dirname "$0")/.."
for job in "$@"; do
  echo "$(date '+%F %T') starting $job"
  docker exec spark-master /opt/spark/bin/spark-submit --class "ddos.ingest.$job" /opt/jobs/ddos-detection.jar > "logs/$job.txt" 2>&1
  code=$?
  echo "EXIT=$code" >> "logs/$job.txt"
  echo "$(date '+%F %T') finished $job with exit code $code"
  [ "$code" -eq 0 ] || exit "$code"
done
