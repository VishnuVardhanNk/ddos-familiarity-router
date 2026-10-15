#!/usr/bin/env bash
cd "$(dirname "$0")/.."
START_AT="${1:-0}"
REPLAY_FRACTION="${2:-0.3}"
KAFKA="docker exec kafka /opt/kafka/bin"
SUBMIT="docker exec spark-master /opt/spark/bin/spark-submit"

echo "1/6 Starting the cluster"
docker compose up -d > /dev/null 2>&1
docker compose restart spark-master spark-worker spark-history grafana web > /dev/null 2>&1
until docker exec spark-master test -f /opt/spark/conf/spark-defaults.conf \
   && docker exec spark-master test -f /opt/jobs/ddos-detection.jar; do sleep 2; done
until curl -s localhost:8080/json/ | grep -Eq '"aliveworkers" *: *1'; do sleep 2; done
until docker exec namenode hdfs dfsadmin -safemode get 2> /dev/null | grep -q OFF; do sleep 3; done

echo "2/6 Stopping any earlier live run"
docker exec spark-master pkill -f LiveScorer 2> /dev/null
sleep 3

echo "3/6 Checking the traffic waiting in Kafka"
flows=$($KAFKA/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic flows 2> /dev/null | cut -d: -f3)
if [ -z "$flows" ] || [ "$flows" -eq 0 ]; then
  echo "    Kafka is empty: replaying ${REPLAY_FRACTION} of day 2 into it (about 10 minutes)"
  $KAFKA/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic flows --partitions 1 > /dev/null
  $SUBMIT --class ddos.stream.ReplayProducer /opt/jobs/ddos-detection.jar "$REPLAY_FRACTION" > logs/ReplayProducer.txt 2>&1
  flows=$($KAFKA/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic flows | cut -d: -f3)
fi
echo "    $flows flows ready"

echo "4/6 Checking the familiarity gate"
if ! docker exec namenode hdfs dfs -test -e /models/gate/limit/_SUCCESS; then
  echo "    Building it from day 1 (a few minutes, first time only)"
  $SUBMIT --class ddos.learn.Gate /opt/jobs/ddos-detection.jar > logs/BuildGate.txt 2>&1
fi

echo "5/6 Resetting the live results and the label memory"
docker exec namenode hdfs dfs -rm -r -f -skipTrash /checkpoints/live_scorer /checkpoints/analyst > /dev/null 2>&1
for topic in label_requests labels; do
  $KAFKA/kafka-topics.sh --bootstrap-server localhost:9092 --delete --if-exists --topic $topic > /dev/null 2>&1
done
sleep 2
for topic in label_requests labels; do
  $KAFKA/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $topic --partitions 1 > /dev/null 2>&1
done
docker exec postgres psql -U ddos -d ddos -q -c "drop table if exists live_scores, live_mistakes, live_batches, live_routing, live_traffic, live_reasons;" > /dev/null 2>&1

echo "6/6 Starting the live scorer"
nohup $SUBMIT --class ddos.stream.LiveScorer /opt/jobs/ddos-detection.jar 12000 "$START_AT" > logs/LiveScorer.txt 2>&1 &
until docker exec postgres psql -U ddos -d ddos -tAc "select count(*) from live_routing" 2> /dev/null | grep -Eq '^[1-9]'; do
  if ! docker exec spark-master pgrep -f LiveScorer > /dev/null; then
    echo "The live scorer stopped. Last lines of logs/LiveScorer.txt:"; tail -20 logs/LiveScorer.txt; exit 1
  fi
  sleep 2
done
docker exec -i postgres psql -U ddos -d ddos -q < infra/web/setup.sql > /dev/null 2>&1

echo
echo "Live. Open:"
echo "  My Network          http://localhost:8090"
echo "  Familiarity router  http://localhost:3000/d/ddos-router"
echo "  Live detection      http://localhost:3000/d/ddos-live"
echo "  Model Lab           http://localhost:3000/d/ddos-lab"
echo "  Spark jobs          http://localhost:4040"
echo "  Spark cluster       http://localhost:8080"
echo "  HDFS                http://localhost:9870"
echo
echo "Stop the live run with:  docker exec spark-master pkill -f LiveScorer"
