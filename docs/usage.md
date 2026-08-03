# Usage

This document describes how to run Pixels Sink and how the runtime is wired.

**Entry Point**
- Main class: `io.pixelsdb.pixels.sink.PixelsSinkApp`
- CLI options: `-c, --config` for the properties file path

**Run with Maven**
```bash
mvn -q -DskipTests exec:java \
  -Dexec.mainClass=io.pixelsdb.pixels.sink.PixelsSinkApp \
  -Dexec.args="-c conf/pixels-sink.pg.properties"
```

**Run with scripts**

```bash
./pixels-sink
```

**Run from an IDE**
- Set the main class to `io.pixelsdb.pixels.sink.PixelsSinkApp`
- Add program args: `-c conf/pixels-sink.pg.properties`

**Pipeline Summary**
- A source reads change events from Debezium Engine, Kafka, or storage files.
- Conversion normalizes source records into canonical Pixels events.
- Pipelines route events through bounded queues to their processors.
- Processors enforce ordering by table and write to the configured sink.
- The sink writer persists the events to Retina, CSV, Proto, Flink, or a no-op sink.

**Lifecycle Notes**
- Metrics and freshness reporting are configured in the properties file.
- If Prometheus metrics are enabled, the HTTP server is started on `sink.monitor.port`.
- If freshness checking is set to `embed`, the freshness client is started with the configured Trino settings.

**Example Configs**
- `conf/pixels-sink.pg.properties` for Postgres + Kafka
- `conf/pixels-sink.aws.properties` for AWS environments, using retina
- `conf/pixels-sink.flink.properties` for Flink sink
