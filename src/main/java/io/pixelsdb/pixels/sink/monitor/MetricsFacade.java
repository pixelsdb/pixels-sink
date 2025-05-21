/*
 * Copyright 2025 PixelsDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.pixelsdb.pixels.sink.monitor;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;

public class MetricsFacade {
    private static MetricsFacade instance;
    private final boolean enabled;
    private final Counter tableChangeCounter;
    private final Counter rowChangeCounter;
    private final Counter transactionCounter;
    private final Summary processingLatency;
    private final Counter rawDataThroughputCounter;

    private final Summary transServiceLatency;
    private final Summary indexServiceLatency;
    private final Summary retinaServiceLatency;
    private final Summary writerLatency;
    private final Summary totalLatency;
    private MetricsFacade(boolean enabled) {
        this.enabled = enabled;
        if (enabled) {
            this.tableChangeCounter = Counter.build()
                    .name("sink_table_changes_total")
                    .help("Total processed table changes")
                    .labelNames("table")
                    .register();

            this.rowChangeCounter = Counter.build()
                    .name("sink_row_changes_total")
                    .help("Total processed row changes")
                    .labelNames("table", "operation")
                    .register();

            this.transactionCounter = Counter.build()
                    .name("sink_transactions_total")
                    .help("Total committed transactions")
                    .register();

            this.processingLatency = Summary.build()
                    .name("sink_processing_latency_seconds")
                    .help("End-to-end processing latency")
                    .quantile(0.5, 0.05)
                    .quantile(0.75, 0.01)
                    .quantile(0.95, 0.005)
                    .quantile(0.99, 0.001)
                    .register();

            this.rawDataThroughputCounter = Counter.build()
                    .name("sink_data_throughput_counter")
                    .help("Data throughput")
                    .register();

            this.transServiceLatency = Summary.build()
                    .name("trans_service_latency_seconds")
                    .help("End-to-end processing latency")
                    .quantile(0.5, 0.05)
                    .quantile(0.75, 0.01)
                    .quantile(0.95, 0.005)
                    .quantile(0.99, 0.001)
                    .register();

            this.indexServiceLatency = Summary.build()
                    .name("index_service_latency_seconds")
                    .help("End-to-end processing latency")
                    .quantile(0.5, 0.05)
                    .quantile(0.75, 0.01)
                    .quantile(0.95, 0.005)
                    .quantile(0.99, 0.001)
                    .register();

            this.retinaServiceLatency = Summary.build()
                    .name("retina_service_latency_seconds")
                    .help("End-to-end processing latency")
                    .quantile(0.5, 0.05)
                    .quantile(0.75, 0.01)
                    .quantile(0.95, 0.005)
                    .quantile(0.99, 0.001)
                    .register();

            this.writerLatency = Summary.build()
                    .name("write_latency_seconds")
                    .help("Write latency")
                    .quantile(0.5, 0.05)
                    .quantile(0.75, 0.01)
                    .quantile(0.95, 0.005)
                    .quantile(0.99, 0.001)
                    .register();

            this.totalLatency = Summary.build()
                    .name("total_latency_seconds")
                    .help("total latency to ETL a row change event")
                    .labelNames("table", "operation")
                    .quantile(0.5, 0.05)
                    .quantile(0.75, 0.01)
                    .quantile(0.95, 0.005)
                    .quantile(0.99, 0.001)
                    .register();

        } else {
            this.rowChangeCounter = null;
            this.transactionCounter = null;
            this.processingLatency = null;
            this.tableChangeCounter = null;
            this.rawDataThroughputCounter = null;
            this.transServiceLatency = null;
            this.indexServiceLatency = null;
            this.retinaServiceLatency = null;
            this.writerLatency = null;
            this.totalLatency = null;
        }
    }

    public static synchronized void initialize() {
        PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
        if (instance == null) {
            instance = new MetricsFacade(config.isMonitorEnabled());
        }
    }

    public static MetricsFacade getInstance() {
        if (instance == null) {
            initialize();
        }
        return instance;
    }

    public void recordRowChange(String table, SinkProto.OperationType operation) {
        recordRowChange(table, operation, 1);
    }

    public void recordRowChange(String table, SinkProto.OperationType operation, int rows) {
        if (enabled && rowChangeCounter != null) {
            tableChangeCounter.labels(table).inc(rows);
            rowChangeCounter.labels(table, operation.toString()).inc(rows);
        }
    }

    public void recordTransaction() {
        if (enabled && transactionCounter != null) {
            transactionCounter.inc();
        }
    }

    public Summary.Timer startProcessLatencyTimer() {
        return enabled ? processingLatency.startTimer() : null;
    }

    public Summary.Timer startIndexLatencyTimer() {
        return enabled ? indexServiceLatency.startTimer() : null;
    }

    public Summary.Timer startTransLatencyTimer() {
        return enabled ? transServiceLatency.startTimer() : null;
    }

    public Summary.Timer startRetinaLatencyTimer() {
        return enabled ? retinaServiceLatency.startTimer() : null;
    }

    public Summary.Timer startWriteLatencyTimer() {
        return enabled ? writerLatency.startTimer() : null;
    }

    public void addRawData(double data) {
        rawDataThroughputCounter.inc(data);
    }

    public void recordTotalLatency(RowChangeEvent event) {
        if(event.getTimeStamp() != 0) {
            long recordLatency = System.currentTimeMillis()- event.getTimeStamp();
            totalLatency.labels(event.getFullTableName(), event.getOp().toString()).observe(recordLatency);
        }
    }
}