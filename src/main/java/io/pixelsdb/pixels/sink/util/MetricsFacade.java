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
 */
package io.pixelsdb.pixels.sink.util;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.freshness.FreshnessHistory;
import io.pixelsdb.pixels.sink.freshness.OneSecondAverage;
import io.pixelsdb.pixels.sink.writer.retina.SinkContextManager;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import io.prometheus.client.Summary;
import lombok.Setter;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class MetricsFacade
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsFacade.class);
    private static final PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
    private static MetricsFacade instance;
    private final boolean enabled;
    private final Counter tableChangeCounter;
    private final Counter rowChangeCounter;
    private final Counter transactionCounter;
    private final Counter serdRowRecordCounter;
    private final Counter serdTxRecordCounter;
    private final Summary processingLatency;
    private final Counter rawDataThroughputCounter;
    private final Counter debeziumEventCounter;
    private final Counter rowEventCounter;
    private final Summary transServiceLatency;
    private final Summary indexServiceLatency;
    private final Summary retinaServiceLatency;
    private final Summary writerLatency;
    private final Summary totalLatency;
    private final Summary tableFreshness;
    private final Histogram transactionRowCountHistogram;
    private final Histogram primaryKeyUpdateDistribution;

    private final boolean monitorReportEnabled;
    private final int monitorReportInterval;
    private final int freshnessReportInterval;

    private final SynchronizedDescriptiveStatistics freshness;
    private final SynchronizedDescriptiveStatistics rowChangeSpeed;
    private final OneSecondAverage freshnessAvg;
    private final Boolean freshnessVerbose;
    private final FreshnessHistory freshnessHistory;

    private final String monitorReportPath;
    private final String freshnessReportPath;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Thread reportThread;
    private final Thread freshnessThread;
    @Setter
    private SinkContextManager sinkContextManager;
    private long lastRowChangeCount = 0;
    private long lastTransactionCount = 0;
    private long lastDebeziumCount = 0;
    private long lastSerdRowRecordCount = 0;
    private long lastSerdTxRecordCount = 0;

    private boolean pkWarned = false;
    // Define this as a class member variable
    private long lastLogTimestampNano = System.nanoTime();

    private MetricsFacade(boolean enabled)
    {
        this.enabled = enabled;
        this.debeziumEventCounter = Counter.build()
                .name("debezium_event_total")
                .help("Debezium Event Total")
                .register();

        this.rowEventCounter = Counter.build()
                .name("row_event_total")
                .help("Debezium Row Event Total")
                .register();

        this.serdRowRecordCounter = Counter.build()
                .name("serd_row_record")
                .help("Serialized Row Record Total")
                .register();

        this.serdTxRecordCounter = Counter.build()
                .name("serd_tx_record")
                .help("Serialized Transaction Record Total")
                .register();

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
                .labelNames("table")
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

        this.tableFreshness = Summary.build()
                .name("data_freshness_latency_ms")
                .help("Data freshness latency in milliseconds per table")
                .labelNames("table")
                .quantile(0.5, 0.01)
                .quantile(0.9, 0.01)
                .quantile(0.99, 0.001)
                .register();

        this.transactionRowCountHistogram = Histogram.build()
                .name("transaction_row_count_histogram")
                .help("Distribution of row counts within a single transaction")
                .buckets(1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200)
                .register();
        this.primaryKeyUpdateDistribution = Histogram.build()
                .name("primary_key_update_distribution")
                .help("Distribution of primary key updates by logical bucket/hash for hot spot analysis")
                .labelNames("table") // Table name tag
                .buckets(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) // 10 buckets for distribution
                .register();
        this.freshness = new SynchronizedDescriptiveStatistics();
        this.rowChangeSpeed = new SynchronizedDescriptiveStatistics();

        freshnessReportInterval = config.getFreshnessReportInterval();
        freshnessReportPath = config.getMonitorFreshnessReportFile();
        freshnessAvg = new OneSecondAverage(freshnessReportInterval);
        freshnessVerbose = config.isSinkMonitorFreshnessVerbose();
        if (freshnessVerbose)
        {
            freshnessHistory = new FreshnessHistory();
        } else
        {
            freshnessHistory = null;
        }


        monitorReportEnabled = config.isMonitorReportEnabled();
        monitorReportInterval = config.getMonitorReportInterval();
        monitorReportPath = config.getMonitorReportFile();
        if (monitorReportEnabled)
        {
            running.set(true);
            reportThread = new Thread(this::run, "Metrics Report Thread");
            LOGGER.info("Metrics Report Thread Started");
            reportThread.start();
            freshnessThread = new Thread(this::runFreshness, "Freshness Thread");
            freshnessThread.start();
        } else
        {
            reportThread = null;
            freshnessThread = null;
        }
    }

    private static synchronized void initialize()
    {
        if (instance == null)
        {
            instance = new MetricsFacade(config.isMonitorEnabled());
            LOGGER.info("Init Metrics Facade");
        }
    }

    public static MetricsFacade getInstance()
    {
        if (instance == null)
        {
            initialize();
        }
        return instance;
    }

    public void stop()
    {
        running.set(false);
        if (reportThread != null)
        {
            reportThread.interrupt();
        }

        if (freshnessThread != null)
        {
            freshnessThread.interrupt();
        }
        LOGGER.info("Monitor report thread stopped.");
    }

    public void recordDebeziumEvent()
    {
        if (enabled && debeziumEventCounter != null)
        {
            debeziumEventCounter.inc();
        }
    }

    public void recordRowChange(String table, SinkProto.OperationType operation)
    {
        recordRowChange(table, operation, 1);
    }

    public void recordRowChange(String table, SinkProto.OperationType operation, int rows)
    {
        if (enabled && rowChangeCounter != null)
        {
            tableChangeCounter.labels(table).inc(rows);
            rowChangeCounter.labels(table, operation.toString()).inc(rows);
        }
    }

    public void recordSerdRowChange()
    {
        recordSerdRowChange(1);
    }

    public void recordSerdRowChange(int i)
    {
        if (enabled && serdRowRecordCounter != null)
        {
            serdRowRecordCounter.inc(i);
        }
    }

    public void recordSerdTxChange()
    {
        recordSerdTxChange(1);
    }

    public void recordSerdTxChange(int i)
    {
        if (enabled && serdTxRecordCounter != null)
        {
            serdTxRecordCounter.inc(i);
        }
    }

    public void recordTransaction(int i)
    {
        if (enabled && transactionCounter != null)
        {
            transactionCounter.inc(i);
        }
    }

    public void recordTransaction()
    {
        recordTransaction(1);
    }

    public Summary.Timer startProcessLatencyTimer()
    {
        return enabled ? processingLatency.startTimer() : null;
    }

    public Summary.Timer startIndexLatencyTimer()
    {
        return enabled ? indexServiceLatency.startTimer() : null;
    }

    public Summary.Timer startTransLatencyTimer()
    {
        return enabled ? transServiceLatency.startTimer() : null;
    }

    public Summary.Timer startRetinaLatencyTimer()
    {
        return enabled ? retinaServiceLatency.startTimer() : null;
    }

    public Summary.Timer startWriteLatencyTimer(String tableName)
    {
        return enabled ? writerLatency.labels(tableName).startTimer() : null;
    }

    public void addRawData(double data)
    {
        rawDataThroughputCounter.inc(data);
    }

    public void recordTotalLatency(RowChangeEvent event)
    {
        if (event.getTimeStamp() != 0)
        {
            long recordLatency = System.currentTimeMillis() - event.getTimeStamp();
            totalLatency.labels(event.getFullTableName(), event.getOp().toString()).observe(recordLatency);
        }
    }

    public void recordRowEvent()
    {
        recordRowEvent(1);
    }

    public void recordRowEvent(int i)
    {
        if (enabled && rowEventCounter != null)
        {
            rowEventCounter.inc(i);
        }
    }

    public int getRecordRowEvent()
    {
        return (int) rowEventCounter.get();
    }

    public int getTransactionEvent()
    {
        return (int) transactionCounter.get();
    }

    public void recordTableFreshness(String table, double freshnessMill)
    {
        if (!enabled)
        {
            return;
        }

        tableFreshness.labels(table).observe(freshnessMill);
        recordFreshness(freshnessMill);
    }

    public void recordTableFreshness(
            String table,
            double freshnessMill,
            double queryTimeMill
    )
    {
        if (!enabled)
        {
            return;
        }
        tableFreshness.labels(table).observe(freshnessMill);
        recordFreshness(freshnessMill);
        if (freshnessVerbose && freshnessHistory != null)
        {
            freshnessHistory.record(freshnessMill, queryTimeMill);
        }
    }

    public void recordFreshness(double freshnessMill)
    {
        if (!enabled)
        {
            return;
        }

        if (freshness != null)
        {
            freshness.addValue(freshnessMill);
        }

        if (freshnessAvg != null)
        {
            freshnessAvg.record(freshnessMill);
        }
    }

    public void recordPrimaryKeyUpdateDistribution(String table, ByteString pkValue)
    {
        if (!enabled || primaryKeyUpdateDistribution == null)
        {
            return;
        }
        if (pkValue == null || pkValue.isEmpty())
        {
            LOGGER.debug("Skipping PK distribution recording: pkValue is null or empty for table {}.", table);
            return;
        }

        long numericPK;
        int length = pkValue.size();

        try
        {
            ByteBuffer buffer = pkValue.asReadOnlyByteBuffer();

            if (length == Integer.BYTES)
            {
                numericPK = Integer.toUnsignedLong(buffer.getInt());
            } else if (length == Long.BYTES)
            {
                numericPK = buffer.getLong();
            } else
            {
                if (!pkWarned)
                {
                    LOGGER.warn("Unsupported PK ByteString length {} for table {}. Expected 4 or 8.", length, table);
                    pkWarned = true;
                }
                return;
            }
        } catch (Exception e)
        {
            LOGGER.error("Failed to convert ByteString to numeric type for table {}: {}", table, e.getMessage());
            return;
        }
        int hash = Long.hashCode(numericPK);
        double bucketIndex = (Math.abs(hash % 10)) + 1;

        // 3. 记录到 Histogram
        primaryKeyUpdateDistribution.labels(table).observe(bucketIndex);

        LOGGER.debug("Table {}: PK {} mapped to bucket index {}", table, numericPK, bucketIndex);
    }

    public void recordTransactionRowCount(int rowCount)
    {
        if (enabled && transactionRowCountHistogram != null)
        {
            // Use observe() to add the value to the Histogram's configured buckets.
            transactionRowCountHistogram.observe(rowCount);
        }
    }

    public void run()
    {
        while (running.get())
        {
            try
            {
                Thread.sleep(monitorReportInterval);
                logPerformance();
            } catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                break;
            } catch (Throwable t)
            {
                LOGGER.warn("Error while reporting performance.", t);
            }
        }
    }

    public void runFreshness()
    {
        try
        {
            Thread.sleep(monitorReportInterval);
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
        while (running.get())
        {
            try
            {
                Thread.sleep(freshnessReportInterval);
                try (FileWriter fw = new FileWriter(freshnessReportPath, true))
                {
                    if (freshnessVerbose)
                    {
                        List<FreshnessHistory.Record> detailedRecords = freshnessHistory.pollAll();
                        if (!detailedRecords.isEmpty())
                        {
                            for (FreshnessHistory.Record record : detailedRecords)
                            {
                                fw.write(record.toString() + "\n");
                            }
                            fw.flush();
                        }
                    } else
                    {
                        long now = System.currentTimeMillis();
                        double avg = freshnessAvg.getWindowAverage();
                        if (Double.isNaN(avg))
                        {
                            continue;
                        }
                        fw.write(now + "," + avg + "\n");
                        fw.flush();
                    }
                } catch (IOException e)
                {
                    LOGGER.warn("Failed to write perf metrics: " + e.getMessage());
                }
            } catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                break;
            } catch (Throwable t)
            {
                LOGGER.warn("Error while reporting performance.", t);
            }
        }
    }

    public void logPerformance()
    {
        // 1. Calculate actual elapsed time since the last execution
        long currentTimestampNano = System.nanoTime();
        long elapsedNano = currentTimestampNano - lastLogTimestampNano;

        // 2. Prevent division by zero and handle edge cases
        if (elapsedNano <= 0)
        {
            return;
        }

        // Convert nanoseconds to seconds for accurate rate calculation
        double seconds = elapsedNano / 1_000_000_000.0;
        lastLogTimestampNano = currentTimestampNano;

        // 3. Capture current counter values
        long currentRows = (long) rowEventCounter.get();
        long currentTxns = (long) transactionCounter.get();
        long currentDebezium = (long) debeziumEventCounter.get();
        long currentSerdRows = (long) serdRowRecordCounter.get();
        long currentSerdTxs = (long) serdTxRecordCounter.get();

        // 4. Calculate the delta (change) since the last log
        long deltaRows = currentRows - lastRowChangeCount;
        long deltaTxns = currentTxns - lastTransactionCount;
        long deltaDebezium = currentDebezium - lastDebeziumCount;
        long deltaSerdRows = currentSerdRows - lastSerdRowRecordCount;
        long deltaSerdTxs = currentSerdTxs - lastSerdTxRecordCount;

        // 5. Update last counts for the next cycle
        lastRowChangeCount = currentRows;
        lastTransactionCount = currentTxns;
        lastDebeziumCount = currentDebezium;
        lastSerdRowRecordCount = currentSerdRows;
        lastSerdTxRecordCount = currentSerdTxs;

        // 6. Calculate Operations Per Second (OIPS) based on actual seconds elapsed
        double rowOips = deltaRows / seconds;
        double txnOips = deltaTxns / seconds;
        double dbOips = deltaDebezium / seconds;
        double serdRowsOips = deltaSerdRows / seconds;
        double serdTxsOips = deltaSerdTxs / seconds;

        rowChangeSpeed.addValue(rowOips);

        // 7. Log detailed performance metrics
        LOGGER.info(
                "Performance report: +{} rows (+{}/s), +{} transactions (+{}/s), +{} debezium (+{}/s)" +
                        ", +{} serdRows (+{}/s), +{} serdTxs (+{}/s)" +
                        " in {} ms (Actual: {} ms)\t activeTxNum: {} min Tx: {}",
                deltaRows, String.format("%.2f", rowOips),
                deltaTxns, String.format("%.2f", txnOips),
                deltaDebezium, String.format("%.2f", dbOips),
                deltaSerdRows, String.format("%.2f", serdRowsOips),
                deltaSerdTxs, String.format("%.2f", serdTxsOips),
                monitorReportInterval,
                String.format("%.2f", seconds * 1000), // Actual interval in ms
                sinkContextManager.getActiveTxnsNum(),
                sinkContextManager.findMinActiveTx()
        );

        // 8. Log statistical summaries
        LOGGER.info(
                String.format(
                        "Row Per/Second Summary: Max=%.2f, Min=%.2f, Mean=%.2f, P10=%.2f, P50=%.2f, P90=%.2f, P95=%.2f, P99=%.2f",
                        rowChangeSpeed.getMax(),
                        rowChangeSpeed.getMin(),
                        rowChangeSpeed.getMean(),
                        rowChangeSpeed.getPercentile(10),
                        rowChangeSpeed.getPercentile(50),
                        rowChangeSpeed.getPercentile(90),
                        rowChangeSpeed.getPercentile(95),
                        rowChangeSpeed.getPercentile(99)
                )
        );

        // 9. Append metrics to CSV for analysis and plotting
        String time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        try (FileWriter fw = new FileWriter(monitorReportPath, true))
        {
            // Format: time, rows/s, txns/s, debezium/s, serdRows/s, serdTxs/s
            fw.write(String.format("%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.4f%n",
                    time, rowOips, txnOips, dbOips, serdRowsOips, serdTxsOips, seconds));
        } catch (IOException e)
        {
            LOGGER.warn("Failed to write performance metrics to CSV: " + e.getMessage());
        }
    }
}