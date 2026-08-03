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
package io.pixelsdb.pixels.sink.freshness;

import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.transaction.TransService;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.util.DateUtil;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.*;

/**
 * FreshnessClient monitors data freshness by periodically querying max(freshness_ts)
 * via JDBC (Trino by default, or HiveServer2 for Hudi when built with {@code -Phudi-hive}).
 */
public class FreshnessClient
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FreshnessClient.class);
    private static final int QUERY_INTERVAL_SECONDS = 1;
    private static volatile FreshnessClient instance;
    // Configuration parameters (should ideally be loaded from a config file)
    private final String queryJdbcUrl;
    private final String queryUser;
    private final String queryPassword;
    private final QueryEngine queryEngine;
    private final int maxConcurrentQueries;
    private final Semaphore queryPermits;
    private final ThreadPoolExecutor connectionExecutor;
    // Key modification: Use a thread-safe Set to maintain the list of tables to monitor dynamically.
    private final Set<String> monitoredTables;
    private final ScheduledExecutorService scheduler;
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private final int warmUpSeconds;
    private final PixelsSinkConfig config;

    private enum QueryEngine
    {
        TRINO("io.trino.jdbc.TrinoDriver",
                "Trino JDBC driver not found on classpath."),
        HIVE("org.apache.hive.jdbc.HiveDriver",
                "Hive JDBC driver not found. Build with -Phudi-hive to enable Hudi freshness via HiveServer2.");

        private final String driverClass;
        private final String missingHint;

        QueryEngine(String driverClass, String missingHint)
        {
            this.driverClass = driverClass;
            this.missingHint = missingHint;
        }
    }

    private FreshnessClient()
    {
        // Initializes the set with thread safety wrapper
        this.monitoredTables = Collections.synchronizedSet(new HashSet<>());

        this.config = PixelsSinkConfigFactory.getInstance();
        this.queryUser = config.getSinkQueryUser();
        this.queryJdbcUrl = config.getSinkQueryUrl();
        this.queryPassword = config.getSinkQueryPassword();
        this.queryEngine = resolveQueryEngine(queryJdbcUrl);
        this.warmUpSeconds = config.getSinkMonitorFreshnessEmbedWarmupSeconds();
        this.maxConcurrentQueries = config.getSinkQueryParallel();
        this.queryPermits = new Semaphore(maxConcurrentQueries);
        // Initializes a single-threaded scheduler for executing freshness queries
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r ->
        {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setName("Freshness-Client-Scheduler");
            t.setDaemon(true);
            return t;
        });

        this.connectionExecutor = new ThreadPoolExecutor(
                maxConcurrentQueries,
                maxConcurrentQueries,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                r ->
                {
                    Thread t = Executors.defaultThreadFactory().newThread(r);
                    t.setName("Freshness-Query-Worker");
                    t.setDaemon(true);
                    return t;
                }
        );
        this.connectionExecutor.allowCoreThreadTimeOut(true);
    }

    public static FreshnessClient getInstance()
    {
        if (instance == null)
        {
            // First check: Reduces synchronization overhead once the instance is created
            synchronized (FreshnessClient.class)
            {
                if (instance == null)
                {
                    // Second check: Only one thread proceeds to create the instance
                    instance = new FreshnessClient();
                }
            }
        }
        return instance;
    }

    /**
     * Unified JDBC entry: resolve engine from URL at construction, load driver,
     * apply engine-specific connection properties, then return a ready Connection.
     *
     * @param queryTimestamp Trino snapshot timestamp when embed.snapshot is enabled; ignored otherwise / for Hive
     */
    protected Connection openConnection(Long queryTimestamp) throws SQLException
    {
        try
        {
            Class.forName(queryEngine.driverClass);
        }
        catch (ClassNotFoundException e)
        {
            throw new SQLException(queryEngine.missingHint, e);
        }

        Properties properties = new Properties();
        properties.setProperty("user", queryUser);
        if (queryEngine == QueryEngine.TRINO
                && queryTimestamp != null
                && config.isSinkMonitorFreshnessEmbedSnapshot())
        {
            String sessionPropValue = String.format(
                    "pixels.query_snapshot_timestamp:%d", queryTimestamp);
            properties.setProperty("sessionProperties", sessionPropValue);
        }
        return DriverManager.getConnection(queryJdbcUrl, properties);
    }

    @Deprecated
    protected Connection createNewConnection() throws SQLException
    {
        return openConnection(null);
    }

    protected Connection createNewConnection(long queryTimestamp) throws SQLException
    {
        return openConnection(queryTimestamp);
    }

    private static QueryEngine resolveQueryEngine(String jdbcUrl)
    {
        if (jdbcUrl != null && jdbcUrl.startsWith("jdbc:hive2:"))
        {
            return QueryEngine.HIVE;
        }
        if (jdbcUrl != null && jdbcUrl.startsWith("jdbc:trino:"))
        {
            return QueryEngine.TRINO;
        }
        throw new IllegalArgumentException(
                "Unsupported freshness JDBC URL (expect jdbc:trino:// or jdbc:hive2://): " + jdbcUrl);
    }

    private void closeConnection(Connection conn)
    {
        if (conn != null)
        {
            try
            {
                conn.close();
            } catch (SQLException e)
            {
                LOGGER.warn("Error closing freshness JDBC connection.", e);
            }
        }
    }

    // -------------------------------------------------------------------------------------------------
    // Dynamic Table List Management
    // -------------------------------------------------------------------------------------------------

    /**
     * Adds a table name to the monitoring list.
     * This method can be called by external components (e.g., config trigger).
     *
     * @param tableName The name of the table to add.
     */
    public void addMonitoredTable(String tableName)
    {
        if (tableName == null || tableName.trim().isEmpty())
        {
            LOGGER.warn("Attempted to add null or empty table name to freshness monitor.");
            return;
        }
        monitoredTables.add(tableName);
    }

    /**
     * Removes a table name from the monitoring list.
     *
     * @param tableName The name of the table to remove.
     */
    public void removeMonitoredTable(String tableName)
    {
        if (monitoredTables.remove(tableName))
        {
            LOGGER.info("Table '{}' removed from freshness monitor list.", tableName);
        } else
        {
            LOGGER.debug("Table '{}' was not found in the freshness monitor list.", tableName);
        }
    }

    // -------------------------------------------------------------------------------------------------
    // Scheduling and Execution
    // -------------------------------------------------------------------------------------------------

    /**
     * Starts the scheduled freshness monitoring task.
     */
    public void start()
    {
        LOGGER.info("Starting Freshness Client, querying every {} seconds.", QUERY_INTERVAL_SECONDS);
        scheduler.scheduleAtFixedRate(this::submitQueryTask,
                warmUpSeconds,
                QUERY_INTERVAL_SECONDS,
                TimeUnit.SECONDS);
    }


    /**
     * Stops the scheduled task and closes the JDBC connection.
     */
    public void stop()
    {
        LOGGER.info("Stopping Freshness Client.");
        scheduler.shutdownNow();
        connectionExecutor.shutdownNow();
    }

    private void submitQueryTask()
    {
        if (monitoredTables.isEmpty() && !config.isSinkMonitorFreshnessEmbedStatic())
        {
            LOGGER.debug("No tables configured for freshness monitoring. Skipping submission cycle.");
            return;
        }

        if (!queryPermits.tryAcquire())
        {
            LOGGER.debug("Max concurrent queries ({}) reached. Skipping query submission this cycle.", maxConcurrentQueries);
            return;
        }

        try
        {
            connectionExecutor.submit(this::queryAndCalculateFreshness);
        } catch (RejectedExecutionException e)
        {
            queryPermits.release();
            LOGGER.error("Query task rejected by executor. Max concurrent queries may be too low or service is stopping.", e);
        } catch (Exception e)
        {
            queryPermits.release();
            LOGGER.error("Unknown error during task submission.", e);
        }
    }

    /**
     * The core scheduled task: queries max(freshness_ts) for all monitored tables
     * and calculates the freshness metric.
     */
    void queryAndCalculateFreshness()
    {
        Connection conn = null;
        TransContext transContext = null;

        String tableName;
        try
        {

            tableName = getRandomTable();
            if (tableName == null)
            {
                return;
            }

            LOGGER.debug("Randomly selected table for this cycle: {}", tableName);
            if (config.getSinkMonitorFreshnessEmbedDelay() > 0)
            {
                Thread.sleep(config.getSinkMonitorFreshnessEmbedDelay());
            }
            // Timestamp when the query is sent (t_send)
            long tSendMillis = System.currentTimeMillis();
            Long snapshotTimestamp = null;
            if (config.isSinkMonitorFreshnessEmbedSnapshot())
            {
                transContext = TransService.Instance().beginTrans(true);
                snapshotTimestamp = transContext.getTimestamp();
            }
            conn = openConnection(snapshotTimestamp);

            String tSendMillisStr = DateUtil.convertDateToString(new Date(tSendMillis));
            // Query to find the latest timestamp in the table
            // Assumes 'freshness_ts' is a long-type epoch timestamp (milliseconds)
            String query = String.format(
                    "SELECT max(freshness_ts) FROM \"%s\" WHERE freshness_ts < TIMESTAMP '%s'",
                    tableName,
                    tSendMillisStr
            );

            try (Statement statement = conn.createStatement();
                 ResultSet rs = statement.executeQuery(query))
            {

                Timestamp maxFreshnessTs = null;

                if (rs.next())
                {
                    // Read the maximum timestamp value
                    maxFreshnessTs = rs.getTimestamp(1);
                }

                if (maxFreshnessTs != null)
                {
                    // Freshness = t_send - data_write_time (maxFreshnessTs)
                    // Result is in milliseconds
                    long tQueryMillis = System.currentTimeMillis() - tSendMillis;
                    long freshnessMillis = tSendMillis - maxFreshnessTs.getTime();
                    metricsFacade.recordTableFreshness(tableName, freshnessMillis, tQueryMillis);
                } else
                {
                    LOGGER.warn("Table {} returned null or zero max(freshness_ts). Skipping freshness calculation.", tableName);
                }

            } catch (SQLException e)
            {
                // Handle database errors (e.g., table not found, query syntax error)
                LOGGER.error("Failed to execute query for table {}: {}", tableName, e.getMessage());
            } catch (Exception e)
            {
                // Catch potential runtime errors (e.g., in MetricsFacade)
                LOGGER.error("Error calculating or recording freshness for table {}.", tableName, e);
            }
        } catch (Exception e)
        {
            LOGGER.error("Error selecting a random table from the monitor list.", e);
        } finally
        {
            if (config.isSinkMonitorFreshnessEmbedSnapshot() && transContext != null)
            {
                try
                {
                    TransService.Instance().commitTrans(transContext.getTransId(), true);
                } catch (TransException e)
                {
                    throw new RuntimeException(e);
                }
            }
            closeConnection(conn);
            queryPermits.release();
        }

    }


    private String getRandomTable()
    {
        List<String> tableList;
        if (config.isSinkMonitorFreshnessEmbedStatic())
        {
            tableList = getStaticList();
        } else
        {
            tableList = getDynamicList();
        }

        if (tableList == null || tableList.isEmpty())
        {
            return null;
        }

        Random random = new Random();
        int randomIndex = random.nextInt(tableList.size());

        return tableList.get(randomIndex);
    }

    private List<String> getDynamicList()
    {
        // Take a snapshot of the tables to monitor for this cycle.
        // This prevents ConcurrentModificationException if a table is added/removed mid-iteration.
        Set<String> tablesSnapshot = new HashSet<>(monitoredTables);

        if (tablesSnapshot.isEmpty())
        {
            LOGGER.debug("No tables configured for freshness monitoring. Skipping cycle.");
            return null;
        }

        monitoredTables.clear();

        List<String> staticList = getStaticList();

        // If staticList is empty or null, return all tablesSnapshot
        if (staticList == null || staticList.isEmpty())
        {
            return new ArrayList<>(tablesSnapshot);
        }

        // Return intersection of tablesSnapshot and staticList
        Set<String> staticSet = new HashSet<>(staticList);
        tablesSnapshot.retainAll(staticSet);

        return new ArrayList<>(tablesSnapshot);
    }

    private List<String> getStaticList()
    {
        return config.getSinkMonitorFreshnessEmbedTableList();
    }
}