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

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.TestConfig;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertTrue;

// We extend FreshnessClient to access the protected queryAndCalculateFreshness method
@Tag("integration")
class FreshnessClientTest
{

    // Mocks for JDBC dependencies
    private Connection mockConnection;
    private Statement mockStatement;
    private ResultSet mockResultSet;

    // Mocks for utility/config dependencies
    private PixelsSinkConfig mockConfig;
    private MetricsFacade mockMetricsFacade;
    private FreshnessClient client; // The instance of the client to test

    @BeforeAll
    static void setUp() throws Exception
    {
        TestConfig.initializeIntegrationConfig();
    }

    @Test
    void testFreshnessCalculationSuccess() throws Exception
    {

        FreshnessClient freshnessClient = FreshnessClient.getInstance();
        freshnessClient.addMonitoredTable("customer");
        try
        {
            freshnessClient.start();
            Thread.sleep(1000);
        } finally
        {
            freshnessClient.stop();
        }
    }

    @Test
    void testSnapshotTs() throws SQLException
    {
        FreshnessClient freshnessClient = FreshnessClient.getInstance();
        try (Connection connection = freshnessClient.createNewConnection(123456L);
             Statement statement = connection.createStatement())
        {
            String query = "SELECT max(freshness_ts) FROM company";
            try (ResultSet resultSet = statement.executeQuery(query))
            {
                assertTrue(resultSet.next());
            }
        }
    }

    @Test
    void testLoanTransQueryPerformance(@org.junit.jupiter.api.io.TempDir Path tempDir)
            throws SQLException
    {
        FreshnessClient freshnessClient = FreshnessClient.getInstance();
        Connection connection = freshnessClient.createNewConnection(12345689100L);
        String query = "SELECT max(freshness_ts) FROM nation";
        Path csvFile = tempDir.resolve("loantrans_query_results.csv");
        int iterations = Integer.getInteger("pixels.sink.test.iterations", 10);
        try (PrintWriter writer = new PrintWriter(
                Files.newBufferedWriter(csvFile, StandardCharsets.UTF_8)))
        {

            for (int i = 0; i < iterations; i++)
            {
                long startTime = System.currentTimeMillis();
                long startNano = System.nanoTime();

                long maxFreshnessTs = 0;
                try (Statement statement = connection.createStatement();
                     ResultSet resultSet = statement.executeQuery(query))
                {

                    if (resultSet.next())
                    {
                        maxFreshnessTs = resultSet.getLong(1);
                    }
                } catch (SQLException e)
                {
                    System.err.println("Query failed at iteration " + i + ": " + e.getMessage());
                }

                long endNano = System.nanoTime();
                long durationMs = (endNano - startNano) / 1_000_000;
                writer.printf("%d,%d,%d%n", startTime, maxFreshnessTs, durationMs);
                writer.flush();
            }
            System.out.println("Test completed. Results saved to: " + csvFile);
        } catch (IOException e)
        {
            e.printStackTrace();
        } finally
        {
            if (connection != null && !connection.isClosed())
            {
                connection.close();
            }
        }
    }
}