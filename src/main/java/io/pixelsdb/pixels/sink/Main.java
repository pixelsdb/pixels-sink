/*
 * Copyright 2018-2019 PixelsDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pixelsdb.pixels.sink;

import com.facebook.presto.jdbc.PrestoDriver;
import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataCache;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Compact;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.physical.Status;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.DateUtil;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.compactor.CompactLayout;
import io.pixelsdb.pixels.core.compactor.PixelsCompactor;
import io.pixelsdb.pixels.core.stats.StatsRecorder;
import io.trino.jdbc.TrinoDriver;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author tao
 * @author hank
 * @date: Create in 2018-10-30 11:07
 **/

/**
 * pixels loader command line tool
 * <p>
 * LOAD -f pixels -o s3://text-105/source -s pixels -t test_105 -n 275000 -r \t -c 16 -l s3://pixels-105/v-0-order
 * -p false [optional, default false]
 * </p>
 * <p>
 * LOAD -f orc -o hdfs://dbiir10:9000/pixels/pixels/test_105/source -s pixels -t test_105 -n 220000 -r \t -c 16
 * -l hdfs://dbiir10:9000/pixels/pixels/test_105/v_0_order_orc/
 * </p>
 * [-l] is optional, its default value is the orderPath of the last writable layout of the table.
 *
 * <br>This should be run under root user to execute cache cleaning commands
 * <p>
 * QUERY -t pixels -w /home/iir/opt/pixels/1187_dedup_query.txt -l /home/iir/opt/pixels/pixels_duration_1187_v_1_compact_cache_2020.01.10-2.csv -c /home/iir/opt/presto-server/sbin/drop-caches.sh
 * </p>
 * <p> Local
 * QUERY -t pixels -w /home/tao/software/station/bitbucket/105_dedup_query.txt -l /home/tao/software/station/bitbucket/pixels_duration_local.csv
 * </p>
 * <p>
 * COPY -p .pxl -s hdfs://dbiir27:9000/pixels/pixels/test_105/v_1_order -d hdfs://dbiir27:9000/pixels/pixels/test_105/v_1_order -n 3 -c 3
 * </p>
 * <p>
 * COMPACT -s pixels -t test_105 -n yes -c 8
 * </p>
 * <p>
 * STAT -s tpch -t region -o false -c true
 * </p>
 */
public class Main
{
    public static void main(String args[])
    {
        Config config = null;
        Scanner scanner = new Scanner(System.in);
        String inputStr;

        while (true)
        {
            System.out.print("pixels> ");
            inputStr = scanner.nextLine().trim();

            if (inputStr.isEmpty() || inputStr.equals(";"))
            {
                continue;
            }

            if (inputStr.endsWith(";"))
            {
                inputStr = inputStr.substring(0, inputStr.length() - 1);
            }

            if (inputStr.equalsIgnoreCase("exit") || inputStr.equalsIgnoreCase("quit") ||
                    inputStr.equalsIgnoreCase("-q"))
            {
                System.out.println("Bye.");
                break;
            }

            if (inputStr.equalsIgnoreCase("help") || inputStr.equalsIgnoreCase("-h"))
            {
                System.out.println("Supported commands:\n" +
                        "LOAD\n" +
                        "QUERY\n" +
                        "COPY\n" +
                        "COMPACT\n" +
                        "STAT");
                System.out.println("{command} -h to show the usage of a command.\nexit / quit / -q to exit.\n");
                continue;
            }

            String command = inputStr.trim().split("\\s+")[0].toUpperCase();

            if (command.equals("LOAD"))
            {
                ArgumentParser argumentParser = ArgumentParsers.newArgumentParser("Pixels ETL LOAD")
                        .defaultHelp(true);

                argumentParser.addArgument("-f", "--format").required(true)
                        .help("Specify the format of files");
                argumentParser.addArgument("-o", "--original_data_path").required(true)
                        .help("specify the path of original data");
                argumentParser.addArgument("-s", "--schema").required(true)
                        .help("specify the name of database");
                argumentParser.addArgument("-t", "--table").required(true)
                        .help("Specify the name of table");
                argumentParser.addArgument("-n", "--row_num").required(true)
                        .help("Specify the max number of rows to write in a file");
                argumentParser.addArgument("-r", "--row_regex").required(true)
                        .help("Specify the split regex of each row in a file");
                argumentParser.addArgument("-c", "--consumer_thread_num").setDefault("4").required(true)
                        .help("specify the number of consumer threads used for data generation");
                argumentParser.addArgument("-p", "--producer").setDefault(false)
                        .help("specify the option of choosing producer");
                argumentParser.addArgument("-e", "--enable_encoding").setDefault(true)
                        .help("specify the option of enabling encoding or not");
                argumentParser.addArgument("-l", "--loading_data_path")
                        .help("specify the path of loading data");

                Namespace ns = null;
                try
                {
                    ns = argumentParser.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                } catch (ArgumentParserException e)
                {
                    argumentParser.handleError(e);
                    System.out.println("Pixels Load.");
                    System.exit(0);
                }

                try
                {
                    String format = ns.getString("format");
                    String schemaName = ns.getString("schema");
                    String tableName = ns.getString("table");
                    String origin = ns.getString("original_data_path");
                    int rowNum = Integer.parseInt(ns.getString("row_num"));
                    String regex = ns.getString("row_regex");
                    String loadingDataPath = ns.getString("loading_data_path");
                    int threadNum = Integer.parseInt(ns.getString("consumer_thread_num"));
                    boolean producer = Boolean.parseBoolean(ns.getString("producer"));
                    boolean enableEncoding = Boolean.parseBoolean(ns.getString("enable_encoding"));
                    System.out.println("enable encoding: " + enableEncoding);
                    if (loadingDataPath != null && !loadingDataPath.isEmpty())
                    {
                        validateOrderOrCompactPath(loadingDataPath);
                    }

                    if (!origin.endsWith("/"))
                    {
                        origin += "/";
                    }

                    Storage storage = StorageFactory.Instance().getStorage(origin);

                    if (format != null)
                    {
                        config = new Config(schemaName, tableName, rowNum, regex, format, loadingDataPath, enableEncoding);
                    }

                    if (producer && config != null)
                    {
                        // todo the producer option is true, means that the producer is dynamic

                    } else if (!producer && config != null)
                    {
                        // source already exist, producer option is false, add list of source to the queue
                        List<String> fileList = storage.listPaths(origin);
                        BlockingQueue<String> fileQueue = new LinkedBlockingQueue<>(fileList.size());
                        for (String filePath : fileList)
                        {
                            fileQueue.add(storage.ensureSchemePrefix(filePath));
                        }

                        ConsumerGenerator instance = ConsumerGenerator.getInstance(threadNum);
                        long startTime = System.currentTimeMillis();

                        if (instance.startConsumer(fileQueue, config))
                        {
                            System.out.println("Executing command " + command + " successfully");
                        } else
                        {
                            System.out.println("Executing command " + command + " unsuccessfully when loading data");
                        }

                        long endTime = System.currentTimeMillis();
                        System.out.println("Text files in '" + origin + "' are loaded into '" + format +
                                "' format by " + threadNum + " threads in " + (endTime - startTime) / 1000 + "s.");

                    } else
                    {
                        System.out.println("Please input the producer option.");
                    }
                } catch (Exception e)
                {
                    e.printStackTrace();
                }
            }

            if (command.equals("QUERY"))
            {
                ArgumentParser argumentParser = ArgumentParsers.newArgumentParser("Pixels QUERY")
                        .defaultHelp(true);

                argumentParser.addArgument("-t", "--type").required(true)
                        .help("Specify the format of files");
                argumentParser.addArgument("-w", "--workload").required(true)
                        .help("specify the path of workload");
                argumentParser.addArgument("-l", "--log").required(true)
                        .help("Specify the path of log");
                argumentParser.addArgument("-c", "--cache")
                        .help("Specify the command of dropping cache");

                Namespace ns = null;
                try
                {
                    ns = argumentParser.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                } catch (ArgumentParserException e)
                {
                    argumentParser.handleError(e);
                    System.out.println("Pixels QUERY.");
                    System.exit(0);
                }

                try
                {
                    String type = ns.getString("type");
                    String workload = ns.getString("workload");
                    String log = ns.getString("log");
                    String cache = ns.getString("cache");

                    if (type != null && workload != null && log != null)
                    {
                        ConfigFactory instance = ConfigFactory.Instance();
                        Properties properties = new Properties();
                        // String user = instance.getProperty("presto.user");
                        String password = instance.getProperty("presto.password");
                        String ssl = instance.getProperty("presto.ssl");
                        String jdbc = instance.getProperty("presto.pixels.jdbc.url");
                        if (type.equalsIgnoreCase("orc"))
                        {
                            jdbc = instance.getProperty("presto.orc.jdbc.url");
                        }

                        if (!password.equalsIgnoreCase("null"))
                        {
                            properties.setProperty("password", password);
                        }
                        properties.setProperty("SSL", ssl);

                        try (BufferedReader workloadReader = new BufferedReader(new FileReader(workload));
                             BufferedWriter timeWriter = new BufferedWriter(new FileWriter(log)))
                        {
                            timeWriter.write("query id,id,duration(ms)\n");
                            timeWriter.flush();
                            String line;
                            int i = 0;
                            String defaultUser = null;
                            while ((line = workloadReader.readLine()) != null)
                            {
                                if (!line.contains("SELECT"))
                                {
                                    defaultUser = line;
                                    properties.setProperty("user", type + "_" + defaultUser);
                                } else
                                {
                                    if (cache != null)
                                    {
                                        long start = System.currentTimeMillis();
                                        ProcessBuilder processBuilder = new ProcessBuilder(cache);
                                        Process process = processBuilder.start();
                                        process.waitFor();
                                        Thread.sleep(1000);
                                        System.out.println("clear cache: " + (System.currentTimeMillis() - start) + "ms\n");
                                    }
                                    else
                                    {
                                        Thread.sleep(15 * 1000);
                                        System.out.println("wait 15000 ms\n");
                                    }

                                    long cost = executeSQL(jdbc, properties, line, defaultUser);
                                    timeWriter.write(defaultUser + "," + i + "," + cost + "\n");

                                    System.out.println(i + "," + cost + "ms");
                                    i++;
                                    if (i % 10 == 0)
                                    {
                                        timeWriter.flush();
                                        System.out.println(i);
                                    }

                                }
                            }
                            timeWriter.flush();
                        } catch (Exception e)
                        {
                            e.printStackTrace();
                        }
                    } else
                    {
                        System.out.println("Please input the parameters.");
                    }
                } catch (Exception e)
                {
                    e.printStackTrace();
                }
            }

            if (command.equals("COPY"))
            {
                ArgumentParser argumentParser = ArgumentParsers.newArgumentParser("Pixels ETL COPY")
                        .defaultHelp(true);

                argumentParser.addArgument("-p", "--postfix").required(true)
                        .help("Specify the postfix of files to be copied");
                argumentParser.addArgument("-s", "--source").required(true)
                        .help("specify the source directory");
                argumentParser.addArgument("-d", "--destination").required(true)
                        .help("Specify the destination directory");
                argumentParser.addArgument("-n", "--number").required(true)
                        .help("Specify the number of copies");
                argumentParser.addArgument("-c", "--concurrency")
                        .setDefault("4").required(true)
                        .help("specify the number of threads used for data compaction");

                Namespace ns = null;
                try
                {
                    ns = argumentParser.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                } catch (ArgumentParserException e)
                {
                    argumentParser.handleError(e);
                    System.out.println("Pixels COPY.");
                    System.exit(0);
                }

                try
                {
                    String postfix = ns.getString("postfix");
                    String source = ns.getString("source");
                    String destination = ns.getString("destination");
                    int n = Integer.parseInt(ns.getString("number"));
                    int threadNum = Integer.parseInt(ns.getString("concurrency"));
                    ExecutorService copyExecutor = Executors.newFixedThreadPool(threadNum);

                    if (!destination.endsWith("/"))
                    {
                        destination += "/";
                    }

                    ConfigFactory configFactory = ConfigFactory.Instance();

                    Storage sourceStorage = StorageFactory.Instance().getStorage(source);
                    Storage destStorage = StorageFactory.Instance().getStorage(destination);

                    List<Status> files =  sourceStorage.listStatus(source);
                    long blockSize = Long.parseLong(configFactory.getProperty("block.size"));
                    short replication = Short.parseShort(configFactory.getProperty("block.replication"));

                    // copy
                    long startTime = System.currentTimeMillis();
                    AtomicInteger copiedNum = new AtomicInteger(0);
                    for (int i = 0; i < n; ++i)
                    {
                        String destination_ = destination;
                        // Issue #192: make copy multi-threaded.
                        for (Status s : files)
                        {
                            String sourceName = s.getName();
                            if (!sourceName.contains(postfix))
                            {
                                continue;
                            }
                            String destPath = destination_ +
                                    sourceName.substring(0, sourceName.indexOf(postfix)) +
                                    "_copy_" + DateUtil.getCurTime() + postfix;
                            copyExecutor.execute(() -> {
                                try
                                {
                                    if (sourceStorage.getScheme() == destStorage.getScheme() &&
                                            destStorage.supportDirectCopy())
                                    {
                                        destStorage.directCopy(s.getPath(), destPath);
                                    } else
                                    {
                                        DataInputStream inputStream = sourceStorage.open(s.getPath());
                                        DataOutputStream outputStream = destStorage.create(destPath, false,
                                                Constants.HDFS_BUFFER_SIZE, replication, blockSize);
                                        IOUtils.copyBytes(inputStream, outputStream,
                                                Constants.HDFS_BUFFER_SIZE, true);
                                    }
                                    copiedNum.incrementAndGet();
                                } catch (IOException e)
                                {
                                    e.printStackTrace();
                                }
                            });
                        }
                    }

                    copyExecutor.shutdown();
                    while (!copyExecutor.awaitTermination(100, TimeUnit.SECONDS));

                    long endTime = System.currentTimeMillis();
                    System.out.println((copiedNum.get()/n) + " file(s) are copied " + n + " time(s) by "
                            + threadNum + " threads in " + (endTime - startTime) / 1000 + "s.");
                }
                catch (IOException | InterruptedException e)
                {
                    e.printStackTrace();
                }
            }

            if (command.equals("COMPACT"))
            {
                ArgumentParser argumentParser = ArgumentParsers.newArgumentParser("Pixels ETL COMPACT")
                        .defaultHelp(true);

                argumentParser.addArgument("-s", "--schema").required(true)
                        .help("Specify the name of schema.");
                argumentParser.addArgument("-t", "--table").required(true)
                        .help("specify the name of table.");
                argumentParser.addArgument("-n", "--naive").required(true)
                        .help("Specify whether or not to create naive compact layout.");
                argumentParser.addArgument("-c", "--concurrency")
                        .setDefault("4").required(true)
                        .help("specify the number of threads used for data compaction");

                Namespace ns = null;
                try
                {
                    ns = argumentParser.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                } catch (ArgumentParserException e)
                {
                    argumentParser.handleError(e);
                    System.out.println("Pixels Compact.");
                    System.exit(0);
                }

                try
                {
                    String schemaName = ns.getString("schema");
                    String tableName = ns.getString("table");
                    String naive = ns.getString("naive");
                    int threadNum = Integer.parseInt(ns.getString("concurrency"));
                    ExecutorService compactExecutor = Executors.newFixedThreadPool(threadNum);

                    String metadataHost = ConfigFactory.Instance().getProperty("metadata.server.host");
                    int metadataPort = Integer.parseInt(ConfigFactory.Instance().getProperty("metadata.server.port"));

                    // get compact layout
                    MetadataService metadataService = new MetadataService(metadataHost, metadataPort);
                    List<Layout> layouts = metadataService.getLayouts(schemaName, tableName);
                    metadataService.shutdown();
                    System.out.println("existing number of layouts: " + layouts.size());
                    Layout layout = null;
                    for (Layout layout1 : layouts)
                    {
                        if (layout1.isWritable())
                        {
                            layout = layout1;
                            break;
                        }
                    }

                    requireNonNull(layout, String.format("writable layout is not found for table '%s.%s'.",
                            schemaName, tableName));
                    Compact compact = layout.getCompactObject();
                    int numRowGroupInBlock = compact.getNumRowGroupInBlock();
                    int numColumn = compact.getNumColumn();
                    CompactLayout compactLayout;
                    if (naive.equalsIgnoreCase("yes") || naive.equalsIgnoreCase("y"))
                    {
                        compactLayout = CompactLayout.buildNaive(numRowGroupInBlock, numColumn);
                    }
                    else
                    {
                        compactLayout = CompactLayout.fromCompact(compact);
                    }

                    // get input file paths
                    ConfigFactory configFactory = ConfigFactory.Instance();
                    validateOrderOrCompactPath(layout.getOrderPath());
                    validateOrderOrCompactPath(layout.getCompactPath());
                    // PIXELS-399: it is not a problem if the order or compact path contains multiple directories
                    Storage orderStorage = StorageFactory.Instance().getStorage(layout.getOrderPath());
                    Storage compactStorage = StorageFactory.Instance().getStorage(layout.getCompactPath());
                    long blockSize = Long.parseLong(configFactory.getProperty("block.size"));
                    short replication = Short.parseShort(configFactory.getProperty("block.replication"));
                    List<Status> statuses = orderStorage.listStatus(layout.getOrderPath());
                    String[] targetPaths = layout.getCompactPath().split(";");
                    int targetPathId = 0;

                    // compact
                    long startTime = System.currentTimeMillis();
                    for (int i = 0, thdId = 0; i < statuses.size(); i += numRowGroupInBlock, ++thdId)
                    {
                        if (i + numRowGroupInBlock > statuses.size())
                        {
                            /**
                             * Issue #160:
                             * Compact the tail files that can not fulfill the compactLayout
                             * defined in the metadata.
                             * Note that if (i + numRowGroupInBlock == statues.size()),
                             * then the remaining files are not tail files.
                             *
                             * Here we set numRowGroupInBlock to the number of tail files,
                             * and rebuild a pure compactLayout for the tail files as the
                             * compactLayout in metadata does not work for the tail files.
                             */
                            numRowGroupInBlock = statuses.size() - i;
                            compactLayout = CompactLayout.buildPure(numRowGroupInBlock, numColumn);
                        }

                        List<String> sourcePaths = new ArrayList<>();
                        for (int j = 0; j < numRowGroupInBlock; ++j)
                        {
                            if (!statuses.get(i+j).getPath().endsWith("/"))
                            {
                                sourcePaths.add(statuses.get(i + j).getPath());
                            }
                        }

                        String targetPath = targetPaths[targetPathId++];
                        targetPathId %= targetPaths.length;
                        if (!targetPath.endsWith("/"))
                        {
                            targetPath += "/";
                        }
                        String filePath = targetPath + DateUtil.getCurTime() + "_compact.pxl";

                        System.out.println("(" + thdId + ") " + sourcePaths.size() +
                                " ordered files to be compacted into '" + filePath + "'.");

                        PixelsCompactor.Builder compactorBuilder =
                                PixelsCompactor.newBuilder()
                                        .setSourcePaths(sourcePaths)
                                        /**
                                         * Issue #192:
                                         * No need to deep copy compactLayout as it is never modified in-place
                                         * (e.g., call setters to change some members). Thus it is safe to use
                                         * the current reference of compactLayout even if the compactors will
                                         * be running multiple threads.
                                         *
                                         * Deep copy it if it is in-place modified in the future.
                                         */
                                        .setCompactLayout(compactLayout)
                                        .setStorage(compactStorage)
                                        .setPath(filePath)
                                        .setBlockSize(blockSize)
                                        .setReplication(replication)
                                        .setBlockPadding(false);

                        long threadStart = System.currentTimeMillis();
                        String finalFilePath = filePath;
                        compactExecutor.execute(() -> {
                            // Issue #192: run compaction in threads.
                            try
                            {
                                // build() spends some time to read file footers and should be called inside sub-thread.
                                PixelsCompactor pixelsCompactor = compactorBuilder.build();
                                pixelsCompactor.compact();
                                pixelsCompactor.close();
                            } catch (IOException e)
                            {
                                e.printStackTrace();
                            }
                            System.out.println("Compact file '" + finalFilePath + "' is built in " +
                                    ((System.currentTimeMillis() - threadStart) / 1000.0) + "s");
                        });
                    }

                    // Issue #192: wait for the compaction to complete.
                    compactExecutor.shutdown();
                    while (!compactExecutor.awaitTermination(100, TimeUnit.SECONDS));

                    long endTime = System.currentTimeMillis();
                    System.out.println("Pixels files in '" + layout.getOrderPath() + "' are compacted into '" +
                            layout.getCompactPath() + "' by " + threadNum + " threads in " +
                            (endTime - startTime) / 1000 + "s.");
                }
                catch (MetadataException | IOException | InterruptedException e)
                {
                    e.printStackTrace();
                }
            }

            if (command.equals("STAT"))
            {
                ArgumentParser argumentParser = ArgumentParsers.newArgumentParser("Pixels Update Statistics")
                        .defaultHelp(true);

                argumentParser.addArgument("-s", "--schema").required(true)
                        .help("Specify the schema name");
                argumentParser.addArgument("-t", "--table").required(true)
                        .help("Specify the table name");
                argumentParser.addArgument("-o", "--ordered_enabled").setDefault(false)
                        .help("Specify whether the ordered path is enabled");
                argumentParser.addArgument("-c", "--compact_enabled").setDefault(true)
                        .help("Specify whether the compact path is enabled");

                Namespace ns = null;
                try
                {
                    ns = argumentParser.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                } catch (ArgumentParserException e)
                {
                    argumentParser.handleError(e);
                    System.out.println("Pixels STAT.");
                    System.exit(0);
                }

                try
                {
                    String schemaName = ns.getString("schema");
                    String tableName = ns.getString("table");
                    boolean orderedEnabled = Boolean.parseBoolean(ns.getString("ordered_enabled"));
                    boolean compactEnabled = Boolean.parseBoolean(ns.getString("compact_enabled"));

                    String metadataHost = ConfigFactory.Instance().getProperty("metadata.server.host");
                    int metadataPort = Integer.parseInt(ConfigFactory.Instance().getProperty("metadata.server.port"));
                    MetadataService metadataService = new MetadataService(metadataHost, metadataPort);
                    List<Layout> layouts = metadataService.getLayouts(schemaName, tableName);
                    List<String> files = new LinkedList<>();
                    for (Layout layout : layouts)
                    {
                        if (layout.isReadable())
                        {
                            if (orderedEnabled)
                            {
                                String orderedPath = layout.getOrderPath();
                                validateOrderOrCompactPath(orderedPath);
                                Storage storage = StorageFactory.Instance().getStorage(orderedPath);
                                files.addAll(storage.listPaths(orderedPath));
                            }
                            if (compactEnabled)
                            {
                                String compactPath = layout.getCompactPath();
                                validateOrderOrCompactPath(compactPath);
                                Storage storage = StorageFactory.Instance().getStorage(compactPath);
                                files.addAll(storage.listPaths(compactPath));
                            }
                        }
                    }

                    // get the statistics.
                    long startTime = System.currentTimeMillis();

                    List<Column> columns = metadataService.getColumns(schemaName, tableName, true);
                    Map<String, Column> columnMap = new HashMap<>(columns.size());
                    Map<String, StatsRecorder> columnStatsMap = new HashMap<>(columns.size());

                    for (Column column : columns)
                    {
                        column.setChunkSize(0);
                        column.setSize(0);
                        columnMap.put(column.getName(), column);
                    }

                    int rowGroupCount = 0;
                    long rowCount = 0;
                    for (String path : files)
                    {
                        if (!path.endsWith("/"))
                        {
                            path += "/";
                        }
                        Storage storage = StorageFactory.Instance().getStorage(path);
                        PixelsReader pixelsReader = PixelsReaderImpl.newBuilder()
                                .setPath(path).setStorage(storage).setEnableCache(false)
                                .setCacheOrder(ImmutableList.of()).setPixelsCacheReader(null)
                                .setPixelsFooterCache(new PixelsFooterCache()).build();
                        PixelsProto.Footer fileFooter = pixelsReader.getFooter();
                        int numRowGroup = pixelsReader.getRowGroupNum();
                        rowGroupCount += numRowGroup;
                        rowCount += pixelsReader.getNumberOfRows();
                        List<PixelsProto.Type> types = fileFooter.getTypesList();
                        for (int i = 0; i < numRowGroup; ++i)
                        {
                            PixelsProto.RowGroupFooter rowGroupFooter = pixelsReader.getRowGroupFooter(i);
                            List<PixelsProto.ColumnChunkIndex> chunkIndices =
                                    rowGroupFooter.getRowGroupIndexEntry().getColumnChunkIndexEntriesList();
                            for (int j = 0; j < types.size(); ++j)
                            {
                                Column column = columnMap.get(types.get(j).getName());
                                long chunkLength = chunkIndices.get(j).getChunkLength();
                                column.setSize(chunkLength + column.getSize());
                            }
                        }
                        List<TypeDescription> fields = pixelsReader.getFileSchema().getChildren();
                        checkArgument(fields.size() == types.size(),
                                "types.size and fields.size are not consistent");
                        for (int i = 0; i < fields.size(); ++i)
                        {
                            TypeDescription field = fields.get(i);
                            PixelsProto.Type type = types.get(i);
                            StatsRecorder statsRecorder = columnStatsMap.get(type.getName());
                            if (statsRecorder == null)
                            {
                                columnStatsMap.put(type.getName(),
                                        StatsRecorder.create(field, fileFooter.getColumnStats(i)));
                            }
                            else
                            {
                                statsRecorder.merge(StatsRecorder.create(field, fileFooter.getColumnStats(i)));
                            }
                        }
                        pixelsReader.close();
                    }

                    ConfigFactory instance = ConfigFactory.Instance();
                    Properties properties = new Properties();
                    properties.setProperty("user", instance.getProperty("presto.user"));
                    // properties.setProperty("password", instance.getProperty("presto.password"));
                    properties.setProperty("SSL", instance.getProperty("presto.ssl"));
                    properties.setProperty("sessionProperties", "pixels.ordered_path_enabled:" + orderedEnabled);
                    properties.setProperty("sessionProperties", "pixels.compact_path_enabled:" + compactEnabled);
                    String jdbc = instance.getProperty("presto.pixels.jdbc.url");
                    try
                    {
                        DriverManager.registerDriver(new TrinoDriver());
                        DriverManager.registerDriver(new PrestoDriver());
                    } catch (SQLException e)
                    {
                        e.printStackTrace();
                    }

                    for (Column column : columns)
                    {
                        column.setChunkSize(column.getSize() / rowGroupCount);
                        column.setRecordStats(columnStatsMap.get(column.getName())
                                .serialize().build().toByteString().asReadOnlyByteBuffer());
                        column.getRecordStats().mark();
                        metadataService.updateColumn(column);
                        column.getRecordStats().reset();
                    }

                    /* Set cardinality and null_fraction after the chunk size and column size,
                     * because chunk size and column size must exist in the metadata when calculating
                     * the cardinality and null_fraction using SQL queries.
                     */
                    MetadataCache.Instance().dropCachedColumns();
                    try (Connection connection = DriverManager.getConnection(jdbc, properties))
                    {
                        for (Column column : columns)
                        {
                            String sql = "SELECT COUNT(DISTINCT(" + column.getName() + ")) AS cardinality, " +
                                    "SUM(CASE WHEN " + column.getName() + " IS NULL THEN 1 ELSE 0 END) AS null_count " +
                                    "FROM " + tableName;
                            Statement statement = connection.createStatement();
                            ResultSet resultSet = statement.executeQuery(sql);
                            if (resultSet.next())
                            {
                                long cardinality = resultSet.getLong("cardinality");
                                double nullFraction = resultSet.getLong("null_count") / (double) rowCount;
                                System.out.println(column.getName() + " cardinality: " + cardinality +
                                        ", null fraction: " + nullFraction);
                                column.setCardinality(cardinality);
                                column.setNullFraction(nullFraction);
                            }
                            resultSet.close();
                            statement.close();
                            metadataService.updateColumn(column);
                        }
                    } catch (SQLException e)
                    {
                        e.printStackTrace();
                    }

                    long endTime = System.currentTimeMillis();
                    System.out.println("Elapsed time: " + (endTime - startTime) / 1000 + "s.");
                    metadataService.shutdown();
                }
                catch (IOException | MetadataException | InterruptedException e)
                {
                    e.printStackTrace();
                }
            }

            if (!command.equals("QUERY") &&
                    !command.equals("LOAD") &&
                    !command.equals("COPY") &&
                    !command.equals("COMPACT") &&
                    !command.equals("STAT"))
            {
                System.out.println("Command error");
            }
        }

    }

    public static long executeSQL(String jdbcUrl, Properties jdbcProperties, String sql, String id) {
        long start = 0L, end = 0L;
        try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcProperties))
        {
            Statement statement = connection.createStatement();
            start = System.currentTimeMillis();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {}
            end = System.currentTimeMillis();
            resultSet.close();
            statement.close();
        } catch (SQLException e)
        {
            System.out.println("SQL: " + id + "\n" + sql);
            System.out.println("Error msg: " + e.getMessage());
        }
        return end - start;
    }

    /**
     * Check if the order or compact path from pixels metadata is valid.
     * @param path the order or compact path from pixels metadata.
     */
    public static void validateOrderOrCompactPath(String path)
    {
        requireNonNull(path, "path is null");
        String[] paths = path.split(";");
        checkArgument(paths.length > 0, "path must contain at least one valid directory");
        try
        {
            Storage.Scheme firstScheme = Storage.Scheme.fromPath(paths[0]);
            for (int i = 1; i < paths.length; ++i)
            {
                Storage.Scheme scheme = Storage.Scheme.fromPath(paths[i]);
                checkArgument(firstScheme.equals(scheme),
                        "all the directories in the path must have the same storage scheme");
            }
        } catch (Throwable e)
        {
            throw new RuntimeException("failed to parse storage scheme from path", e);
        }
    }
}