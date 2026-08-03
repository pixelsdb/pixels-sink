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
package io.pixelsdb.pixels.sink.writer.proto;


import com.google.protobuf.InvalidProtocolBufferException;
import io.pixelsdb.pixels.common.physical.PhysicalWriter;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import io.pixelsdb.pixels.sink.util.TableCounters;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriter;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @package: io.pixelsdb.pixels.sink.writer
 * @className: ProtoWriter
 * @author: AntiO2
 * @date: 2025/10/5 07:10
 */
public class ProtoWriter implements PixelsSinkWriter
{
    private final Logger LOGGER = LoggerFactory.getLogger(ProtoWriter.class);
    private final RotatingWriterManager writerManager;
    private final TableMetadataRegistry instance;
    private final ReentrantLock lock = new ReentrantLock();
    private final Boolean debug = true;
    /**
     * Data structure to track transaction progress:
     * Map<TransId, TransactionContext>
     */
    private final Map<String, TransactionContext> transTracker = new ConcurrentHashMap<>();


    public ProtoWriter() throws IOException
    {
        PixelsSinkConfig sinkConfig = PixelsSinkConfigFactory.getInstance();

        String dataPath = sinkConfig.getSinkProtoData();
        this.writerManager = new RotatingWriterManager(dataPath);
        this.instance = TableMetadataRegistry.Instance();
    }

    /**
     * Checks if all tables within a transaction have reached their expected row count.
     * If complete, the transaction is removed from the tracker and final metrics are recorded.
     *
     * @param transId The ID of the transaction to check.
     */
    private void checkAndCleanupTransaction(String transId)
    {
        TransactionContext context = transTracker.get(transId);

        if (context == null || !context.isEndReceived())
        {
            // Transaction has not received TX END or has been cleaned up already.
            return;
        }

        Map<String, TableCounters> tableMap = context.tableCounters;
        if (tableMap == null || tableMap.isEmpty())
        {
            // Empty transaction with no tables. Clean up immediately.
            transTracker.remove(transId);
            LOGGER.info("Transaction {} (empty) successfully completed and removed from tracker.", transId);
            return;
        }

        boolean allComplete = true;
        int actualProcessedRows = 0;

        // Iterate through all tables to check completion status
        for (Map.Entry<String, TableCounters> entry : tableMap.entrySet())
        {
            TableCounters counters = entry.getValue();
            if (!counters.isComplete())
            {
                allComplete = false;
            }
        }

        if (allComplete)
        {
            transTracker.remove(transId);
            ByteBuffer transInfo = getTransBuffer(context);
            transInfo.rewind();
            writeBuffer(transInfo);
        }
    }

    @Override
    public boolean writeTrans(SinkProto.TransactionMetadata transactionMetadata)
    {
        try
        {
            lock.lock();
            String transId = transactionMetadata.getId();
            if (transactionMetadata.getStatus() == SinkProto.TransactionStatus.BEGIN)
            {
                // 1. BEGIN: Create context if not exists (in case ROWChange arrived first).
                TransactionContext transactionContext = transTracker.computeIfAbsent(transId, k -> new TransactionContext());
                LOGGER.debug("Transaction {} BEGIN received.", transId);
                transactionContext.txBegin = transactionMetadata;
            } else if (transactionMetadata.getStatus() == SinkProto.TransactionStatus.END)
            {
                // 2. END: Finalize tracker state, merge pre-counts, and trigger cleanup.

                // Get existing context or create a new one (in case BEGIN was missed).
                TransactionContext context = transTracker.computeIfAbsent(transId, k -> new TransactionContext());

                // --- Initialization Step: Set Total Counts ---
                Map<String, TableCounters> newTableCounters = new ConcurrentHashMap<>();
                for (SinkProto.DataCollection dataCollection : transactionMetadata.getDataCollectionsList())
                {
                    String fullTable = dataCollection.getDataCollection();
                    // Create official counter with total count
                    newTableCounters.put(fullTable, new TableCounters((int) dataCollection.getEventCount()));
                }

                // Set the final state (must be volatile write)
                context.setEndReceived(newTableCounters);

                // --- Merge Step: Apply pre-received rows ---
                for (Map.Entry<String, AtomicInteger> preEntry : context.preEndCounts.entrySet())
                {
                    String table = preEntry.getKey();
                    int accumulatedCount = preEntry.getValue().get();
                    TableCounters finalCounter = newTableCounters.get(table);

                    if (finalCounter != null)
                    {
                        // Apply the accumulated count to the official counter
                        for (int i = 0; i < accumulatedCount; i++)
                        {
                            finalCounter.increment();
                        }
                    } else
                    {
                        LOGGER.warn("Pre-received rows for table {} (count: {}) but table was not in TX END metadata. Discarding accumulated count.", table, accumulatedCount);
                    }
                }
                context.txEnd = transactionMetadata;

                // --- Cleanup/Validation Step ---
                // Trigger cleanup. This will validate if all rows (pre and post END) have satisfied the total counts.
                checkAndCleanupTransaction(transId);
            }
            return true;
        } finally
        {
            lock.unlock();
        }
    }

    private ByteBuffer getTransBuffer(TransactionContext transactionContext)
    {
        int total = 0;
        byte[] transDataBegin = transactionContext.txBegin.toByteArray();
        ByteBuffer beginByteBuffer = writeData(-1, transDataBegin);
        total += beginByteBuffer.remaining();

        byte[] transDataEnd = transactionContext.txEnd.toByteArray();
        ByteBuffer endByteBuffer = writeData(-1, transDataEnd);
        total += endByteBuffer.remaining();

        if (debug)
        {
            validMetaInfo(transDataBegin);
            validMetaInfo(transDataEnd);
        }

        List<ByteBuffer> rowEvents = new CopyOnWriteArrayList<>();
        for (RowChangeEvent rowChangeEvent : transactionContext.rowChangeEventList)
        {
            ByteBuffer byteBuffer = write(rowChangeEvent.getRowRecord());
            if (byteBuffer == null)
            {
                return null;
            }
            rowEvents.add(byteBuffer);
            total += byteBuffer.remaining();
        }
        ByteBuffer buffer = ByteBuffer.allocate(total).order(ByteOrder.BIG_ENDIAN);
        beginByteBuffer.rewind();
        try
        {
            buffer.put(beginByteBuffer);
        } catch (BufferOverflowException e)
        {
            throw new RuntimeException(e);
        }

        for (ByteBuffer rowEvent : rowEvents)
        {
            rowEvent.rewind();
            buffer.put(rowEvent);
        }
        endByteBuffer.rewind();
        buffer.put(endByteBuffer);
        buffer.flip();
        return buffer;
    }

    private boolean validMetaInfo(byte[] data)
    {
        try
        {
            SinkProto.TransactionMetadata.parseFrom(data);
        } catch (InvalidProtocolBufferException e)
        {
            throw new RuntimeException(e);
        }
        return true;
    }

    public ByteBuffer write(SinkProto.RowRecord rowRecord)
    {
        byte[] rowData = rowRecord.toByteArray();
        String tableName = rowRecord.getSource().getTable();
        String schemaName = rowRecord.getSource().getDb();

        if (debug)
        {
            try
            {
                SinkProto.RowRecord.parseFrom(rowData);
            } catch (InvalidProtocolBufferException e)
            {
                throw new RuntimeException(e);
            }
        }


        long tableId;
        try
        {
            tableId = instance.getTableId(schemaName, tableName);
        } catch (SinkException e)
        {
            LOGGER.error("Error while getting schema table id.", e);
            return null;
        }
        {
            return writeData((int) tableId, rowData);
        }
    }

    // key: -1 means transaction, else means table id
    private ByteBuffer writeData(int key, byte[] data)
    {
        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES + data.length).order(ByteOrder.BIG_ENDIAN); // key + value len + data
        buf.putInt(key).putInt(data.length).put(data);
        buf.flip();
        return buf;
    }

    private synchronized boolean writeBuffer(ByteBuffer buf)
    {
        if (debug)
        {
            // Perform deep check before writing to physical storage
            try
            {
                debugVerifyBuffer(buf);
            } catch (Exception e)
            {
                LOGGER.error("CRITICAL: Data corruption detected in memory buffer!", e);
                // Block the write to prevent disk pollution
                return false;
            }
        }
        PhysicalWriter writer;
        try
        {
            writer = writerManager.current();
            int size = buf.remaining();
            writer.prepare(size);

            byte[] effectiveData = new byte[size];
            buf.get(effectiveData);
            writer.append(effectiveData);
            writer.flush();
        } catch (IOException e)
        {
            LOGGER.error("Error while writing row record.", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean writeRow(RowChangeEvent rowChangeEvent)
    {
        try
        {
            lock.lock();
            String transId = rowChangeEvent.getTransaction().getId();
            String fullTable = rowChangeEvent.getFullTableName();

            // 1. Get or create the transaction context
            TransactionContext context = transTracker.computeIfAbsent(transId, k -> new TransactionContext());
            context.rowChangeEventList.add(rowChangeEvent);
            // 2. Check if TX END has arrived
            if (context.isEndReceived())
            {
                // TX END arrived: Use official TableCounters
                TableCounters counters = context.tableCounters.get(fullTable);
                if (counters != null)
                {
                    // Increment the processed row count for this table
                    counters.increment();

                    // If this table completed, check if the entire transaction is complete.
                    if (counters.isComplete())
                    {
                        checkAndCleanupTransaction(transId);
                    }
                } else
                {
                    LOGGER.warn("Row received for TransId {} / Table {} but was not included in TX END metadata.", transId, fullTable);
                }
            } else
            {
                context.incrementPreEndCount(fullTable);
                LOGGER.debug("Row received for TransId {} / Table {} before TX END. Accumulating count.", transId, fullTable);
            }
            return true;
        } finally
        {
            lock.unlock();
        }
    }

    @Override
    public void flush()
    {

    }

    @Override
    public void close() throws IOException
    {
        this.writerManager.close();
    }

    /**
     * Verifies the structure and content of the ByteBuffer before physical write.
     * It checks [Key(4B)][Len(4B)][Data(nB)] framing and validates Protobuf integrity.
     * * @param buf The ByteBuffer prepared for writing.
     */
    private void debugVerifyBuffer(ByteBuffer buf)
    {
        // Create a snapshot to avoid modifying the original buffer's position
        ByteBuffer temp = buf.duplicate();
        temp.order(ByteOrder.BIG_ENDIAN);
        temp.rewind();

        int recordCount = 0;
//        LOGGER.info("--- [Debug Verify] Starting deep verification (Size: {} bytes) ---", temp.remaining());

        while (temp.hasRemaining())
        {
            int startPos = temp.position();

            // 1. Ensure enough bytes for the Header (Key + Len = 8 bytes)
            if (temp.remaining() < 8)
            {
                throw new RuntimeException(String.format(
                        "Header truncated at pos %d. Only %d bytes remaining.", startPos, temp.remaining()));
            }

            int key = temp.getInt();
            int len = temp.getInt();

            // 2. Boundary Check: Ensure the buffer contains the full data payload
            if (len < 0 || temp.remaining() < len)
            {
                throw new RuntimeException(String.format(
                        "Data mismatch at pos %d. Key: %d, Declared Len: %d, Available: %d",
                        startPos, key, len, temp.remaining()));
            }

            // 3. Extract payload for Protobuf parsing
            byte[] payload = new byte[len];
            temp.get(payload);

            try
            {
                if (key == -1)
                {
                    // Key -1: Must be valid TransactionMetadata
                    SinkProto.TransactionMetadata.parseFrom(payload);
                    if (debug) LOGGER.debug("Record {}: Verified Metadata at pos {}", recordCount, startPos);
                } else
                {
                    // Key >= 0: Must be valid RowRecord
                    SinkProto.RowRecord.parseFrom(payload);
                    if (debug)
                        LOGGER.debug("Record {}: Verified RowRecord (TableId: {}) at pos {}", recordCount, key, startPos);
                }
            } catch (InvalidProtocolBufferException e)
            {
                // This is where "invalid tag (zero)" or "invalid UTF-8" would be caught
                LOGGER.error("[Debug Verify] Protobuf corruption detected!");
                LOGGER.error("Record: {}, Key: {}, Offset: {}, Error: {}", recordCount, key, startPos, e.getMessage());
                throw new RuntimeException("Pre-write Protobuf validation failed", e);
            }
            recordCount++;
        }
    }

    private static class TransactionContext
    {
        // Key: Full Table Name, Value: Row Count
        private final Map<String, AtomicInteger> preEndCounts = new ConcurrentHashMap<>();
        public List<RowChangeEvent> rowChangeEventList = new CopyOnWriteArrayList<>();
        public SinkProto.TransactionMetadata txBegin;
        public SinkProto.TransactionMetadata txEnd;
        @Getter
        private volatile boolean endReceived = false;
        // Key: Full Table Name
        private Map<String, TableCounters> tableCounters = null;

        public void setEndReceived(Map<String, TableCounters> counters)
        {
            this.tableCounters = counters;
            this.endReceived = true;
        }

        /**
         * @param table Full table name
         */
        public void incrementPreEndCount(String table)
        {
            preEndCounts.computeIfAbsent(table, k -> new AtomicInteger(0)).incrementAndGet();
        }
    }
}