/*
 * Copyright 2026 PixelsDB.
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
package io.pixelsdb.pixels.sink.cdc;

import com.google.protobuf.TextFormat;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriter;
import io.pixelsdb.pixels.sink.writer.retina.RetinaPayloadBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

final class RecordingRetinaWriter implements PixelsSinkWriter
{
    static final String TOKEN = "cdc-validation-token";
    static final int VIRTUAL_NODE_ID = 23;
    static final long TIMESTAMP_BASE = 1_800_000_000_000L;

    record RecordedRequest(
            RowChangeEvent row,
            RetinaProto.UpdateRecordRequest request,
            Path textProtoPath)
    {
    }

    private final Path outputDirectory;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final List<RecordedRequest> recordedRequests = new ArrayList<>();
    private Throwable failure;

    RecordingRetinaWriter(Path outputDirectory) throws IOException
    {
        this.outputDirectory = outputDirectory;
        Files.createDirectories(outputDirectory);
        try (DirectoryStream<Path> files =
                     Files.newDirectoryStream(outputDirectory, "*.textproto"))
        {
            for (Path file : files)
            {
                Files.delete(file);
            }
        }
    }

    @Override
    public void flush()
    {
    }

    @Override
    public boolean writeRow(RowChangeEvent row)
    {
        lock.lock();
        try
        {
            int ordinal = recordedRequests.size() + 1;
            long timestamp = TIMESTAMP_BASE + ordinal;
            row.setTimeStamp(timestamp);
            row.updateIndexKey();

            RetinaProto.TableUpdateData tableUpdate =
                    RetinaPayloadBuilder.buildTableUpdateData(
                            row.getTable(), timestamp, List.of(row));
            RetinaProto.UpdateRecordRequest request =
                    RetinaPayloadBuilder.buildUpdateRecordRequest(
                            TOKEN,
                            row.getSchemaName(),
                            VIRTUAL_NODE_ID,
                            List.of(tableUpdate));
            Path output = outputDirectory.resolve(String.format(
                    Locale.ROOT,
                    "%03d-%s.textproto",
                    ordinal,
                    row.getOp().name().toLowerCase(Locale.ROOT)));
            String textProto = TextFormat.printer().printToString(request);
            Files.writeString(
                    output,
                    textProto,
                    StandardCharsets.UTF_8);
            RetinaProto.UpdateRecordRequest.Builder parsedText =
                    RetinaProto.UpdateRecordRequest.newBuilder();
            TextFormat.getParser().merge(
                    Files.readString(output, StandardCharsets.UTF_8),
                    parsedText);
            if (!request.equals(parsedText.build()) ||
                    !request.equals(RetinaProto.UpdateRecordRequest.parseFrom(
                            request.toByteArray())))
            {
                throw new IOException("CDC request round-trip mismatch: " + output);
            }
            recordedRequests.add(new RecordedRequest(row, request, output));
            changed.signalAll();
            return true;
        } catch (Throwable t)
        {
            failure = t;
            changed.signalAll();
            return false;
        } finally
        {
            lock.unlock();
        }
    }

    @Override
    public boolean writeTrans(SinkProto.TransactionMetadata transactionMetadata)
    {
        return true;
    }

    List<RecordedRequest> records()
    {
        lock.lock();
        try
        {
            throwIfFailed();
            return List.copyOf(recordedRequests);
        } finally
        {
            lock.unlock();
        }
    }

    List<RecordedRequest> await(
            Predicate<List<RecordedRequest>> completed,
            Duration timeout) throws InterruptedException
    {
        long remaining = timeout.toNanos();
        lock.lock();
        try
        {
            while (!completed.test(List.copyOf(recordedRequests)))
            {
                throwIfFailed();
                if (remaining <= 0)
                {
                    throw new AssertionError(
                            "Timed out waiting for CDC requests; recorded=" +
                                    recordedRequests.size());
                }
                remaining = changed.awaitNanos(remaining);
            }
            throwIfFailed();
            return List.copyOf(recordedRequests);
        } finally
        {
            lock.unlock();
        }
    }

    @Override
    public void close()
    {
    }

    private void throwIfFailed()
    {
        if (failure != null)
        {
            throw new AssertionError("Recording Retina writer failed", failure);
        }
    }
}
