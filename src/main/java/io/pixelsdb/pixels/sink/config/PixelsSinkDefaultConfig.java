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

package io.pixelsdb.pixels.sink.config;

import io.pixelsdb.pixels.sink.deserializer.RowChangeEventJsonDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class PixelsSinkDefaultConfig {
    public static final String PROPERTIES_PATH = "pixels-sink.properties";
    public static final String CSV_SINK_PATH = "./data";

    public static final String KEY_DESERIALIZER = StringDeserializer.class.getName(); // org.apache.kafka.common.serialization.StringDeserializer
    public static final String VALUE_DESERIALIZER = RowChangeEventJsonDeserializer.class.getName();

    public static final String SINK_MODE = "csv";

    public static final int SINK_CSV_RECORD_FLUSH = 1000;

    public static final int SINK_THREAD = 32;
    public static final int SINK_CONSUMER_THREAD = 8;
    //    sink.remote.host=localhost
//    sink.remote.port=229422
//    sink.batch.size=100
//    sink.timeout.ms=5000
//    sink.flush.interval.ms=5000
//    sink.max.retries=3
    // REMOTE BUFFER
    public static final String SINK_REMOTE_HOST = "localhost";
    public static final short SINK_REMOTE_PORT = 22942;
    public static final int SINK_BATCH_SIZE = 100;
    public static final int SINK_TIMEOUT_MS = 5000;
    public static final int SINK_FLUSH_INTERVAL_MS = 5000;
    public static final int SINK_MAX_RETRIES = 3;
    public static final boolean SINK_CSV_ENABLE_HEADER = false;

    // Monitor Config
    public static final boolean SINK_MONITOR_ENABLED = true;
    public static final short SINK_MONITOR_PORT = 9464;

    // Mock RPC
    public static final boolean SINK_RPC_ENABLED = true;
    public static final int MOCK_RPC_DELAY = 100;
}
