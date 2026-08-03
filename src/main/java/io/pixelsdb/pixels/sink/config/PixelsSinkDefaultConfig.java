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
package io.pixelsdb.pixels.sink.config;

public class PixelsSinkDefaultConfig
{
    public static final String DATA_SOURCE = "engine";
    public static final String SOURCE_DECODE_THREADS = "4";
    public static final String PROPERTIES_PATH = "pixels-sink.properties";
    public static final String CSV_SINK_PATH = "./data";

    public static final String SINK_MODE = "retina";

    public static final int SINK_CSV_RECORD_FLUSH = 1000;

    public static final int SINK_THREAD = 32;
    public static final int SINK_CONSUMER_THREAD = 8;

    // Transaction Service
    public static final int TRANSACTION_BATCH_SIZE = 100;

    // REMOTE BUFFER
    public static final String SINK_REMOTE_HOST = "localhost";
    public static final short SINK_REMOTE_PORT = 22942;
    public static final int SINK_BATCH_SIZE = 100;
    public static final int SINK_TIMEOUT_MS = 5000;
    public static final int SINK_FLUSH_INTERVAL_MS = 5000;
    public static final int SINK_MAX_RETRIES = 3;
    public static final boolean SINK_CSV_ENABLE_HEADER = false;
    public static final String SINK_RETINA_MODE = "stub";

    // Monitor Config
    public static final boolean SINK_MONITOR_ENABLED = true;
    public static final short SINK_MONITOR_PORT = 9464;

    // Mock RPC
    public static final boolean SINK_RPC_ENABLED = true;
    public static final String MAX_RECORDS_PER_FILE = "100000";
    public static final String STORAGE_MODE = "stream";
}
