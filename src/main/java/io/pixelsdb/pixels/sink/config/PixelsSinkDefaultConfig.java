package io.pixelsdb.pixels.sink.config;

import io.pixelsdb.pixels.sink.deserializer.RowChangeEventDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class PixelsSinkDefaultConfig {
    public static final String PROPERTIES_PATH = "pixels-sink.properties";
    public static final String CSV_SINK_PATH = "./data";

    public static final String KEY_DESERIALIZER = StringDeserializer.class.getName(); // org.apache.kafka.common.serialization.StringDeserializer
    public static final String VALUE_DESERIALIZER = RowChangeEventDeserializer.class.getName();

    public static final String SINK_MODE = "csv";

    public static final int SINK_CSV_RECORD_FLUSH = 1000;

    public static final int SINK_THREAD = 4;
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
}
