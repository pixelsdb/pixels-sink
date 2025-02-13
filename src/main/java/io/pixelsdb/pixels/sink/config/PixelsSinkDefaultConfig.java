package io.pixelsdb.pixels.sink.config;

import io.pixelsdb.pixels.sink.deserializer.DebeziumJsonMessageDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class PixelsSinkDefaultConfig {
    public static final String PROPERTIES_PATH = "pixels-sink.properties";
    public static final String CSV_SINK_PATH = "./data";

    public static final String KEY_DESERIALIZER = StringDeserializer.class.getName(); // org.apache.kafka.common.serialization.StringDeserializer
    public static final String VALUE_DESERIALIZER = DebeziumJsonMessageDeserializer.class.getName();

    public static final Long CSV_RECORD_FLUSH = 1000L;
}
