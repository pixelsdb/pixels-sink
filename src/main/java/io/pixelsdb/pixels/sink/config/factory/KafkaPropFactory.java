package io.pixelsdb.pixels.sink.config.factory;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;

import java.util.Properties;

public interface KafkaPropFactory {
    Properties createKafkaProperties(PixelsSinkConfig config);
}
