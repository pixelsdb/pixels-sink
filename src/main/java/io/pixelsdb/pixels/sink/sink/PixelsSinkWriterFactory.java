package io.pixelsdb.pixels.sink.sink;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;

import java.io.IOException;

public class PixelsSinkWriterFactory {
    static PixelsSinkWriter getWriterByConfig(PixelsSinkConfig pixelsSinkConfig, String tableName) throws IOException {
        switch (pixelsSinkConfig.getPixelsSinkMode()) {
            case CSV:
                return new CsvWriter(pixelsSinkConfig, tableName);
            case BUFFER:
                return new RemoteBufferWriter(pixelsSinkConfig);
        }
        return null;
    }
}
