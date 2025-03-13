package io.pixelsdb.pixels.sink.sink;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;

import java.io.IOException;

public class PixelsSinkWriterFactory {
    static public PixelsSinkWriter getWriterByConfig(PixelsSinkConfig pixelsSinkConfig) throws IOException {
        switch (pixelsSinkConfig.getPixelsSinkMode()) {
            case CSV:
                return new CsvWriter(pixelsSinkConfig);
            case BUFFER:
                return new RemoteBufferWriter(pixelsSinkConfig);
        }
        return null;
    }
}
