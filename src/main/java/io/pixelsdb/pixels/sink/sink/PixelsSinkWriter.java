package io.pixelsdb.pixels.sink.sink;

import java.io.Closeable;
import java.util.Map;

public interface PixelsSinkWriter extends Closeable {
    void flush();

    void write(Map<String, Object> message);
}
