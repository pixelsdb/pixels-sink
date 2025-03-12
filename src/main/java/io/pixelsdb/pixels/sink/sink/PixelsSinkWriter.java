package io.pixelsdb.pixels.sink.sink;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public interface PixelsSinkWriter extends Closeable {
    void flush();

    boolean write(Map<String, Object> message) throws IOException;
}
