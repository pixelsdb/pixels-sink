package io.pixelsdb.pixels.sink.sink;

import io.pixelsdb.pixels.sink.core.event.RowChangeEvent;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public interface PixelsSinkWriter extends Closeable {
    void flush();

    @Deprecated
    boolean write(Map<String, Object> message) throws IOException;

    boolean write(RowChangeEvent rowChangeEvent);

    // boolean write(RowChangeEvent rowChangeEvent, ByteBuffer byteBuffer);
}
