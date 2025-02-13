package io.pixelsdb.pixels.sink.processer;


import java.util.Map;

public interface MessageProcessor {
    void processMessage(String tableName, Map<String, Object> message) throws Exception;
}
