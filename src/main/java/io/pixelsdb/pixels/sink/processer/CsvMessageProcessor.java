package io.pixelsdb.pixels.sink.processer;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import io.pixelsdb.pixels.sink.writer.CsvWriter;
public class CsvMessageProcessor implements MessageProcessor {
    @Override
    public void processMessage(String tableName, Map<String, Object> message) throws IOException {
        // CsvWriter.writeToCsv(tableName + ".csv", message);
    }
}