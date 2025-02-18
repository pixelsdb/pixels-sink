package io.pixelsdb.pixels.sink.pojo;

import java.util.Map;

public class DebeziumMessageDTO {

    private Map<String, Object> before;
    private Map<String, Object> after;
    private Map<String, Object> source;
    private String op;
    private long tsMs;

    // Getters 和 Setters
    public Map<String, Object> getBefore() {
        return before;
    }

    public void setBefore(Map<String, Object> before) {
        this.before = before;
    }

    public Map<String, Object> getAfter() {
        return after;
    }

    public void setAfter(Map<String, Object> after) {
        this.after = after;
    }

    public Map<String, Object> getSource() {
        return source;
    }

    public void setSource(Map<String, Object> source) {
        this.source = source;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public long getTsMs() {
        return tsMs;
    }

    public void setTsMs(long tsMs) {
        this.tsMs = tsMs;
    }

    @Override
    public String toString() {
        return "DebeziumMessage{" +
                "before=" + before +
                ", after=" + after +
                ", source=" + source +
                ", op='" + op + '\'' +
                ", tsMs=" + tsMs +
                '}';
    }
}