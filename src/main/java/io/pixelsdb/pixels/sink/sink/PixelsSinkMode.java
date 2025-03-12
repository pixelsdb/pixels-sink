package io.pixelsdb.pixels.sink.sink;


public enum PixelsSinkMode {
    CSV,
    BUFFER;

    public static PixelsSinkMode fromValue(String value) {
        for (PixelsSinkMode mode : values()) {
            if (mode.name().equalsIgnoreCase(value)) {
                return mode;
            }
        }
        throw new RuntimeException(String.format("Can't convert %s to sink type", value));
    }
}
