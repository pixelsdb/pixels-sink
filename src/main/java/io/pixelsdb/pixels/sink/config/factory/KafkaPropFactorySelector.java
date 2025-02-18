package io.pixelsdb.pixels.sink.config.factory;

import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;

import java.util.HashMap;

public class KafkaPropFactorySelector {
    private final HashMap<String, KafkaPropFactory> factories = new HashMap<>();

    public KafkaPropFactorySelector() {
        factories.put(PixelsSinkConstants.TRANSACTION_KAFKA_PROP_FACTORY, new TransactionKafkaPropFactory());
        factories.put(PixelsSinkConstants.ROW_RECORD_KAFKA_PROP_FACTORY, new RowRecordKafkaPropFactory());
    }

    public KafkaPropFactory getFactory(String type) {
        if (!factories.containsKey(type)) {
            throw new IllegalArgumentException("Unknown factory type: " + type);
        }
        return factories.get(type);
    }
}
