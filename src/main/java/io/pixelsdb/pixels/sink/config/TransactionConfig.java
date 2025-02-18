package io.pixelsdb.pixels.sink.config;

import io.pixelsdb.pixels.sink.deserializer.TransactionMessageDeserializer;

public class TransactionConfig {
    public static final String DEFAULT_TRANSACTION_TOPIC_SUFFIX = "transaction";
    public static final String DEFAULT_TRANSACTION_TOPIC_KEY_DESERIALIZER = TransactionMessageDeserializer.class.getName();;
}
