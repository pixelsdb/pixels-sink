package io.pixelsdb.pixels.sink.config;

import io.pixelsdb.pixels.sink.deserializer.TransactionJsonMessageDeserializer;

public class TransactionConfig {
    public static final String DEFAULT_TRANSACTION_TOPIC_SUFFIX = "transaction";
    public static final String DEFAULT_TRANSACTION_TOPIC_KEY_DESERIALIZER = TransactionJsonMessageDeserializer.class.getName();
    public static final String DEFAULT_TRANSACTION_TOPIC_GROUP_ID= "transaction_consumer";

    public static final String DEFAULT_TRANSACTION_TIME_OUT = "300";
}
