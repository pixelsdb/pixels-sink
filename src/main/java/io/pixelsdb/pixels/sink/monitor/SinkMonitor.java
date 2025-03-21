package io.pixelsdb.pixels.sink.monitor;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;
import io.pixelsdb.pixels.sink.config.factory.KafkaPropFactorySelector;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;

import java.util.Properties;

public class SinkMonitor implements StoppableMonitor {
    private MonitorThreadManager manager;
    private volatile boolean running = true;

    public void startSinkMonitor() {
        PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();
        KafkaPropFactorySelector kafkaPropFactorySelector = new KafkaPropFactorySelector();

        Properties transactionKafkaProperties = kafkaPropFactorySelector
                .getFactory(PixelsSinkConstants.TRANSACTION_KAFKA_PROP_FACTORY)
                .createKafkaProperties(pixelsSinkConfig);
        TransactionMonitor transactionMonitor = new TransactionMonitor(pixelsSinkConfig, transactionKafkaProperties);

        Properties topicKafkaProperties = kafkaPropFactorySelector
                .getFactory(PixelsSinkConstants.ROW_RECORD_KAFKA_PROP_FACTORY)
                .createKafkaProperties(pixelsSinkConfig);
        TopicMonitor topicMonitor = new TopicMonitor(pixelsSinkConfig, topicKafkaProperties);

        manager = new MonitorThreadManager();
        manager.startMonitor(transactionMonitor);
        manager.startMonitor(topicMonitor);
    }


    @Override
    public void stopMonitor() {
        manager.shutdown();
        running = false;
    }

    public boolean isRunning() {
        return running;
    }
}
