package io.pixelsdb.pixels.sink.monitor;

import io.pixelsdb.pixels.sink.TableConsumerTask;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TopicMonitor extends Thread {

    private static final Logger log = LoggerFactory.getLogger(TopicMonitor.class);
    private final Properties kafkaProperties;
    private final PixelsSinkConfig pixelsSinkConfig;
    private final String[] includeTables;
    private final Set<String> subscribedTopics;
    private final String bootsTrapServers;
    private final ExecutorService executorService;
    private final String baseTopic;

    public TopicMonitor(PixelsSinkConfig pixelsSinkConfig, Properties kafkaProperties) {
        this.pixelsSinkConfig = pixelsSinkConfig;
        this.kafkaProperties = kafkaProperties;
        this.baseTopic = pixelsSinkConfig.getTopicPrefix() + "." + pixelsSinkConfig.getCaptureDatabase();
        this.includeTables = pixelsSinkConfig.getIncludeTables();
        this.subscribedTopics = new HashSet<>();
        this.bootsTrapServers = pixelsSinkConfig.getBootstrapServers();
        this.executorService = Executors.newCachedThreadPool();
    }

    @Override
    public void run() {
        String topicPrefix = baseTopic + ".";
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootsTrapServers);
        AdminClient adminClient = AdminClient.create(props);
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TopicMonitorTask(adminClient, topicPrefix), 0, 5000);
        // timer.schedule(new TopicMonitorTask(this.bootsTrapServers, baseTopic), 0);
    }

    private Set<String> detectNewTopics(Set<String> currentTopics) {
        Set<String> newTopics = new java.util.HashSet<>(currentTopics);
        newTopics.removeAll(subscribedTopics);
        Set<String> filteredNewTopics = newTopics.stream()
                .filter(this::shouldProcessTable)
                .collect(java.util.stream.Collectors.toSet());
        return newTopics;
    }

    private static Set<String> filterTopics(Set<String> topics, String prefix) {
        return topics.stream()
                .filter(topic -> topic.startsWith(prefix))
                .collect(java.util.stream.Collectors.toSet());
    }

    private boolean shouldProcessTable(String tableName) {
        if (includeTables.length == 0) {
            return true;
        }
        for (String table : includeTables) {
            if (tableName.equals(table)) {
                return true;
            }
        }
        return false;
    }

    private String extractTableName(String topic) {
        String[] parts = topic.split("\\.");
        return parts[parts.length - 1];
    }

    private class TopicMonitorTask extends TimerTask {
        private final AdminClient adminClient;
        private final String topicPrefix;

        TopicMonitorTask(AdminClient adminClient, String topicPrefix) {
            this.adminClient = adminClient;
            this.topicPrefix = topicPrefix;
        }

        @Override
        public void run() {
            try {
                ListTopicsResult listTopicsResult = adminClient.listTopics();
                Set<String> currentTopics = listTopicsResult.names().get();
                Set<String> filteredTopics = filterTopics(currentTopics, this.topicPrefix);

                Set<String> newTopics = detectNewTopics(filteredTopics);
                if (!newTopics.isEmpty()) {
                    for (String newTopic : newTopics) {
                        log.info("Discover new topic: " + newTopic);
                        TableConsumerTask task = new TableConsumerTask(pixelsSinkConfig, kafkaProperties, newTopic);
                        executorService.submit(task);
                        subscribedTopics.add(newTopic);
                    }
                }
            } catch (Exception e) {
                log.error(e.toString());
            }
        }
    }

}

