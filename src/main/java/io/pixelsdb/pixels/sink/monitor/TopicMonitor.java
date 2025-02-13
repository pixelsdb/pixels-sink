package io.pixelsdb.pixels.sink.monitor;

import io.pixelsdb.pixels.sink.TableConsumerTask;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.internals.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Properties;

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
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TopicMonitorTask(this.bootsTrapServers, baseTopic), 0, 5000);
        // timer.schedule(new TopicMonitorTask(this.bootsTrapServers, baseTopic), 0);
    }

    private class TopicMonitorTask extends TimerTask {
        private final String bootsTrapServers;
        private final String topicPrefix;

        TopicMonitorTask(String bootsTrapServers, String baseTopic) {
            this.bootsTrapServers = bootsTrapServers;
            this.topicPrefix = baseTopic + ".";
        }

        @Override
        public void run() {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootsTrapServers);
            try (AdminClient adminClient = AdminClient.create(props)) {
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

    private Set<String> detectNewTopics(Set<String> currentTopics) {
        Set<String> newTopics = new java.util.HashSet<>(currentTopics);
        newTopics.removeAll(subscribedTopics);
        Set<String> filteredNewTopics = newTopics.stream()
                .filter(topic -> shouldProcessTable(topic))
                .collect(java.util.stream.Collectors.toSet());
        return newTopics;
    }

}

