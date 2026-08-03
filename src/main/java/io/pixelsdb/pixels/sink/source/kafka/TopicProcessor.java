/*
 * Copyright 2026 PixelsDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pixelsdb.pixels.sink.source.kafka;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.pipeline.TablePipelineManager;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public final class TopicProcessor implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicProcessor.class);

    private final Properties kafkaProperties;
    private final PixelsSinkConfig config;
    private final String[] includeTables;
    private final String bootstrapServers;
    private final String baseTopic;
    private final TablePipelineManager tablePipelineManager;
    private final Set<String> subscribedTopics = ConcurrentHashMap.newKeySet();
    private final Map<String, KafkaRowSource> activeSources = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Object sourceLifecycleLock = new Object();
    private AdminClient adminClient;
    private Timer timer;

    public TopicProcessor(
            PixelsSinkConfig config,
            Properties kafkaProperties,
            TablePipelineManager tablePipelineManager)
    {
        this.config = config;
        this.kafkaProperties = kafkaProperties;
        this.includeTables = config.getIncludeTables();
        this.bootstrapServers = config.getBootstrapServers();
        this.baseTopic = config.getTopicPrefix() + "." + config.getCaptureDatabase();
        this.tablePipelineManager = tablePipelineManager;
    }

    @Override
    public void run()
    {
        try
        {
            Properties adminProperties = new Properties();
            adminProperties.put(
                    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            adminClient = AdminClient.create(adminProperties);
            timer = new Timer("TopicProcessor-Timer", true);
            timer.scheduleAtFixedRate(new TopicMonitorTask(), 0, 5000);
            while (running.get())
            {
                try
                {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e)
                {
                    if (running.get())
                    {
                        LOGGER.warn("Kafka topic monitor interrupted", e);
                    }
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } finally
        {
            requestStop();
        }
    }

    void requestStop()
    {
        if (!running.compareAndSet(true, false))
        {
            return;
        }
        if (timer != null)
        {
            timer.cancel();
        }
        if (adminClient != null)
        {
            adminClient.close(Duration.ofSeconds(5));
        }
        synchronized (sourceLifecycleLock)
        {
            activeSources.values().forEach(KafkaRowSource::requestStop);
            activeSources.clear();
            executor.shutdown();
        }
        try
        {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS))
            {
                executor.shutdownNow();
            }
        } catch (InterruptedException e)
        {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    void abort()
    {
        running.set(false);
        if (timer != null)
        {
            timer.cancel();
        }
        if (adminClient != null)
        {
            adminClient.close(Duration.ofSeconds(5));
        }
        synchronized (sourceLifecycleLock)
        {
            activeSources.values().forEach(KafkaRowSource::requestStop);
            activeSources.clear();
            executor.shutdownNow();
        }
    }

    private class TopicMonitorTask extends TimerTask
    {
        @Override
        public void run()
        {
            if (!running.get())
            {
                cancel();
                return;
            }
            try
            {
                Set<String> topics = adminClient.listTopics().names().get(5, TimeUnit.SECONDS)
                        .stream()
                        .filter(topic -> topic.startsWith(baseTopic + "."))
                        .collect(Collectors.toSet());
                topics.stream()
                        .filter(topic -> !subscribedTopics.contains(topic))
                        .filter(TopicProcessor.this::shouldProcessTable)
                        .forEach(this::startTopic);
            } catch (Exception e)
            {
                if (running.get())
                {
                    LOGGER.warn("Failed to inspect Kafka topics", e);
                }
            }
        }

        private void startTopic(String topic)
        {
            synchronized (sourceLifecycleLock)
            {
                if (!running.get() || executor.isShutdown())
                {
                    return;
                }
                KafkaRowSource source = new KafkaRowSource(
                        kafkaProperties, topic, tablePipelineManager);
                activeSources.put(topic, source);
                subscribedTopics.add(topic);
                executor.submit(source);
            }
        }
    }

    private boolean shouldProcessTable(String topic)
    {
        String tableName = topic.substring(topic.lastIndexOf('.') + 1);
        return includeTables.length == 0 ||
                Arrays.stream(includeTables).anyMatch(tableName::equals);
    }
}
