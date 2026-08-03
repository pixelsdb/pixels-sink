/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.sink.util;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.etcd.jetcd.KeyValue;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @package: io.pixelsdb.pixels.sink.util
 * @className: EtcdFileRegistry
 * @author: AntiO2
 * @date: 2025/10/5 08:24
 */
public class EtcdFileRegistry
{
    private static final Logger LOGGER = LoggerFactory.getLogger(EtcdFileRegistry.class);

    private static final String REGISTRY_PREFIX = "/sink/proto/registry/";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final String topic;
    private final String baseDir;
    private final EtcdUtil etcd = EtcdUtil.Instance();
    private final AtomicInteger nextFileId = new AtomicInteger(0);
    private String currentFileKey;

    public EtcdFileRegistry(String topic, String baseDir)
    {
        this.topic = topic;
        this.baseDir = baseDir;
        initRegistry();
    }

    public static String extractPath(String etcdValue)
    {
        try
        {
            Map meta = OBJECT_MAPPER.readValue(etcdValue, Map.class);
            return (String) meta.get("path");
        } catch (IOException e)
        {
            LOGGER.error("Failed to parse etcd value: {}", etcdValue, e);
            return null;
        }
    }

    private void initRegistry()
    {
        List<KeyValue> files = etcd.getKeyValuesByPrefix(filePrefix());
        if (!files.isEmpty())
        {
            int maxId = files.stream()
                    .mapToInt(kv -> extractFileId(kv.getKey().toString()))
                    .max()
                    .orElse(0);
            nextFileId.set(maxId + 1);
            LOGGER.info("Initialized registry for topic {} with nextFileId={}", topic, nextFileId.get());
        } else
        {
            LOGGER.info("No existing files found for topic {}, starting fresh", topic);
        }
    }

    private String topicPrefix()
    {
        return REGISTRY_PREFIX + topic;
    }

    private String filePrefix()
    {
        return topicPrefix() + "/files/";
    }

    private int extractFileId(String key)
    {
        try
        {
            String fileName = key.substring(key.lastIndexOf('/') + 1);
            String id = fileName.replace(".proto", "");
            return Integer.parseInt(id);
        } catch (Exception e)
        {
            return 0;
        }
    }

    /**
     * Create a new file and register it in etcd.
     */
    public synchronized String createNewFile()
    {
        String fileName = String.format("%05d.proto", nextFileId.getAndIncrement());
        String fullPath = baseDir + "/" + topic + "/" + fileName;

        Map<String, String> fileMeta = new HashMap<>();
        fileMeta.put("path", fullPath);
        fileMeta.put("created_at", String.valueOf(System.currentTimeMillis()));
        fileMeta.put("status", "active");
        currentFileKey = filePrefix() + fileName;

        String jsonValue = null;
        try
        {
            jsonValue = OBJECT_MAPPER.writeValueAsString(fileMeta);
        } catch (JsonProcessingException e)
        {
            throw new RuntimeException(e);
        }

        currentFileKey = filePrefix() + fileName;
        etcd.putKeyValue(currentFileKey, jsonValue);
        etcd.putKeyValue(topicPrefix() + "/current", fileName);
        LOGGER.info("Created new file [{}] for topic [{}]", fileName, topic);
        return fullPath;
    }

    public synchronized String getCurrentFileKey()
    {
        return currentFileKey;
    }

    /**
     * List all files (for readers).
     */
    public List<String> listAllFiles()
    {
        List<KeyValue> files = etcd.getKeyValuesByPrefix(filePrefix());
        return files.stream()
                .map(kv ->
                {
                    String value = kv.getValue().toString();
                    return extractPath(value);
                })
                .sorted()
                .collect(Collectors.toList());
    }

    /**
     * Mark a file as completed (for writer rotation).
     */
    public void markFileCompleted(String fileName)
    {
        KeyValue kv = etcd.getKeyValue(fileName);
        if (kv == null) return;

        Map meta = null;
        try
        {
            meta = OBJECT_MAPPER.readValue(kv.getValue().toString(), Map.class);
            meta.put("completed_at", String.valueOf(System.currentTimeMillis()));
            meta.put("status", "completed");
            String jsonValue = OBJECT_MAPPER.writeValueAsString(meta);
            etcd.putKeyValue(fileName, jsonValue);
        } catch (JsonProcessingException e)
        {
            throw new RuntimeException(e);
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        LOGGER.info("Marked file [{}] as completed", fileName);
    }

}
