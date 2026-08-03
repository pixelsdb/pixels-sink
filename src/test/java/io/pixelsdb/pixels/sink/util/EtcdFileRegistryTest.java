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


import io.pixelsdb.pixels.common.utils.EtcdUtil;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @package: io.pixelsdb.pixels.sink.util
 * @className: EtcdFileRegistryTest
 * @author: AntiO2
 * @date: 2025/10/5 08:54
 */
@Tag("integration")
class EtcdFileRegistryTest
{
    @Test
    void shouldCreateAndListCompletedFiles(@TempDir Path tempDir)
    {
        String topic = "test-" + UUID.randomUUID();
        try
        {
            EtcdFileRegistry etcdFileRegistry = new EtcdFileRegistry(
                    topic, tempDir.resolve("ray").toUri().toString());
            for (int i = 0; i < 10; i++)
            {
                String newFile = etcdFileRegistry.createNewFile();
                etcdFileRegistry.markFileCompleted(newFile);
            }
            List<String> files = etcdFileRegistry.listAllFiles();

            assertEquals(10, files.size());
        } finally
        {
            EtcdUtil.Instance().deleteByPrefix("/sink/proto/registry/" + topic);
        }
    }
}
