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
package io.pixelsdb.pixels.sink.writer.retina;

import io.pixelsdb.pixels.sink.TestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("integration")
class TableWriterProxyTest
{
    private static final String TABLE_NAME = "test";

    @BeforeAll
    static void init() throws Exception
    {
        TestConfig.initializeIntegrationConfig();
    }

    @Test
    void shouldReuseTableWriter() throws IOException
    {
        TableWriterProxy tableWriterProxy = TableWriterProxy.getInstance();
        TableWriter first = tableWriterProxy.getTableWriter(TABLE_NAME, 0, 0);

        for (int i = 0; i < 10; i++)
        {
            TableWriter tableWriter = tableWriterProxy.getTableWriter(TABLE_NAME, 0, 0);
            assertNotNull(tableWriter);
            assertSame(first, tableWriter);
        }
    }
}
