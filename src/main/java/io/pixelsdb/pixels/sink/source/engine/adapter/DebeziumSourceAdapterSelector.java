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
package io.pixelsdb.pixels.sink.source.engine.adapter;

import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.conversion.debezium.dialect.DebeziumSourceAdapter;
import io.pixelsdb.pixels.sink.conversion.debezium.dialect.DebeziumSourceAdapterRegistry;

/**
 * Wiring-side selector for sink CDC dialect ({@code sink.debezium.dialect},
 * with fallback to {@code debezium.connector.class}).
 */
public final class DebeziumSourceAdapterSelector
{
    private DebeziumSourceAdapterSelector()
    {
    }

    public static DebeziumSourceAdapter configured()
    {
        return DebeziumSourceAdapterRegistry.resolve(
                PixelsSinkConfigFactory.getInstance().resolveDebeziumSourceDialect());
    }

    public static DebeziumSourceAdapter configuredIfPresent()
    {
        String dialect =
                PixelsSinkConfigFactory.getInstance().resolveDebeziumSourceDialect();
        return dialect == null || dialect.isBlank()
                ? null
                : DebeziumSourceAdapterRegistry.resolve(dialect);
    }
}
