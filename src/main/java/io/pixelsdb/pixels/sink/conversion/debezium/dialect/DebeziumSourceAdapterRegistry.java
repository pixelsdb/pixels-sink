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
package io.pixelsdb.pixels.sink.conversion.debezium.dialect;

import java.util.List;
import java.util.ServiceLoader;

public final class DebeziumSourceAdapterRegistry
{
    private static final List<DebeziumSourceAdapter> ADAPTERS = ServiceLoader
            .load(DebeziumSourceAdapter.class)
            .stream()
            .map(ServiceLoader.Provider::get)
            .toList();

    private DebeziumSourceAdapterRegistry()
    {
    }

    public static DebeziumSourceAdapter forSource(String connector)
    {
        return resolve(connector);
    }

    public static DebeziumSourceAdapter resolve(String connector)
    {
        if (connector == null || connector.isBlank())
        {
            throw new IllegalArgumentException("Unsupported Debezium connector: " + connector);
        }

        List<DebeziumSourceAdapter> matches = ADAPTERS.stream()
                .filter(adapter -> adapter.supports(connector))
                .toList();
        if (matches.size() == 1)
        {
            return matches.get(0);
        }
        if (matches.size() > 1)
        {
            throw new IllegalStateException("Multiple Debezium connector adapters support: " + connector);
        }
        throw new IllegalArgumentException("Unsupported Debezium connector: " + connector);
    }
}
