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
package io.pixelsdb.pixels.sink.config;

import java.util.Locale;

/**
 * Resolves {@code sink.datasource.engine.format}. Only {@code connect} is implemented.
 */
public final class EngineValueFormat
{
    public static final String CONNECT = "connect";

    private EngineValueFormat()
    {
    }

    public static String resolve(String configuredFormat)
    {
        if (configuredFormat == null || configuredFormat.isBlank())
        {
            return CONNECT;
        }
        String normalized = normalize(configuredFormat);
        if (!CONNECT.equals(normalized))
        {
            throw new IllegalArgumentException(
                    "sink.datasource.engine.format='" + configuredFormat +
                            "' is not implemented yet; only 'connect' is supported. " +
                            "Json/Avro Engine paths will reuse conversion.debezium converters later.");
        }
        return normalized;
    }

    private static String normalize(String format)
    {
        return format.trim().toLowerCase(Locale.ROOT);
    }
}
