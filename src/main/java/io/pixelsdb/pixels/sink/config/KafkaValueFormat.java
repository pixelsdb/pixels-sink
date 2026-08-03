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
 * Resolves {@code sink.kafka.value.format}. Supported values: {@code json}, {@code avro}.
 */
public final class KafkaValueFormat
{
    public static final String JSON = "json";
    public static final String AVRO = "avro";

    private KafkaValueFormat()
    {
    }

    public static String resolve(String configuredFormat)
    {
        if (configuredFormat == null || configuredFormat.isBlank())
        {
            return JSON;
        }
        return normalize(configuredFormat);
    }

    private static String normalize(String format)
    {
        String normalized = format.trim().toLowerCase(Locale.ROOT);
        if (!JSON.equals(normalized) && !AVRO.equals(normalized))
        {
            throw new IllegalArgumentException(
                    "sink.kafka.value.format must be json or avro, got: " + format);
        }
        return normalized;
    }
}
