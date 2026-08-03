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
package io.pixelsdb.pixels.sink.config;


import io.pixelsdb.pixels.sink.writer.PixelsSinkMode;
import io.pixelsdb.pixels.sink.writer.retina.RetinaServiceProxy;
import io.pixelsdb.pixels.sink.writer.retina.TransactionMode;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;

public class ConfigLoader
{
    public static void load(Properties props, Object target)
    {
        try
        {
            Class<?> clazz = target.getClass();
            for (Field field : clazz.getDeclaredFields())
            {
                ConfigKey annotation = field.getAnnotation(ConfigKey.class);
                if (annotation != null)
                {
                    String key = annotation.value();
                    String value = props.getProperty(key);
                    if (value == null || value.isEmpty())
                    {
                        if (!annotation.defaultValue().isEmpty())
                        {
                            value = annotation.defaultValue();
                        } else if (annotation.defaultClass() != Void.class)
                        {
                            value = annotation.defaultClass().getName();
                        }
                    }
                    Object parsed = convert(value, field.getType());
                    field.setAccessible(true);
                    try
                    {
                        field.set(target, parsed);
                    } catch (IllegalAccessException e)
                    {
                        throw new RuntimeException("Failed to inject config for " + key, e);
                    }
                }
            }
        } catch (Exception e)
        {
            throw new RuntimeException("Failed to load config", e);
        }
    }

    private static Object convert(String value, Class<?> type)
    {
        if (type.equals(int.class) || type.equals(Integer.class))
        {
            return Integer.parseInt(value);
        } else if (type.equals(long.class) || type.equals(Long.class))
        {
            return Long.parseLong(value);
        } else if (type.equals(short.class) || type.equals(Short.class))
        {
            return Short.parseShort(value);
        } else if (type.equals(boolean.class) || type.equals(Boolean.class))
        {
            return Boolean.parseBoolean(value);
        } else if (type.equals(PixelsSinkMode.class))
        {
            return PixelsSinkMode.fromValue(value);
        } else if (type.equals(TransactionMode.class))
        {
            return TransactionMode.fromValue(value);
        } else if (type.equals(RetinaServiceProxy.RetinaWriteMode.class))
        {
            return RetinaServiceProxy.RetinaWriteMode.fromValue(value);
        } else if (type.equals(List.class))
        {
            // Handle List type: split the string by comma (",")
            // and return a List<String>. Trimming each element is recommended.
            if (value == null || value.isEmpty())
            {
                return java.util.Collections.emptyList();
            }

            // Use Arrays.asList(String.split(",")) to handle the splitting,
            // then stream to trim whitespace from each element.
            return java.util.Arrays.stream(value.split(","))
                    .map(String::trim)
                    .collect(java.util.stream.Collectors.toList());
        }
        return value;
    }
}

