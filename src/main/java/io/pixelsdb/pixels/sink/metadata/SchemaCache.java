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
 *
 */

package io.pixelsdb.pixels.sink.metadata;

import io.pixelsdb.pixels.core.TypeDescription;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

public class SchemaCache {
    private static final SchemaCache INSTANCE = new SchemaCache();
    private final ConcurrentMap<TableMetadataKey, TypeDescription> cache = new ConcurrentHashMap<>();

    private SchemaCache() {
    }

    ;

    public static SchemaCache getInstance() {
        return INSTANCE;
    }

    public TypeDescription computeIfAbsent(TableMetadataKey tableMetadataKey, Supplier<TypeDescription> supplier) {
        return cache.computeIfAbsent(tableMetadataKey, k -> supplier.get());
    }

    //  public TypeDescription get(String topic) {
//        return cache.get(topic);
//    }
}