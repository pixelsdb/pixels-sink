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

package io.pixelsdb.pixels.sink.config.factory;

import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;

import java.util.HashMap;

public class KafkaPropFactorySelector {
    private final HashMap<String, KafkaPropFactory> factories = new HashMap<>();

    public KafkaPropFactorySelector() {
        factories.put(PixelsSinkConstants.TRANSACTION_KAFKA_PROP_FACTORY, new TransactionKafkaPropFactory());
        factories.put(PixelsSinkConstants.ROW_RECORD_KAFKA_PROP_FACTORY, new RowRecordKafkaPropFactory());
    }

    public KafkaPropFactory getFactory(String type) {
        if (!factories.containsKey(type)) {
            throw new IllegalArgumentException("Unknown factory type: " + type);
        }
        return factories.get(type);
    }
}
