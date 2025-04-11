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

package io.pixelsdb.pixels.sink.config;

import io.pixelsdb.pixels.sink.deserializer.TransactionJsonMessageDeserializer;

public class TransactionConfig {
    public static final String DEFAULT_TRANSACTION_TOPIC_SUFFIX = "transaction";
    public static final String DEFAULT_TRANSACTION_TOPIC_VALUE_DESERIALIZER = TransactionJsonMessageDeserializer.class.getName();
    public static final String DEFAULT_TRANSACTION_TOPIC_GROUP_ID= "transaction_consumer";

    public static final String DEFAULT_TRANSACTION_TIME_OUT = "300";
}
