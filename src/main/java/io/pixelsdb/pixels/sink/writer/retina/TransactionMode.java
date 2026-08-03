/*
 * Copyright 2023 PixelsDB.
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

public enum TransactionMode
{
    SINGLE,
    RECORD,
    BATCH;

    public static TransactionMode fromValue(String value)
    {
        for (TransactionMode mode : values())
        {
            if (mode.name().equalsIgnoreCase(value))
            {
                return mode;
            }
        }
        throw new RuntimeException(String.format("Can't convert %s to transaction mode", value));
    }
}
