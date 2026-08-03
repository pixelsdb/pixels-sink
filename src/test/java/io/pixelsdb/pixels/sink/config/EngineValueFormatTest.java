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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EngineValueFormatTest
{
    @Test
    void shouldDefaultToConnect()
    {
        assertEquals("connect", EngineValueFormat.resolve(null));
        assertEquals("connect", EngineValueFormat.resolve(""));
        assertEquals("connect", EngineValueFormat.resolve("CONNECT"));
    }

    @Test
    void shouldRejectNonConnectOnResolve()
    {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> EngineValueFormat.resolve("json"));
        assertTrue(error.getMessage().contains("not implemented"));
    }
}
