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

package io.pixelsdb.pixels.sink.pojo.enums;

import io.pixelsdb.pixels.sink.proto.SinkProto;

import java.util.Objects;

public class OperationType {

    public static SinkProto.OperationType fromString(String op) {
        if (Objects.equals(op, "c")) {
            return SinkProto.OperationType.INSERT;
        }
        if (Objects.equals(op, "u")) {
            return SinkProto.OperationType.UPDATE;
        }
        if (Objects.equals(op, "d")) {
            return SinkProto.OperationType.DELETE;
        }

        if (Objects.equals(op, "r")) {
            return SinkProto.OperationType.SNAPSHOT;
        }
        throw new RuntimeException(String.format("Can't convert %s to operation type", op));
    }
}
