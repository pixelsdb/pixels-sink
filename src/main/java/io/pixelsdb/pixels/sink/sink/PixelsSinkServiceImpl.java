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

package io.pixelsdb.pixels.sink.sink;

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.sink.proto.PixelsSinkServiceGrpc;
import io.pixelsdb.pixels.sink.proto.SinkProto;

public class PixelsSinkServiceImpl extends PixelsSinkServiceGrpc.PixelsSinkServiceImplBase {
    @Override
    public void writeData(SinkProto.WriteRequest request, StreamObserver<SinkProto.WriteResponse> responseObserver) {
        super.writeData(request, responseObserver);
    }

    @Override
    public void flush(SinkProto.FlushRequest request, StreamObserver<SinkProto.FlushResponse> responseObserver) {
        super.flush(request, responseObserver);
    }
}
