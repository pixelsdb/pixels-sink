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
