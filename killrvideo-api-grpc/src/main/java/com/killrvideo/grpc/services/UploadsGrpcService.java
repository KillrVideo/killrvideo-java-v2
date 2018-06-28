package com.killrvideo.grpc.services;

import org.springframework.stereotype.Service;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import killrvideo.uploads.UploadsServiceGrpc.UploadsServiceImplBase;
import killrvideo.uploads.UploadsServiceOuterClass.GetStatusOfVideoRequest;
import killrvideo.uploads.UploadsServiceOuterClass.GetStatusOfVideoResponse;
import killrvideo.uploads.UploadsServiceOuterClass.GetUploadDestinationRequest;
import killrvideo.uploads.UploadsServiceOuterClass.GetUploadDestinationResponse;
import killrvideo.uploads.UploadsServiceOuterClass.MarkUploadCompleteRequest;
import killrvideo.uploads.UploadsServiceOuterClass.MarkUploadCompleteResponse;

/**
 * Updload video from UI.
 *
 * @author DataStax Evangelist Team
 */
@Service
public class UploadsGrpcService extends UploadsServiceImplBase {
    
    /** {@inheritDoc} */
    @Override
    public void getUploadDestination(
            final GetUploadDestinationRequest request, 
            final StreamObserver<GetUploadDestinationResponse> responseObserver) {
        responseObserver
                .onError(Status.UNIMPLEMENTED
                .withDescription("Uploading videos is currently not supported")
                .asRuntimeException());
    }

    /** {@inheritDoc} */
    @Override
    public void markUploadComplete(
            final MarkUploadCompleteRequest request, 
            final StreamObserver<MarkUploadCompleteResponse> responseObserver) {
        responseObserver
                .onError(Status.UNIMPLEMENTED
                .withDescription("Uploading videos is currently not supported")
                .asRuntimeException());
    }

    /** {@inheritDoc} */
    @Override
    public void getStatusOfVideo(
            final GetStatusOfVideoRequest request, 
            final StreamObserver<GetStatusOfVideoResponse> responseObserver) {
        responseObserver
                .onError(Status.UNIMPLEMENTED
                .withDescription("Uploading videos is currently not supported")
                .asRuntimeException());
    }
   
}
