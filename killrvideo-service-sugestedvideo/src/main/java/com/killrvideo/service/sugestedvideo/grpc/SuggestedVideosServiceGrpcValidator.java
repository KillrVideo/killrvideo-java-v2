package com.killrvideo.service.sugestedvideo.grpc;

import static com.killrvideo.utils.ValidationUtils.initErrorString;
import static com.killrvideo.utils.ValidationUtils.validate;
import static org.apache.commons.lang3.StringUtils.isBlank;

import org.slf4j.Logger;
import org.springframework.util.Assert;

import io.grpc.stub.StreamObserver;
import killrvideo.suggested_videos.SuggestedVideosService.GetRelatedVideosRequest;
import killrvideo.suggested_videos.SuggestedVideosService.GetSuggestedForUserRequest;

public class SuggestedVideosServiceGrpcValidator {

    /**
     * Hind Constructor
     */
    private SuggestedVideosServiceGrpcValidator() {}
    
    
    public static void validateGrpcRequest_getRelatedVideo(Logger logger, GetRelatedVideosRequest request, StreamObserver<?> streamObserver) {
        final StringBuilder errorMessage = initErrorString(request);
        boolean isValid = true;

        if (request.getVideoId() == null || isBlank(request.getVideoId().getValue())) {
             errorMessage.append("\t\tvideo id should be provided for get related videos request\n");
            isValid = false;
        }
        Assert.isTrue(validate(logger, streamObserver, errorMessage, isValid), "Invalid parameter for 'getRelatedVideo'");
    }
    
    
    public static void validateGrpcRequest_getUserSuggestedVideo(Logger logger, GetSuggestedForUserRequest request, StreamObserver<?> streamObserver) {
        final StringBuilder errorMessage = initErrorString(request);
        boolean isValid = true;
        if (request.getUserId() == null || isBlank(request.getUserId().getValue())) {
            errorMessage.append("\t\tuser id should be provided for get suggested for user request\n");
            isValid = false;
        }
        Assert.isTrue(validate(logger, streamObserver, errorMessage, isValid), "Invalid parameter for 'getSuggestedForUser'");
    }
    
}
