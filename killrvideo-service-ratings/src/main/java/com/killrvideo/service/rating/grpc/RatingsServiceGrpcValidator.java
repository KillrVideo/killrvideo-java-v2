package com.killrvideo.service.rating.grpc;

import static com.killrvideo.utils.ValidationUtils.initErrorString;
import static com.killrvideo.utils.ValidationUtils.validate;
import static org.apache.commons.lang3.StringUtils.isBlank;

import org.slf4j.Logger;
import org.springframework.util.Assert;

import io.grpc.stub.StreamObserver;
import killrvideo.ratings.RatingsServiceOuterClass.GetRatingRequest;
import killrvideo.ratings.RatingsServiceOuterClass.GetUserRatingRequest;
import killrvideo.ratings.RatingsServiceOuterClass.RateVideoRequest;

public class RatingsServiceGrpcValidator  {

    /**
     * Hide constructor.
     */
    private RatingsServiceGrpcValidator() {
    }
    
    public static void validateGrpcRequest_RateVideo(Logger logger, RateVideoRequest request, StreamObserver<?> streamObserver) {
        final StringBuilder errorMessage = initErrorString(request);
        boolean isValid = true;
        if (!request.hasVideoId() || isBlank(request.getVideoId().getValue())) {
            errorMessage.append("\t\tvideo id should be provided for rate video request\n");
            isValid = false;
        }
        if (!request.hasUserId() || isBlank(request.getUserId().getValue())) {
            errorMessage.append("\t\tuser id should be provided for rate video request");
            isValid = false;
        }
        Assert.isTrue(validate(logger, streamObserver, errorMessage, isValid), "Invalid parameter for 'rateVideo'");
    }
    
    public static void validateGrpcRequest_GetRating(Logger logger, GetRatingRequest request, StreamObserver<?> streamObserver) {
        final StringBuilder errorMessage = initErrorString(request);
        boolean isValid = true;

        if (!request.hasVideoId() || isBlank(request.getVideoId().getValue())) {
            errorMessage.append("\t\tvideo id should be provided for get video rating request\n");
            isValid = false;
        }
        Assert.isTrue(validate(logger, streamObserver, errorMessage, isValid), "Invalid parameter for 'getRating'");
    }
    
    public static void validateGrpcRequest_GetUserRating(Logger logger,GetUserRatingRequest request, StreamObserver<?> streamObserver) {
        final StringBuilder errorMessage = initErrorString(request);
        boolean isValid = true;
        if (!request.hasVideoId() || isBlank(request.getVideoId().getValue())) {
            errorMessage.append("\t\tvideo id should be provided for get user rating request\n");
            isValid = false;
        }
        if (!request.hasUserId() || isBlank(request.getUserId().getValue())) {
            errorMessage.append("\t\tuser id should be provided for get user rating request\n");
            isValid = false;
        }
        Assert.isTrue(validate(logger, streamObserver, errorMessage, isValid), "Invalid parameter for 'getUserRating'");
    }
    
    
}
