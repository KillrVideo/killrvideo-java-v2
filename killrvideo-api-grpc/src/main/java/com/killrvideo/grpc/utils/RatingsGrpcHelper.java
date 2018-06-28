package com.killrvideo.grpc.utils;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.time.Instant;
import java.util.Optional;

import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import com.killrvideo.dse.model.VideoRating;
import com.killrvideo.dse.model.VideoRatingByUser;
import com.killrvideo.messaging.MessagingDao;

import io.grpc.stub.StreamObserver;
import killrvideo.ratings.RatingsServiceOuterClass.GetRatingRequest;
import killrvideo.ratings.RatingsServiceOuterClass.GetRatingResponse;
import killrvideo.ratings.RatingsServiceOuterClass.GetUserRatingRequest;
import killrvideo.ratings.RatingsServiceOuterClass.GetUserRatingResponse;
import killrvideo.ratings.RatingsServiceOuterClass.RateVideoRequest;
import killrvideo.ratings.events.RatingsEvents.UserRatedVideo;

/**
 * Helper and mappers for DAO <=> GRPC Communications
 *
 * @author DataStax Evangelist Team
 */
@Component
public class RatingsGrpcHelper extends AbstractGrpcHelper {
    
    @Autowired
    private MessagingDao msgDao;
    
    public void validateGrpcRequest_RateVideo(Logger logger, RateVideoRequest request, StreamObserver<?> streamObserver) {
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
    
    public void validateGrpcRequest_GetRating(Logger logger, GetRatingRequest request, StreamObserver<?> streamObserver) {
        final StringBuilder errorMessage = initErrorString(request);
        boolean isValid = true;

        if (!request.hasVideoId() || isBlank(request.getVideoId().getValue())) {
            errorMessage.append("\t\tvideo id should be provided for get video rating request\n");
            isValid = false;
        }
        Assert.isTrue(validate(logger, streamObserver, errorMessage, isValid), "Invalid parameter for 'getRating'");
    }
    
    public void validateGrpcRequest_GetUserRating(Logger logger,GetUserRatingRequest request, StreamObserver<?> streamObserver) {
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
    
    /**
     * Publish comment to message bus. 
     * 
     * @param request
     * @param commentCreationDate
     */
    public void publishVideoRatedEvent(RateVideoRequest request, Instant commentCreationDate) {
        msgDao.publishEvent(UserRatedVideo.newBuilder()
                .setVideoId(request.getVideoId())
                .setRating(request.getRating())
                .setUserId(request.getUserId())
                .setRatingTimestamp(GrpcMapper.instantToTimeStamp(commentCreationDate))
                .build());
    }
    
   /**
     * Mapping to generated GPRC beans.
     */
    public GetRatingResponse maptoRatingResponse(VideoRating vr) {
        return GetRatingResponse.newBuilder()
                .setVideoId(GrpcMapper.uuidToUuid(vr.getVideoid()))
                .setRatingsCount(Optional.ofNullable(vr.getRatingCounter()).orElse(0L))
                .setRatingsTotal(Optional.ofNullable(vr.getRatingTotal()).orElse(0L))
                .build();
    }
    
    /**
     * Mapping to generated GPRC beans.
     */
    public GetUserRatingResponse maptoUserRatingResponse(VideoRatingByUser vr) {
        return GetUserRatingResponse.newBuilder()
                .setVideoId(GrpcMapper.uuidToUuid(vr.getVideoid()))
                .setUserId(GrpcMapper.uuidToUuid(vr.getUserid()))
                .setRating(vr.getRating())
                .build();
    }
    
}
