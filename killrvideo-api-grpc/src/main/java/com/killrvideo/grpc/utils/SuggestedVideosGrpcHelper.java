package com.killrvideo.grpc.utils;

import static org.apache.commons.lang3.StringUtils.isBlank;

import org.slf4j.Logger;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import com.killrvideo.dse.model.Video;

import io.grpc.stub.StreamObserver;
import killrvideo.suggested_videos.SuggestedVideosService.GetRelatedVideosRequest;
import killrvideo.suggested_videos.SuggestedVideosService.GetSuggestedForUserRequest;
import killrvideo.suggested_videos.SuggestedVideosService.SuggestedVideoPreview;

/**
 * Helper and mappers for DAO <=> GRPC Communications
 *
 * @author DataStax Evangelist Team
 */
@Component
public class SuggestedVideosGrpcHelper extends AbstractGrpcHelper {
    
    public void validateGrpcRequest_getRelatedVideo(Logger logger, GetRelatedVideosRequest request, StreamObserver<?> streamObserver) {
        final StringBuilder errorMessage = initErrorString(request);
        boolean isValid = true;

        if (request.getVideoId() == null || isBlank(request.getVideoId().getValue())) {
            errorMessage.append("\t\tvideo id should be provided for get related videos request\n");
            isValid = false;
        }
        Assert.isTrue(validate(logger, streamObserver, errorMessage, isValid), "Invalid parameter for 'getRelatedVideo'");
    }
    
    public void validateGrpcRequest_getUserSuggestedVideo(Logger logger, GetSuggestedForUserRequest request, StreamObserver<?> streamObserver) {
        final StringBuilder errorMessage = initErrorString(request);
        boolean isValid = true;
        if (request.getUserId() == null || isBlank(request.getUserId().getValue())) {
            errorMessage.append("\t\tuser id should be provided for get suggested for user request\n");
            isValid = false;
        }
        Assert.isTrue(validate(logger, streamObserver, errorMessage, isValid), "Invalid parameter for 'getSuggestedForUser'");
    }

    /**
     * Mapping to generated GPRC beans. (Suggested videos special)
     */
    public SuggestedVideoPreview mapVideotoSuggestedVideoPreview(Video v) {
        return SuggestedVideoPreview.newBuilder()
                .setName(v.getName())
                .setVideoId(GrpcMapper.uuidToUuid(v.getVideoid()))
                .setUserId(GrpcMapper.uuidToUuid(v.getUserid()))
                .setPreviewImageLocation(v.getPreviewImageLocation())
                .setAddedDate(GrpcMapper.epochTimeToTimeStamp(v.getAddedDate().getTime()))
                .build();
    }
    
    
}
