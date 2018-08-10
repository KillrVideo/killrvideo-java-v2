package com.killrvideo.grpc.utils;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import com.google.common.collect.Sets;
import com.killrvideo.dse.dao.dto.LatestVideosPage;
import com.killrvideo.dse.model.LatestVideo;
import com.killrvideo.dse.model.UserVideo;
import com.killrvideo.dse.model.Video;
import com.killrvideo.messaging.MessagingDao;

import io.grpc.stub.StreamObserver;
import killrvideo.common.CommonTypes;
import killrvideo.search.SearchServiceOuterClass.SearchResultsVideoPreview;
import killrvideo.suggested_videos.SuggestedVideosService.SuggestedVideoPreview;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetLatestVideoPreviewsRequest;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetLatestVideoPreviewsResponse;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetUserVideoPreviewsRequest;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetVideoPreviewsRequest;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetVideoRequest;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetVideoResponse;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.SubmitYouTubeVideoRequest;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.VideoLocationType;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.VideoPreview;

/**
 * Validation of inputs and mapping
 *
 * @author DataStax evangelist team.
 */
@Component
public class VideoCatalogGrpcHelper extends AbstractGrpcHelper {

    @Autowired
    private MessagingDao msgDao;
    
    /**
     * Validate arguments for 'SubmitYouTubeVideo'
     */
    public void validateGrpcRequest_submitYoutubeVideo(Logger logger, SubmitYouTubeVideoRequest request, StreamObserver<?> streamObserver) {
        final StringBuilder errorMessage = initErrorString(request);
        boolean isValid = true;
        if (request.getVideoId() == null || isBlank(request.getVideoId().getValue())) {
            errorMessage.append("\t\tvideo id should be provided for submit youtube video request\n");
            isValid = false;
        }
        if (request.getUserId() == null || isBlank(request.getUserId().getValue())) {
            errorMessage.append("\t\tuser id should be provided for submit youtube video request\n");
            isValid = false;
        }
        if (isBlank(request.getName())) {
            errorMessage.append("\t\tvideo name should be provided for submit youtube video request\n");
            isValid = false;
        }
        if (isBlank(request.getDescription())) {
            errorMessage.append("\t\tvideo description should be provided for submit youtube video request\n");
            isValid = false;
        }
        if (isBlank(request.getYouTubeVideoId())) {
            errorMessage.append("\t\tvideo youtube id should be provided for submit youtube video request\n");
            isValid = false;
        }
        Assert.isTrue(validate(logger, streamObserver, errorMessage, isValid), "Invalid parameter for 'submitVideo'");
    }
    
    /**
     * Validate arguments for 'getLatestVideoPreview'
     */
    public void validateGrpcRequest_getLatestPreviews(Logger logger, GetLatestVideoPreviewsRequest request, StreamObserver<?> streamObserver) {
        final StringBuilder errorMessage = initErrorString(request);
        boolean isValid = true;
        if (request.getPageSize() <= 0) {
            errorMessage.append("\t\tpage size should be strictly positive for get latest preview video request\n");
            isValid = false;
        }
        Assert.isTrue(validate(logger, streamObserver, errorMessage, isValid),  "Invalid parameter for 'getLatestVideoPreviews'");
    }
    
    public void  validateGrpcRequest_getVideo(Logger logger,  GetVideoRequest request, StreamObserver<?> streamObserver) {
        final StringBuilder errorMessage = initErrorString(request);
        boolean isValid = true;
        if (request.getVideoId() == null || isBlank(request.getVideoId().getValue())) {
            errorMessage.append("\t\tvideo id should be provided for submit youtube video request\n");
            isValid = false;
        }
        Assert.isTrue(validate(logger, streamObserver, errorMessage, isValid),  "Invalid parameter for 'getVideo'");
    }
    
    public void validateGrpcRequest_getVideoPreviews(Logger logger, GetVideoPreviewsRequest request, StreamObserver<?> streamObserver) {
        final StringBuilder errorMessage = initErrorString(request);
        boolean isValid = true;

        if (request.getVideoIdsCount() >= 20) {
            errorMessage.append("\t\tcannot get more than 20 videos at once for get video previews request\n");
            isValid = false;
        }
        for (CommonTypes.Uuid uuid : request.getVideoIdsList()) {
            if (uuid == null || isBlank(uuid.getValue())) {
                errorMessage.append("\t\tprovided UUID values cannot be null or blank for get video previews request\n");
                isValid = false;
            }
        }
        Assert.isTrue(validate(logger, streamObserver, errorMessage, isValid),  "Invalid parameter for 'getVideoPreview'");
    }
    
    public void validateGrpcRequest_getUserVideoPreviews(Logger logger, GetUserVideoPreviewsRequest request, StreamObserver<?> streamObserver) {
        final StringBuilder errorMessage = initErrorString(request);
        boolean isValid = true;

        if (request.getUserId() == null || isBlank(request.getUserId().getValue())) {
            errorMessage.append("\t\tuser id should be provided for get user video previews request\n");
            isValid = false;
        }

        if (request.getPageSize() <= 0) {
            errorMessage.append("\t\tpage size should be strictly positive for get user video previews request\n");
            isValid = false;
        }        
        Assert.isTrue(validate(logger, streamObserver, errorMessage, isValid),  "Invalid parameter for 'getUserVideoPreview'");
    }
    
    /**
     * eventbus.post() for youTubeVideoAdded below is located both in the
     * VideoAddedhandlers and SuggestedVideos Service classes within the handle() method.
     * The YouTubeVideoAdded type triggers the handler.  The call in SuggestedVideos is
     * responsible for adding data into our graph recommendation engine.
     */
    public void publishSubmitVideoSuccess(Video video) {
        msgDao.publishEvent(video);
    }
 
    public Video mapSubmitYouTubeVideoRequestAsVideo(SubmitYouTubeVideoRequest request) {
        Video targetVideo = new Video();
        targetVideo.setVideoid(UUID.fromString(request.getVideoId().getValue()));
        targetVideo.setUserid(UUID.fromString(request.getUserId().getValue()));
        targetVideo.setName(request.getName());
        targetVideo.setLocation(request.getYouTubeVideoId());
        targetVideo.setDescription(request.getDescription());
        targetVideo.setPreviewImageLocation("//img.youtube.com/vi/"+ targetVideo.getLocation() + "/hqdefault.jpg");
        targetVideo.setTags(Sets.newHashSet(request.getTagsList().iterator()));
        targetVideo.setLocationType(VideoLocationType.YOUTUBE.ordinal());
        return targetVideo;
    }
    
    /**
     * Mapping to GRPC generated classes.
     */
    public VideoPreview mapLatestVideotoVideoPreview(LatestVideo lv) {
        return VideoPreview.newBuilder()
                .setAddedDate(GrpcMapper.dateToTimestamp(lv.getAddedDate()))
                .setName(lv.getName())
                .setPreviewImageLocation(Optional.ofNullable(lv.getPreviewImageLocation()).orElse("N/A"))
                .setUserId(GrpcMapper.uuidToUuid(lv.getUserid()))
                .setVideoId(GrpcMapper.uuidToUuid(lv.getVideoid()))
                .build();
    }
    
    public GetLatestVideoPreviewsResponse mapLatestVideoToGrpcResponse(LatestVideosPage returnedPage) {
        return GetLatestVideoPreviewsResponse.newBuilder()
                .addAllVideoPreviews(
                        returnedPage.getListOfPreview().stream()
                        .map(this::mapLatestVideotoVideoPreview)
                        .collect(Collectors.toList()))
                .setPagingState(returnedPage.getNextPageState())
                .build();
    }
    
    /**
     * Mapping to generated GPRC beans.
     */
    public VideoPreview mapFromVideotoVideoPreview(Video v) {
        return VideoPreview.newBuilder()
                .setAddedDate(GrpcMapper.dateToTimestamp(v.getAddedDate()))
                .setName(v.getName())
                .setPreviewImageLocation(Optional.ofNullable(v.getPreviewImageLocation()).orElse("N/A"))
                .setUserId(GrpcMapper.uuidToUuid(v.getUserid()))
                .setVideoId(GrpcMapper.uuidToUuid(v.getVideoid()))
                .build();
    }
    
    /**
     * Mapping to generated GPRC beans.
     */
    public VideoPreview mapFromUserVideotoVideoPreview(UserVideo v) {
        return VideoPreview.newBuilder()
                .setAddedDate(GrpcMapper.dateToTimestamp(v.getAddedDate()))
                .setName(v.getName())
                .setPreviewImageLocation(Optional.ofNullable(v.getPreviewImageLocation()).orElse("N/A"))
                .setUserId(GrpcMapper.uuidToUuid(v.getUserid()))
                .setVideoId(GrpcMapper.uuidToUuid(v.getVideoid()))
                .build();
    }
    
    /**
     * Mapping to generated GPRC beans (Full detailed)
     */
    public GetVideoResponse mapFromVideotoVideoResponse(Video v) {
        return GetVideoResponse.newBuilder()
                .setAddedDate(GrpcMapper.dateToTimestamp(v.getAddedDate()))
                .setDescription(v.getDescription())
                .setLocation(v.getLocation())
                .setLocationType(VideoLocationType.forNumber(v.getLocationType()))
                .setName(v.getName())
                .setUserId(GrpcMapper.uuidToUuid(v.getUserid()))
                .setVideoId(GrpcMapper.uuidToUuid(v.getVideoid()))
                .addAllTags(v.getTags())
                .build();
    }

    /**
     * Mapping to generated GPRC beans (Search result special).
     */
    public SearchResultsVideoPreview mapFromVideotoResultVideoPreview(Video v) {
        return SearchResultsVideoPreview.newBuilder()
                .setAddedDate(GrpcMapper.dateToTimestamp(v.getAddedDate()))
                .setName(v.getName())
                .setPreviewImageLocation(Optional.ofNullable(v.getPreviewImageLocation()).orElse("N/A"))
                .setUserId(GrpcMapper.uuidToUuid(v.getUserid()))
                .setVideoId(GrpcMapper.uuidToUuid(v.getVideoid()))
                .build();
    }

    /**
     * Mapping to generated GPRC beans. (Suggested videos special)
     */
    public SuggestedVideoPreview mapFromVideotoSuggestedVideoPreview(Video v) {
        return SuggestedVideoPreview
                .newBuilder()
                .setVideoId(GrpcMapper.uuidToUuid(v.getVideoid()))
                .setAddedDate(GrpcMapper.dateToTimestamp(v.getAddedDate()))
                .setName(v.getName())
                .setPreviewImageLocation(Optional.ofNullable(v.getPreviewImageLocation()).orElse("N/A"))
                .setUserId(GrpcMapper.uuidToUuid(v.getUserid()))
                .build();
    }
}
