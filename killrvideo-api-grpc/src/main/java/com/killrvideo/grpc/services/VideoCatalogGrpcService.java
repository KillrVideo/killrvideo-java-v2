package com.killrvideo.grpc.services;

import static java.util.stream.Collectors.toList;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.killrvideo.core.error.ErrorEvent;
import com.killrvideo.dse.dao.VideoCatalogDseDao;
import com.killrvideo.dse.dao.dto.CustomPagingState;
import com.killrvideo.dse.dao.dto.LatestVideosPage;
import com.killrvideo.dse.model.Video;
import com.killrvideo.grpc.utils.GrpcMapper;
import com.killrvideo.grpc.utils.VideoCatalogGrpcHelper;
import com.killrvideo.messaging.MessagingDao;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import killrvideo.common.CommonTypes.Uuid;
import killrvideo.video_catalog.VideoCatalogServiceGrpc.VideoCatalogServiceImplBase;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetLatestVideoPreviewsRequest;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetLatestVideoPreviewsResponse;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetUserVideoPreviewsRequest;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetUserVideoPreviewsResponse;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetVideoPreviewsRequest;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetVideoPreviewsResponse;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetVideoRequest;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetVideoResponse;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.SubmitYouTubeVideoRequest;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.SubmitYouTubeVideoResponse;

/**
 * Exposition of comment services with GPRC Technology & Protobuf Interface
 * 
 * @author DataStax evangelist team.
 */
@Service
public class VideoCatalogGrpcService extends VideoCatalogServiceImplBase {

    /** Logger for this class. */
    private static Logger LOGGER = LoggerFactory.getLogger(VideoCatalogGrpcService.class);
    
    @Autowired
    private MessagingDao messagingDao;
    
    @Autowired
    private VideoCatalogDseDao videoCatalogDao;
    
    @Autowired
    private VideoCatalogGrpcHelper helper;

    /** {@inheritDoc} */
    @Override
    public void submitYouTubeVideo(SubmitYouTubeVideoRequest grpcReq, StreamObserver<SubmitYouTubeVideoResponse> grpcResObserver) {
        
        // GRPC Parameters Validation
        helper.validateGrpcRequest_submitYoutubeVideo(LOGGER, grpcReq, grpcResObserver);
        
        // Stands as stopwatch for logging and messaging 
        final Instant starts = Instant.now();
        
        // Mapping GRPC => Domain (Dao)
        Video video = helper.mapSubmitYouTubeVideoRequestAsVideo(grpcReq);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Insert youtube video for user {} : {}",  video.getVideoid(), video.getUserid(), video);
        }
        
        // Execute query (ASYNC)
        CompletableFuture<Void> future = videoCatalogDao.insertVideoAsync(video);
        
        // Building Response
        future.whenComplete((result, error) -> { 
            if (error != null ) {
                helper.traceError(LOGGER, "submitYouTubeVideo", starts, error);
                messagingDao.publishExceptionEvent(new ErrorEvent(grpcReq, error), error);
                grpcResObserver.onError(Status.INTERNAL.withCause(error).asRuntimeException());
            } else {
                helper.traceSuccess(LOGGER, "submitYouTubeVideo", starts);
                helper.publishSubmitVideoSuccess(video);
                grpcResObserver.onNext(SubmitYouTubeVideoResponse.newBuilder().build());
                grpcResObserver.onCompleted();
            }
        });
    }
   
    /**
     * Get latest video (Home Page)
     * 
     * In this method, we craft our own paging state. The custom paging state format is:
     * <br/>
     * <br/>
     * <code>
     * yyyyMMdd_yyyyMMdd_yyyyMMdd_yyyyMMdd_yyyyMMdd_yyyyMMdd_yyyyMMdd_yyyyMMdd,&lt;index&gt;,&lt;Cassandra paging state as string&gt;
     * </code>
     * <br/>
     * <br/>
     * <ul>
     *     <li>The first field is the date of 7 days in the past, starting from <strong>now</strong></li>
     *     <li>The second field is the index in this date list, to know at which day in the past we stop at the previous query</li>
     *     <li>The last field is the serialized form of the native Cassandra paging state</li>
     * </ul>
     *
     * On the first query, we create our own custom paging state in the server by computing the list of 8 days
     * in the past, the <strong>index</strong> is set to 0 and there is no native Cassandra paging state
     *
     * <br/>
     * On subsequent request, we decode the custom paging state coming from the web app and resume querying from
     * the appropriate date and we inject also the native Cassandra paging state.
     * <br/>
     * <strong>However, we can only use the native Cassandra paging state for the 1st query in the for loop. Indeed
     * Cassandra paging state is a hash of query string and bound values. We may switch partition to move one day
     * back in the past to fetch more results so the paging state will no longer be usable</strong>]
     */
    @Override
    public void getLatestVideoPreviews(GetLatestVideoPreviewsRequest grpcReq, StreamObserver<GetLatestVideoPreviewsResponse> grpcResObserver) {
        
        // GRPC Parameters Validation
        helper.validateGrpcRequest_getLatestPreviews(LOGGER, grpcReq, grpcResObserver);
        
        // Stands as stopwatch for logging and messaging 
        final Instant starts = Instant.now();
       
        // GRPC Parameters Mappings
        CustomPagingState pageState = 
                CustomPagingState.parse(Optional.ofNullable(grpcReq.getPagingState()))
                                 .orElse(videoCatalogDao.buildFirstCustomPagingState());
        final Optional<Date> startDate = Optional.ofNullable(grpcReq.getStartingAddedDate())
                .filter(x -> StringUtils.isNotBlank(x.toString()))
                .map(x -> Instant.ofEpochSecond(x.getSeconds(), x.getNanos()))
                .map(Date::from);
        final Optional<UUID> startVideoId = Optional.ofNullable(grpcReq.getStartingVideoId())
                .filter(x -> StringUtils.isNotBlank(x.toString()))
                .map(x -> x.getValue())
                .filter(StringUtils::isNotBlank)
                .map(UUID::fromString);
        int pageSize = grpcReq.getPageSize();
        
        try {
            // Queries against DSE day per day aysnchronously
            LatestVideosPage returnedPage = 
                    videoCatalogDao.getLatestVideoPreviews(pageState, pageSize, startDate, startVideoId);
            helper.traceSuccess(LOGGER, "getLatestVideoPreviews", starts);
            grpcResObserver.onNext(helper.mapLatestVideoToGrpcResponse(returnedPage));
            grpcResObserver.onCompleted();
        } catch(Exception error) {
            helper.traceError(LOGGER, "getLatestVideoPreviews", starts, error);
            messagingDao.publishExceptionEvent(new ErrorEvent(grpcReq, error), error);
            grpcResObserver.onError(Status.INTERNAL.withCause(error).asRuntimeException());
        }  
        LOGGER.debug("End getting latest video preview");
    }
    
    /** {@inheritDoc} */
    @Override
    public void getVideo(GetVideoRequest grpcReq, StreamObserver<GetVideoResponse> grpcResObserver) {
        
        // GRPC Parameters Validation
        helper.validateGrpcRequest_getVideo(LOGGER, grpcReq, grpcResObserver);
        
        // Stands as stopwatch for logging and messaging 
        final Instant starts = Instant.now();
       
        // GRPC Parameters Mappings
        final UUID videoId = UUID.fromString(grpcReq.getVideoId().getValue());

        // Invoke Async
        CompletableFuture<Video> futureVideo = videoCatalogDao.getVideoById(videoId);
        
        // Map back as GRPC (if correct invalid credential otherwize)
        futureVideo.whenComplete((video, error) -> {
            if (error != null ) {
                helper.traceError(LOGGER, "getVideo", starts, error);
                messagingDao.publishExceptionEvent(new ErrorEvent(grpcReq, error), error);
                
            } else {
                if (video != null) {
                    // Check to see if any tags exist, if not, ensure to send an empty set instead of null
                    if (CollectionUtils.isEmpty(video.getTags())) {
                        video.setTags(Collections.emptySet());
                    }
                    helper.traceSuccess(LOGGER, "getVideo", starts);
                    grpcResObserver.onNext(helper.mapFromVideotoVideoResponse(video));
                    grpcResObserver.onCompleted();
                } else {
                    LOGGER.warn("Video with id " + videoId + " was not found");
                    helper.traceError(LOGGER, "getVideo", starts, error);
                    grpcResObserver.onError(Status.NOT_FOUND.withDescription("Video with id " + videoId + " was not found").asRuntimeException());
                }
            }
        });
    }
    
    /** {@inheritDoc} */
    @Override
    public void getVideoPreviews(GetVideoPreviewsRequest grpcReq, StreamObserver<GetVideoPreviewsResponse> grpcResObserver) {
        
        // GRPC Parameters Validation
        helper.validateGrpcRequest_getVideoPreviews(LOGGER, grpcReq, grpcResObserver);
        
        // Stands as stopwatch for logging and messaging 
        final Instant starts = Instant.now();
       
        final GetVideoPreviewsResponse.Builder builder = GetVideoPreviewsResponse.newBuilder();
        if (grpcReq.getVideoIdsCount() == 0 || CollectionUtils.isEmpty(grpcReq.getVideoIdsList())) {
            helper.traceSuccess(LOGGER, "getVideoPreviews", starts);
            grpcResObserver.onNext(builder.build());
            grpcResObserver.onCompleted();
            LOGGER.warn("No video id provided for video preview");
        } else {
            
            // GRPC Parameters Mappings
            List <UUID> listOfVideoIds = grpcReq.getVideoIdsList().stream().map(Uuid::getValue).map(UUID::fromString).collect(toList());
            
            // Execute Async
            CompletableFuture<List<Video>> futureVideoList = videoCatalogDao.getVideoPreview(listOfVideoIds);
            
            // Mapping back as GRPC
            futureVideoList.whenComplete((videos, error) -> {
                if (error != null ) {
                    helper.traceError(LOGGER, "getVideoPreviews", starts, error);
                    messagingDao.publishExceptionEvent(new ErrorEvent(grpcReq,  error), error);
                    grpcResObserver.onError(Status.INTERNAL.withCause(error).asRuntimeException());
                    
                } else {
                    helper.traceSuccess(LOGGER, "getVideoPreviews", starts);
                    videos.stream().map(helper::mapFromVideotoVideoPreview).forEach(builder::addVideoPreviews);
                    grpcResObserver.onNext(builder.build());
                    grpcResObserver.onCompleted();
                }
            }); 
        }
    }
    
    /** {@inheritDoc} */
    @Override
    public void getUserVideoPreviews(GetUserVideoPreviewsRequest grpcReq, StreamObserver<GetUserVideoPreviewsResponse> grpcResObserver) {
        
        // GRPC Parameters Validation
        helper.validateGrpcRequest_getUserVideoPreviews(LOGGER, grpcReq, grpcResObserver);
        
        // Stands as stopwatch for logging and messaging 
        final Instant starts = Instant.now();
       
        // GRPC Parameters Mappings
        final UUID userId = UUID.fromString(grpcReq.getUserId().getValue());
        final Optional<UUID> startingVideoId = Optional
                .ofNullable(grpcReq.getStartingVideoId())
                .map(Uuid::getValue)
                .filter(StringUtils::isNotBlank)
                .map(UUID::fromString);
        final Optional<Date> startingAddedDate = Optional
                .ofNullable(grpcReq.getStartingAddedDate())
                .map(ts -> Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos()))
                .map(Date::from);
        final Optional<String> pagingState = 
                Optional.ofNullable(grpcReq.getPagingState()).filter(StringUtils::isNotBlank);
        final Optional<Integer> pagingSize =
                Optional.ofNullable(grpcReq.getPageSize());
        
        
        // Map Result back to GRPC
        videoCatalogDao
            .getUserVideosPreview(userId, startingVideoId, startingAddedDate, pagingSize, pagingState)
            .whenComplete((resultPage, error) -> {
            
            if (error != null ) {
                helper.traceError(LOGGER, "getUserVideoPreviews", starts, error);
                messagingDao.publishExceptionEvent(new ErrorEvent(grpcReq,  error), error);
                grpcResObserver.onError(Status.INTERNAL.withCause(error).asRuntimeException());
                
            } else {
                
                helper.traceSuccess(LOGGER, "getUserVideoPreviews", starts);
                Uuid userGrpcUUID = GrpcMapper.uuidToUuid(userId);
                final GetUserVideoPreviewsResponse.Builder builder = GetUserVideoPreviewsResponse.newBuilder().setUserId(userGrpcUUID);
                resultPage.getResults().stream()
                      .map(helper::mapFromUserVideotoVideoPreview)
                      .forEach(builder::addVideoPreviews);
                resultPage.getPagingState().ifPresent(builder::setPagingState);
                grpcResObserver.onNext(builder.build());
                grpcResObserver.onCompleted();
            }
        });
        
    }
}
