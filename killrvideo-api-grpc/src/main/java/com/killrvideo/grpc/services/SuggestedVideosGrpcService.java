package com.killrvideo.grpc.services;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.killrvideo.core.error.ErrorEvent;
import com.killrvideo.dse.dao.SuggestedVideosDseDao;
import com.killrvideo.dse.dao.dto.ResultListPage;
import com.killrvideo.dse.model.Video;
import com.killrvideo.grpc.utils.GrpcMapper;
import com.killrvideo.grpc.utils.SuggestedVideosGrpcHelper;
import com.killrvideo.messaging.MessagingDao;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import killrvideo.common.CommonTypes.Uuid;
import killrvideo.suggested_videos.SuggestedVideoServiceGrpc.SuggestedVideoServiceImplBase;
import killrvideo.suggested_videos.SuggestedVideosService.GetRelatedVideosRequest;
import killrvideo.suggested_videos.SuggestedVideosService.GetRelatedVideosResponse;
import killrvideo.suggested_videos.SuggestedVideosService.GetSuggestedForUserRequest;
import killrvideo.suggested_videos.SuggestedVideosService.GetSuggestedForUserResponse;

/**
 * Suggested video for a user.
 *
 * @author DataStax Evangelist Team
 */
@Service
public class SuggestedVideosGrpcService extends SuggestedVideoServiceImplBase {
    
    /** Loger for that class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SuggestedVideosGrpcService.class);
    
    @Autowired
    private SuggestedVideosGrpcHelper helper;
    
    @Autowired
    private MessagingDao messagingDao;
    
    @Autowired
    private SuggestedVideosDseDao suggestedVideosDseDao;
    
    /** {@inheritDoc} */
    @Override
    public void getRelatedVideos(GetRelatedVideosRequest grpcReq, StreamObserver<GetRelatedVideosResponse> grpcResObserver) {
        
        // Validate Parameters
        helper.validateGrpcRequest_getRelatedVideo(LOGGER, grpcReq, grpcResObserver);
        
        // Stands as stopwatch for logging and messaging 
        final Instant starts = Instant.now();
        
        // Mapping GRPC => Domain (Dao)
        final UUID       videoId = UUID.fromString(grpcReq.getVideoId().getValue());
        int              videoPageSize = grpcReq.getPageSize();
        Optional<String> videoPagingState = Optional.ofNullable(grpcReq.getPagingState()).filter(StringUtils::isNotBlank);
        
        // Invoke DAO Async
        CompletableFuture<ResultListPage<Video>> futureDao = 
                suggestedVideosDseDao.getRelatedVideos(videoId, videoPageSize, videoPagingState);
        
        // Map Result back to GRPC
        futureDao.whenComplete((resultPage, error) -> {
            
            if (error != null ) {
                helper.traceError(LOGGER, "getRelatedVideos", starts, error);
                messagingDao.publishExceptionEvent(new ErrorEvent(grpcReq,  error), error);
                grpcResObserver.onError(Status.INTERNAL.withCause(error).asRuntimeException());
                
            } else {
                
                helper.traceSuccess(LOGGER, "getRelatedVideos", starts);
                Uuid videoGrpcUUID = GrpcMapper.uuidToUuid(videoId);
                final GetRelatedVideosResponse.Builder builder = 
                        GetRelatedVideosResponse.newBuilder().setVideoId(videoGrpcUUID);
                resultPage.getResults().stream()
                      .map(helper::mapVideotoSuggestedVideoPreview)
                      .filter(preview -> !preview.getVideoId().equals(videoGrpcUUID))
                      .forEach(builder::addVideos);
                resultPage.getPagingState().ifPresent(builder::setPagingState);
                grpcResObserver.onNext(builder.build());
                grpcResObserver.onCompleted();
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public void getSuggestedForUser(GetSuggestedForUserRequest grpcReq, StreamObserver<GetSuggestedForUserResponse> grpcResObserver) {
        
        // Validate Parameters
        helper.validateGrpcRequest_getUserSuggestedVideo(LOGGER, grpcReq, grpcResObserver);
        
        // Stands as stopwatch for logging and messaging 
        final Instant starts = Instant.now();
        
        // Mapping GRPC => Domain (Dao)
        final UUID userid = UUID.fromString(grpcReq.getUserId().getValue());
        
        // Invoke DAO Async
        CompletableFuture<List<Video>> futureDao = suggestedVideosDseDao.getSuggestedVideosForUser(userid);
        
        // Map Result back to GRPC
        futureDao.whenComplete((videos, error) -> {
            
            if (error != null ) {
                helper.traceError(LOGGER, "getSuggestedForUser", starts, error);
                messagingDao.publishExceptionEvent(new ErrorEvent(grpcReq,  error), error);
                grpcResObserver.onError(Status.INTERNAL.withCause(error).asRuntimeException());
                
            } else {
                
                helper.traceSuccess(LOGGER, "getSuggestedForUser", starts);
                Uuid userGrpcUUID = GrpcMapper.uuidToUuid(userid);
                final GetSuggestedForUserResponse.Builder builder = GetSuggestedForUserResponse.newBuilder().setUserId(userGrpcUUID);
                videos.stream().map(helper::mapVideotoSuggestedVideoPreview).forEach(builder::addVideos);
                grpcResObserver.onNext(builder.build());
                grpcResObserver.onCompleted();
            }
        });
    }
}
