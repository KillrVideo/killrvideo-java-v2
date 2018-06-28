package com.killrvideo.grpc.services;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.killrvideo.core.error.ErrorEvent;
import com.killrvideo.dse.dao.StatisticsDseDao;
import com.killrvideo.dse.model.VideoPlaybackStats;
import com.killrvideo.grpc.utils.StatisticsGrpcHelper;
import com.killrvideo.messaging.MessagingDao;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import killrvideo.common.CommonTypes.Uuid;
import killrvideo.statistics.StatisticsServiceGrpc.StatisticsServiceImplBase;
import killrvideo.statistics.StatisticsServiceOuterClass.GetNumberOfPlaysRequest;
import killrvideo.statistics.StatisticsServiceOuterClass.GetNumberOfPlaysResponse;
import killrvideo.statistics.StatisticsServiceOuterClass.RecordPlaybackStartedRequest;
import killrvideo.statistics.StatisticsServiceOuterClass.RecordPlaybackStartedResponse;

/**
 * Get statistics on a video.
 *
 * @author DataStax Evangelist Team
 */
@Service
public class StatisticsGrpcService extends StatisticsServiceImplBase {

    /** Loger for that class. */
    private static Logger LOGGER = LoggerFactory.getLogger(StatisticsGrpcService.class);
    
    @Autowired
    private StatisticsGrpcHelper helper;
    
    @Autowired
    private MessagingDao messagingDao;
    
    @Autowired
    private StatisticsDseDao statisticsDseDao;
    
    /** {@inheritDoc} */
    @Override
    public void recordPlaybackStarted(RecordPlaybackStartedRequest grpcReq, StreamObserver<RecordPlaybackStartedResponse> grpcResObserver) {
        
        // Validate Parameters
        helper.validateGrpcRequest_RecordPlayback(LOGGER, grpcReq, grpcResObserver);
        
        // Stands as stopwatch for logging and messaging 
        final Instant starts = Instant.now();
        
        // Mapping GRPC => Domain (Dao)
        final UUID videoId = UUID.fromString(grpcReq.getVideoId().getValue());
        
        // Invoke DAO Async
        CompletableFuture<Void> futureDao = statisticsDseDao.recordPlaybackStartedAsync(videoId);
        
        // Map Result back to GRPC
        futureDao.whenComplete((result, error) -> {
            if (error != null ) {
                helper.traceError(LOGGER, "recordPlaybackStarted", starts, error);
                messagingDao.publishExceptionEvent(new ErrorEvent(grpcReq, error), error);
                grpcResObserver.onError(Status.INTERNAL.withCause(error).asRuntimeException());
            } else {
                helper.traceSuccess(LOGGER, "recordPlaybackStarted", starts);
                grpcResObserver.onNext(RecordPlaybackStartedResponse.newBuilder().build());
                grpcResObserver.onCompleted();
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public void getNumberOfPlays(GetNumberOfPlaysRequest grpcReq, StreamObserver<GetNumberOfPlaysResponse> grpcResObserver) {
        
        // Validate Parameters
        helper.validateGrpcRequest_GetNumberPlays(LOGGER, grpcReq, grpcResObserver);
        
        // Stands as stopwatch for logging and messaging 
        final Instant starts = Instant.now();
        
        // Mapping GRPC => Domain (Dao)
        List <UUID> listOfVideoId = grpcReq.getVideoIdsList()
                                           .stream()
                                           .map(Uuid::getValue)
                                           .map(UUID::fromString)
                                           .collect(Collectors.toList());
        
        // Invoke DAO Async
        CompletableFuture<List<VideoPlaybackStats>> futureDao = 
                statisticsDseDao.getNumberOfPlaysAsync(listOfVideoId);
        
        // Map Result back to GRPC
        futureDao.whenComplete((videoList, error) -> {
            if (error != null ) {
                 
                helper.traceError(LOGGER, "getNumberOfPlays", starts, error);
                messagingDao.publishExceptionEvent(new ErrorEvent(grpcReq, error), error);
                grpcResObserver.onError(Status.INTERNAL.withCause(error).asRuntimeException());
            } else {
                
                helper.traceSuccess(LOGGER, "getNumberOfPlays", starts);
                grpcResObserver.onNext(helper.buildGetNumberOfPlayResponse(grpcReq, videoList));
                grpcResObserver.onCompleted();
            }
        });
    }
    
}
