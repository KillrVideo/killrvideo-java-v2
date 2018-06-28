package com.killrvideo.grpc.services;

import java.time.Instant;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.killrvideo.core.error.ErrorEvent;
import com.killrvideo.dse.dao.RatingDseDao;
import com.killrvideo.grpc.utils.RatingsGrpcHelper;
import com.killrvideo.messaging.MessagingDao;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import killrvideo.ratings.RatingsServiceGrpc.RatingsServiceImplBase;
import killrvideo.ratings.RatingsServiceOuterClass.GetRatingRequest;
import killrvideo.ratings.RatingsServiceOuterClass.GetRatingResponse;
import killrvideo.ratings.RatingsServiceOuterClass.GetUserRatingRequest;
import killrvideo.ratings.RatingsServiceOuterClass.GetUserRatingResponse;
import killrvideo.ratings.RatingsServiceOuterClass.RateVideoRequest;
import killrvideo.ratings.RatingsServiceOuterClass.RateVideoResponse;

/**
 * Operations on Ratings with GRPC.
 *
 * @author DataStax Evangelist Team
 */
@Service
public class RatingsGrpcService extends RatingsServiceImplBase {
    
    /** Loger for that class. */
    private static Logger LOGGER = LoggerFactory.getLogger(RatingsGrpcService.class);
 
    @Autowired
    private RatingDseDao dseRatingDao;
    
    @Autowired
    private MessagingDao messagingDao;
    
    @Autowired
    private RatingsGrpcHelper helper;
    
    /** {@inheritDoc} */
    @Override
    public void rateVideo(final RateVideoRequest grpcReq, final StreamObserver<RateVideoResponse> grpcResObserver) {
        
        // Validate Parameters
        helper.validateGrpcRequest_RateVideo(LOGGER, grpcReq, grpcResObserver);
        
        // Stands as stopwatch for logging and messaging 
        final Instant starts = Instant.now();
        
        // Mapping GRPC => Domain (Dao)
        UUID videoid = UUID.fromString(grpcReq.getVideoId().getValue());
        UUID userid  = UUID.fromString(grpcReq.getUserId().getValue());
        Integer rate = grpcReq.getRating();
        
        // Invoking Dao (Async), publish event if successful
        dseRatingDao.rateVideo(videoid, userid, rate).whenComplete((result, error) -> {
            if (error == null) {
                helper.traceSuccess(LOGGER,"rateVideo", starts);
                helper.publishVideoRatedEvent(grpcReq, starts);
                grpcResObserver.onNext(RateVideoResponse.newBuilder().build());
                grpcResObserver.onCompleted();
            } else {
                helper.traceError(LOGGER, "rateVideo", starts, error);
                messagingDao.publishExceptionEvent(new ErrorEvent(grpcReq, error), error);
                grpcResObserver.onError(Status.INTERNAL.withCause(error).asRuntimeException());
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public void getRating(GetRatingRequest grpcReq, StreamObserver<GetRatingResponse> grpcResObserver) {
        
        // Validate Parameters
        helper.validateGrpcRequest_GetRating(LOGGER, grpcReq, grpcResObserver);
        
        // Stands as stopwatch for logging and messaging 
        final Instant starts = Instant.now();
        
        // Mapping GRPC => Domain (Dao)
        UUID videoid = UUID.fromString(grpcReq.getVideoId().getValue());
        
        // Invoking Dao (Async) and map result back to GRPC (maptoRatingResponse)
        dseRatingDao.findRating(videoid).whenComplete((videoRating, error) -> {
            if (error == null) {
                helper.traceSuccess(LOGGER, "getRating", starts);
                if (videoRating.isPresent()) {
                    grpcResObserver.onNext(helper.maptoRatingResponse(videoRating.get()));
                } else {
                    grpcResObserver.onNext(GetRatingResponse.newBuilder()
                            .setVideoId(grpcReq.getVideoId())
                            .setRatingsCount(0L)
                            .setRatingsTotal(0L)
                            .build());
                }
                grpcResObserver.onCompleted();
            } else {
                helper.traceError(LOGGER, "getRating", starts, error);
                messagingDao.publishExceptionEvent(new ErrorEvent(grpcReq, error), error);
                grpcResObserver.onError(Status.INTERNAL.withCause(error).asRuntimeException());
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public void getUserRating(GetUserRatingRequest grpcReq, StreamObserver<GetUserRatingResponse> grpcResObserver) {
        
        // Validate Parameters
        helper.validateGrpcRequest_GetUserRating(LOGGER, grpcReq, grpcResObserver);
        
        // Stands as stopwatch for logging and messaging 
        final Instant starts = Instant.now();
        
        // Mapping GRPC => Domain (Dao)
        UUID videoid = UUID.fromString(grpcReq.getVideoId().getValue());
        UUID userid  = UUID.fromString(grpcReq.getUserId().getValue());
        
        // Invoking Dao (Async) and map result back to GRPC (maptoRatingResponse)
        dseRatingDao.findUserRating(videoid,userid).whenComplete((videoRating, error) -> {
            if (error == null) {
                helper.traceSuccess(LOGGER, "getUserRating" , starts);
                if (videoRating.isPresent()) {
                    grpcResObserver.onNext(helper.maptoUserRatingResponse(videoRating.get()));
                } else {
                    grpcResObserver.onNext(GetUserRatingResponse.newBuilder()
                            .setUserId(grpcReq.getUserId())
                            .setVideoId(grpcReq.getVideoId())
                            .setRating(0)
                            .build());
                }
                grpcResObserver.onCompleted();
            } else {
                helper.traceError(LOGGER, "getUserRating", starts, error);
                messagingDao.publishExceptionEvent(new ErrorEvent(grpcReq, error), error);
                grpcResObserver.onError(Status.INTERNAL.withCause(error).asRuntimeException());
            }
        });
    }

}
