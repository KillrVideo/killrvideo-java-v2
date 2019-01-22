package com.killrvideo;

import org.springframework.util.Assert;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import killrvideo.comments.CommentsServiceGrpc;
import killrvideo.comments.CommentsServiceGrpc.CommentsServiceBlockingStub;
import killrvideo.ratings.RatingsServiceGrpc;
import killrvideo.ratings.RatingsServiceGrpc.RatingsServiceBlockingStub;
import killrvideo.search.SearchServiceGrpc;
import killrvideo.search.SearchServiceGrpc.SearchServiceBlockingStub;
import killrvideo.statistics.StatisticsServiceGrpc;
import killrvideo.statistics.StatisticsServiceGrpc.StatisticsServiceBlockingStub;
import killrvideo.suggested_videos.SuggestedVideoServiceGrpc;
import killrvideo.suggested_videos.SuggestedVideoServiceGrpc.SuggestedVideoServiceBlockingStub;
import killrvideo.user_management.UserManagementServiceGrpc;
import killrvideo.user_management.UserManagementServiceGrpc.UserManagementServiceBlockingStub;
import killrvideo.video_catalog.VideoCatalogServiceGrpc;
import killrvideo.video_catalog.VideoCatalogServiceGrpc.VideoCatalogServiceBlockingStub;

/**
 * As Unit Test or cany consumer you may want USE the runing GRPC API.
 *
 * @author DataStax advocates Team
 */
public class KillrvideoServicesGrpcClient {
    
    /** Grpc Endpoint */
    private ManagedChannel grpcEndPoint;
   
    /** Clients for different services in GRPC. */
    public CommentsServiceBlockingStub         commentServiceGrpcClient;
    public RatingsServiceBlockingStub          ratingServiceGrpcClient;
    public SearchServiceBlockingStub           searchServiceGrpcClient;
    public StatisticsServiceBlockingStub       statisticServiceGrpcClient;
    public SuggestedVideoServiceBlockingStub   suggestedVideoServiceGrpcClient;
    public UserManagementServiceBlockingStub   userServiceGrpcClient;
    public VideoCatalogServiceBlockingStub     videoCatalogServiceGrpcClient;
    
    /**
     * Connection to GRPC Server.
     * 
     * @param grpcServer
     *      current grpc hostname
     * @param grpcPort
     *      current grpc portnumber
     */
    public KillrvideoServicesGrpcClient(String grpcServer, int grpcPort) {
       this(ManagedChannelBuilder.forAddress(grpcServer, grpcPort).usePlaintext(true).build());
    }
    
    /**
     * Extension point for your own GRPC channel.
     * 
     * @param grpcEnpoint
     *      current GRPC Channe
     */
    public KillrvideoServicesGrpcClient(ManagedChannel grpcEnpoint) {
        this.grpcEndPoint = grpcEnpoint;
        initServiceClients();
    }
    
    /**
     * Init item
     */
    public void initServiceClients() {
        Assert.notNull(grpcEndPoint, "GrpcEnpoint must be setup");
        commentServiceGrpcClient         = CommentsServiceGrpc.newBlockingStub(grpcEndPoint);
        ratingServiceGrpcClient          = RatingsServiceGrpc.newBlockingStub(grpcEndPoint);
        searchServiceGrpcClient          = SearchServiceGrpc.newBlockingStub(grpcEndPoint);
        statisticServiceGrpcClient       = StatisticsServiceGrpc.newBlockingStub(grpcEndPoint);
        suggestedVideoServiceGrpcClient  = SuggestedVideoServiceGrpc.newBlockingStub(grpcEndPoint);
        userServiceGrpcClient            = UserManagementServiceGrpc.newBlockingStub(grpcEndPoint);
        videoCatalogServiceGrpcClient    = VideoCatalogServiceGrpc.newBlockingStub(grpcEndPoint);
    }

    /**
     * Getter accessor for attribute 'commentServiceGrpcClient'.
     *
     * @return
     *       current value of 'commentServiceGrpcClient'
     */
    public CommentsServiceBlockingStub getCommentService() {
        return commentServiceGrpcClient;
    }
    
    /**
     * Getter accessor for attribute 'ratingServiceGrpcClient'.
     *
     * @return
     *       current value of 'ratingServiceGrpcClient'
     */
    public RatingsServiceBlockingStub getRatingService() {
        return ratingServiceGrpcClient;
    }

    /**
     * Getter accessor for attribute 'searchServiceGrpcClient'.
     *
     * @return
     *       current value of 'searchServiceGrpcClient'
     */
    public SearchServiceBlockingStub getSearchService() {
        return searchServiceGrpcClient;
    }

    /**
     * Getter accessor for attribute 'statisticServiceGrpcClient'.
     *
     * @return
     *       current value of 'statisticServiceGrpcClient'
     */
    public StatisticsServiceBlockingStub getStatisticService() {
        return statisticServiceGrpcClient;
    }

    /**
     * Getter accessor for attribute 'suggestedVideoServiceGrpcClient'.
     *
     * @return
     *       current value of 'suggestedVideoServiceGrpcClient'
     */
    public SuggestedVideoServiceBlockingStub getSuggestedVideoService() {
        return suggestedVideoServiceGrpcClient;
    }
    
    /**
     * Getter accessor for attribute 'userServiceGrpcClient'.
     *
     * @return
     *       current value of 'userServiceGrpcClient'
     */
    public UserManagementServiceBlockingStub getUserService() {
        return userServiceGrpcClient;
    }

    /**
     * Getter accessor for attribute 'videoCatalogServiceGrpcClient'.
     *
     * @return
     *       current value of 'videoCatalogServiceGrpcClient'
     */
    public VideoCatalogServiceBlockingStub getVideoCatalogService() {
        return videoCatalogServiceGrpcClient;
    }
}
