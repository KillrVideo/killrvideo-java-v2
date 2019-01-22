package com.killrvideo;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.killrvideo.conf.KillrVideoConfiguration;
import com.killrvideo.etcd.EtcdDao;
import com.killrvideo.service.comment.grpc.CommentsServiceGrpc;
import com.killrvideo.service.rating.grpc.RatingsServiceGrpc;
import com.killrvideo.service.search.grpc.SearchServiceGrpc;
import com.killrvideo.service.statistic.grpc.StatisticsServiceGrpc;
import com.killrvideo.service.sugestedvideo.grpc.SuggestedVideosServiceGrpc;
import com.killrvideo.service.user.grpc.UserManagementServiceGrpc;
import com.killrvideo.service.video.grpc.VideoCatalogServiceGrpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerServiceDefinition;

/**
 * Startup a GRPC server on expected port and register all services.
 *
 * @author DataStax advocates team.
 */
@Component
public class KillrvideoServicesGrpcServer {

    /** Some logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(KillrvideoServicesGrpcServer.class);
    
    /** Listening Port for GRPC. */
    @Value("${grpc.port: 8899}")
    private int grpcPort;
    
    /** Connectivity to ETCD Service discovery. */
    @Autowired
    private KillrVideoConfiguration config;
    
    /** Connectivity to ETCD Service discovery. */
    @Autowired
    private EtcdDao serviceDiscoveryDao;
    
    // ==== SERVICES ====
    
    @Autowired
    private CommentsServiceGrpc commentService;
    
    @Autowired
    private RatingsServiceGrpc ratingService;
    
    @Autowired
    private SearchServiceGrpc searchService;
    
    @Autowired
    private StatisticsServiceGrpc statisticsService;
    
    @Autowired
    private VideoCatalogServiceGrpc videoCatalogService;
 
    @Autowired
    private UserManagementServiceGrpc userService;
    
    @Autowired
    private SuggestedVideosServiceGrpc suggestedVideosGrpcService;
  
    /**
     * GRPC Server to set up.
     */
    private Server server;
    
    @PostConstruct
    public void start() throws Exception {
        LOGGER.info("Initializing Grpc Server...");
        
        // Binding Services
        final ServerServiceDefinition commentService             = this.commentService.bindService();
        final ServerServiceDefinition ratingService              = this.ratingService.bindService();
        final ServerServiceDefinition searchService              = this.searchService.bindService();
        final ServerServiceDefinition statisticsService          = this.statisticsService.bindService();
        final ServerServiceDefinition suggestedVideosGrpcService = this.suggestedVideosGrpcService.bindService();
        final ServerServiceDefinition videoCatalogService        = this.videoCatalogService.bindService();
        final ServerServiceDefinition userService                = this.userService.bindService();
        
        // Reference Service in Server
        server = ServerBuilder.forPort(grpcPort)
                   .addService(commentService)
                   .addService(ratingService)
                   .addService(searchService)
                   .addService(statisticsService)
                   .addService(suggestedVideosGrpcService)
                   .addService(videoCatalogService)
                   .addService(userService)
                   .build();
            /**
             * Declare a shutdown hook otherwise the JVM
             * cannot be stop since the Grpc server
             * is listening on  a port forever
             */
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    LOGGER.info("Calling shutdown for GrpcServer");
                    server.shutdown();
                }
            });

        // Start Grpc listener
        server.start();
        LOGGER.info("[OK] Grpc Server started on port: '{}'", grpcPort);
        
        // Registering Services to Service Discovery (ETCD if needed, do nothing with Kubernetes
        serviceDiscoveryDao.registerServiceEndpoint(
                CommentsServiceGrpc.COMMENTS_SERVICE_NAME, config.getApplicationHost(), grpcPort);
        serviceDiscoveryDao.registerServiceEndpoint(
                RatingsServiceGrpc.RATINGS_SERVICE_NAME, config.getApplicationHost(), grpcPort);
        serviceDiscoveryDao.registerServiceEndpoint(
                SearchServiceGrpc.SEARCH_SERVICE_NAME, config.getApplicationHost(), grpcPort);
        serviceDiscoveryDao.registerServiceEndpoint(
                StatisticsServiceGrpc.STATISTICS_SERVICE_NAME, config.getApplicationHost(), grpcPort);
        serviceDiscoveryDao.registerServiceEndpoint(
                SuggestedVideosServiceGrpc.SUGESTEDVIDEOS_SERVICE_NAME, config.getApplicationHost(), grpcPort);
        serviceDiscoveryDao.registerServiceEndpoint(
               VideoCatalogServiceGrpc.VIDEOCATALOG_SERVICE_NAME, config.getApplicationHost(), grpcPort);
        serviceDiscoveryDao.registerServiceEndpoint(
                UserManagementServiceGrpc.USER_SERVICE_NAME, config.getApplicationHost(), grpcPort);
    }

    @PreDestroy
    public void stop() {
        server.shutdown();
    }
    
}
