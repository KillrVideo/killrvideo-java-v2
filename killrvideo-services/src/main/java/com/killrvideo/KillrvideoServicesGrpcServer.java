package com.killrvideo;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.killrvideo.conf.KillrVideoConfiguration;
import com.killrvideo.discovery.ServiceDiscoveryDaoEtcd;
import com.killrvideo.service.comment.grpc.CommentsServiceGrpc;
import com.killrvideo.service.rating.grpc.RatingsServiceGrpc;
import com.killrvideo.service.search.grpc.SearchServiceGrpc;
import com.killrvideo.service.statistic.grpc.StatisticsServiceGrpc;
import com.killrvideo.service.sugestedvideo.grpc.SuggestedVideosServiceGrpc;
import com.killrvideo.service.user.grpc.UserManagementServiceGrpc;
import com.killrvideo.service.video.grpc.VideoCatalogServiceGrpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;

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
    private ServiceDiscoveryDaoEtcd serviceDiscoveryDao;
    
    // ==== SERVICES ====
    
    @Autowired
    private CommentsServiceGrpc commentService;
    
    @Value("${killrvideo.service.comment: true}")
    private boolean commentServiceEnabled = true;
    
    @Autowired
    private RatingsServiceGrpc ratingService;
    
    @Value("${killrvideo.service.rating: true}")
    private boolean ratingServiceEnabled = true;
    
    @Autowired
    private SearchServiceGrpc searchService;
    
    @Value("${killrvideo.service.search: true}")
    private boolean searchServiceEnabled = true;
    
    @Autowired
    private StatisticsServiceGrpc statisticsService;
    
    @Value("${killrvideo.service.statistic: true}")
    private boolean statisticServiceEnabled = true;
    
    @Autowired
    private VideoCatalogServiceGrpc videoCatalogService;
 
    @Value("${killrvideo.service.videoCatalog: true}")
    private boolean videoCatalogServiceEnabled = true;
    
    @Autowired
    private UserManagementServiceGrpc userService;
    
    @Value("${killrvideo.service.user: true}")
    private boolean userServiceEnabled = true;
    
    @Autowired
    private SuggestedVideosServiceGrpc suggestedVideosGrpcService;
    
    @Value("${killrvideo.service.suggestedVideo: true}")
    private boolean suggestedVideoServiceEnabled = true;
  
    /**
     * GRPC Server to set up.
     */
    private Server grpcServer;
    
    @PostConstruct
    public void start() throws Exception {
        LOGGER.info("Initializing Grpc Server...");
        
        // Create GRPC server referencing only enabled services
        ServerBuilder<?> builder = ServerBuilder.forPort(grpcPort);
        if (commentServiceEnabled) {
            builder.addService(this.commentService.bindService());
        }
        if (ratingServiceEnabled) {
            builder.addService(this.ratingService.bindService());
        }
        if (searchServiceEnabled) {
            builder.addService(this.searchService.bindService());
        }
        if (statisticServiceEnabled) {
            builder.addService(this.statisticsService.bindService());
        }
        if (videoCatalogServiceEnabled) {
            builder.addService(this.videoCatalogService.bindService());
        }
        if (suggestedVideoServiceEnabled) {
            builder.addService(this.suggestedVideosGrpcService.bindService());
        }
        if (userServiceEnabled) {
            builder.addService(this.userService.bindService());
        }
        grpcServer = builder.build();
        
        // Declare a shutdown hook otherwise JVM is listening on  a port forever
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                stopGrpcServer();
            }
        });

        // Start Grpc listener
        grpcServer.start();
        LOGGER.info("[OK] Grpc Server started on port: '{}'", grpcPort);
        registerServices();
    }
    
    @PreDestroy
    public void stopGrpcServer() {
        LOGGER.info("Calling shutdown for GrpcServer");
        grpcServer.shutdown();
        unRegisterServices();
    }
    
    /**
     * Registering Services to Service Discovery : - ETCD if needed - do nothing with Kubernetes
     */
    private void registerServices() {
        if (commentServiceEnabled) {
            serviceDiscoveryDao.registerService(
                    CommentsServiceGrpc.COMMENTS_SERVICE_NAME, 
                    config.getApplicationHost(), 
                    grpcPort);
        }
        if (ratingServiceEnabled) {
            serviceDiscoveryDao.registerService(
                    RatingsServiceGrpc.RATINGS_SERVICE_NAME, 
                    config.getApplicationHost(), 
                    grpcPort);
        }
        if (searchServiceEnabled) {
            serviceDiscoveryDao.registerService(
                    RatingsServiceGrpc.RATINGS_SERVICE_NAME, 
                    config.getApplicationHost(), 
                    grpcPort);
        }
        if (statisticServiceEnabled) {
            serviceDiscoveryDao.registerService(
                    StatisticsServiceGrpc.STATISTICS_SERVICE_NAME, 
                    config.getApplicationHost(),
                    grpcPort);
        }
        if (videoCatalogServiceEnabled) {
            serviceDiscoveryDao.registerService(
                    VideoCatalogServiceGrpc.VIDEOCATALOG_SERVICE_NAME, 
                    config.getApplicationHost(),
                    grpcPort);
        }
        if (suggestedVideoServiceEnabled) {
            serviceDiscoveryDao.registerService(
                    SuggestedVideosServiceGrpc.SUGESTEDVIDEOS_SERVICE_NAME,
                    config.getApplicationHost(), 
                    grpcPort);
        }
        if (userServiceEnabled) {
            serviceDiscoveryDao.registerService(
                    UserManagementServiceGrpc.USER_SERVICE_NAME, 
                    config.getApplicationHost(),
                    grpcPort);
        }
    }
    
    private void unRegisterServices() {
        if (commentServiceEnabled) {
            serviceDiscoveryDao.unRegisterService(
                    CommentsServiceGrpc.COMMENTS_SERVICE_NAME, 
                    config.getApplicationHost(), 
                    grpcPort);
        }
        if (ratingServiceEnabled) {
            serviceDiscoveryDao.unRegisterService(
                    RatingsServiceGrpc.RATINGS_SERVICE_NAME, 
                    config.getApplicationHost(), 
                    grpcPort);
        }
        if (searchServiceEnabled) {
            serviceDiscoveryDao.unRegisterService(
                    SearchServiceGrpc.SEARCH_SERVICE_NAME, 
                    config.getApplicationHost(), grpcPort);
        }
        if (statisticServiceEnabled) {
            serviceDiscoveryDao.unRegisterService(
                    StatisticsServiceGrpc.STATISTICS_SERVICE_NAME, 
                    config.getApplicationHost(), 
                    grpcPort);
        }
        if (videoCatalogServiceEnabled) {
            serviceDiscoveryDao.unRegisterService(
                    VideoCatalogServiceGrpc.VIDEOCATALOG_SERVICE_NAME, 
                    config.getApplicationHost(), 
                    grpcPort);
        }
        if (suggestedVideoServiceEnabled) {
            serviceDiscoveryDao.unRegisterService(
                    SuggestedVideosServiceGrpc.SUGESTEDVIDEOS_SERVICE_NAME, 
                    config.getApplicationHost(), 
                    grpcPort);
        }
        if (userServiceEnabled) {
            serviceDiscoveryDao.unRegisterService(
                    UserManagementServiceGrpc.USER_SERVICE_NAME, 
                    config.getApplicationHost(), 
                    grpcPort);
        }
    }
    
}
