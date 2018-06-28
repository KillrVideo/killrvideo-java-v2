package com.killrvideo.grpc.server;

import static java.lang.String.format;

import java.io.IOException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.killrvideo.core.conf.KillrVideoConfiguration;
import com.killrvideo.core.dao.EtcdDao;
import com.killrvideo.dse.utils.CassandraMutationErrorHandler;
import com.killrvideo.grpc.services.CommentsGrpcService;
import com.killrvideo.grpc.services.RatingsGrpcService;
import com.killrvideo.grpc.services.SearchGrpcService;
import com.killrvideo.grpc.services.StatisticsGrpcService;
import com.killrvideo.grpc.services.SuggestedVideosGrpcService;
import com.killrvideo.grpc.services.UploadsGrpcService;
import com.killrvideo.grpc.services.UserManagementGrpcService;
import com.killrvideo.grpc.services.VideoCatalogGrpcService;
import com.killrvideo.messaging.MessagingDao;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerServiceDefinition;

/**
 * Startup a GRPC server on expected port and register all services.
 *
 * @author DataStax evangelist team.
 */
@Component
public class GrpcServer {

    /** Some logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(GrpcServer.class);
    
    /** Listening Port for GRPC. */
    @Value("${grpc.port: 8899}")
    private int grpcPort;
    
    /** Connectivity to ETCD Service discovery. */
    @Autowired
    private KillrVideoConfiguration config;
    
    /** Connectivity to ETCD Service discovery. */
    @Autowired
    private EtcdDao serviceDiscoveryDao;
    
    /** Communication channel between service. */
    @Autowired
    private MessagingDao messagingDao;
    
    /** Trap error and push to bus. */
    @Autowired
    private CassandraMutationErrorHandler errorHandler;
    
    // ==== GRPC SERVICES ====
    
    @Autowired
    private CommentsGrpcService commentService;
    
    @Autowired
    private RatingsGrpcService ratingService;
    
    @Autowired
    private SearchGrpcService searchService;
    
    @Autowired
    private StatisticsGrpcService statisticsService;
    
    @Autowired
    private VideoCatalogGrpcService videoCatalogService;
    
    @Autowired
    private UploadsGrpcService uploadGrpcService;
    
    @Autowired
    private UserManagementGrpcService userService;
    
    @Autowired
    private SuggestedVideosGrpcService suggestedVideosGrpcService;
  
    /**
     * GRPC Server to set up.
     */
    private Server server;
    
    /** Initiqlized once at startup use for ETCD. */
    private String applicationUID;
    
    @PostConstruct
    public void start() throws Exception {
        applicationUID = config.getApplicationName().trim()  + ":" + config.getApplicationInstanceId();
        LOGGER.info("Initializing Grpc Server...");
        
        // Binding Services
        final ServerServiceDefinition commentService             = this.commentService.bindService();
        final ServerServiceDefinition ratingService              = this.ratingService.bindService();
        final ServerServiceDefinition searchService              = this.searchService.bindService();
        final ServerServiceDefinition statisticsService          = this.statisticsService.bindService();
        final ServerServiceDefinition suggestedVideosGrpcService = this.suggestedVideosGrpcService.bindService();
        final ServerServiceDefinition videoCatalogService        = this.videoCatalogService.bindService();
        final ServerServiceDefinition uploadGrpcService          = this.uploadGrpcService.bindService();
        final ServerServiceDefinition userService                = this.userService.bindService();
        
        // Reference Service in Server
        server = ServerBuilder.forPort(grpcPort)
                   .addService(commentService)
                   .addService(ratingService)
                   .addService(searchService)
                   .addService(statisticsService)
                   .addService(suggestedVideosGrpcService)
                   .addService(videoCatalogService)
                   .addService(uploadGrpcService)
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
        LOGGER.info("Grpc Server started on port: '{}'", grpcPort);
       
        // Initialize Event bus
        messagingDao.register(errorHandler);
        
        // Service are now Bound an started, declare in ETCD
        final String applicationAdress = format("%s:%d", config.getApplicationHost(), grpcPort);
        LOGGER.info("Registering services in ETCD with address {}", applicationAdress);
        registerServicesToEtcd(applicationAdress, 
                commentService, ratingService, searchService, 
                statisticsService, suggestedVideosGrpcService, 
                videoCatalogService, uploadGrpcService, userService);
        LOGGER.info(" = Services now registered in ETCD");
    }

    @PreDestroy
    public void stop() {
        //eventBus.unregister(suggestedVideosService);
        messagingDao.unRegister(errorHandler);
        server.shutdown();
    }

    /**
     * Enter information into ETCD.
     *
     * @param serviceDefinitions
     *          services definitions from GRPC
     * @throws IOException
     *          exception when accessing ETCD
     */
    private void registerServicesToEtcd(String applicationAdress, ServerServiceDefinition... serviceDefinitions) 
    throws IOException {
        // Note that we don't use a lambda to ease Exception propagation
        for (ServerServiceDefinition service : serviceDefinitions) {
            String serviceKey = serviceDiscoveryDao.buildServiceKey(applicationUID, service.getServiceDescriptor().getName());
            LOGGER.info(" + key={}", serviceKey);
            serviceDiscoveryDao.register(serviceKey, applicationAdress);
        }
    }
    
}
