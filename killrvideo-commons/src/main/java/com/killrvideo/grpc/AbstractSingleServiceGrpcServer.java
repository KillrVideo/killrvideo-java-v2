package com.killrvideo.grpc;

import java.util.Optional;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.killrvideo.conf.KillrVideoConfiguration;
import com.killrvideo.discovery.ServiceDiscoveryDaoEtcd;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerServiceDefinition;

/**
 * Support class to build GRPC Server per service.
 *
 * @author Cedrick LUNVEN (@clunven)
 */
public abstract class AbstractSingleServiceGrpcServer {
    
    /** Some logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSingleServiceGrpcServer.class);
   
    /** Global Configuration. s*/
    @Autowired
    protected KillrVideoConfiguration killrVideoConfig;
    
    /** Connectivity to ETCD Service discovery. */
    @Autowired
    protected ServiceDiscoveryDaoEtcd serviceDiscoveryDao;
    
    /** GRPC Server to start. */
    protected Server grpcServer;
    
    /** Port to be allocated dynamically based on ETCD. */
    protected static int grpcServerPort = 0;
    
    /** Service Name. */
    protected abstract String getServiceName();
    
    /** Lock target port. */
    protected abstract int getDefaultPort();
    
    /** Service Definition. */
    protected abstract ServerServiceDefinition getService();
   
    /**
     * Start
     */
    @PostConstruct
    public void startGrpcServer() throws Exception {
        LOGGER.info("Initializing Comment Service");
        grpcServerPort = getDefaultPort();
        Optional<Integer> maxUsedPort = serviceDiscoveryDao.lookupServicePorts(getServiceName(), 
                killrVideoConfig.getApplicationHost());
        if (maxUsedPort.isPresent()) {
            grpcServerPort = maxUsedPort.get() + 1;
        }
        grpcServer = ServerBuilder.forPort(grpcServerPort)
                              .addService(getService())
                              .build();
        Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    stopGrpcServer();
                }
        });
        grpcServer.start();
        LOGGER.info("[OK] Grpc Server started on port: '{}'", grpcServerPort);
        serviceDiscoveryDao.register(getServiceName(), 
                killrVideoConfig.getApplicationHost(), grpcServerPort);
    }
    
    @PreDestroy
    public void stopGrpcServer() {
        LOGGER.info("Calling shutdown for GrpcServer");
        serviceDiscoveryDao.register(getServiceName(), 
                killrVideoConfig.getApplicationHost(), grpcServerPort);
        grpcServer.shutdown();
    }
    
}
