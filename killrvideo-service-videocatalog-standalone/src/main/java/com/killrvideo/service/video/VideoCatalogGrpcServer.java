package com.killrvideo.service.video;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.killrvideo.grpc.AbstractSingleServiceGrpcServer;
import com.killrvideo.service.video.grpc.VideoCatalogServiceGrpc;

import io.grpc.ServerServiceDefinition;

/**
 * Startup a GRPC server on expected port and register all services.
 *
 * @author DataStax Developer Advocates team.
 */
@Component
public class VideoCatalogGrpcServer extends AbstractSingleServiceGrpcServer {

    @Autowired
    private VideoCatalogServiceGrpc videoCatalogService;
    
    /** Listening Port for GRPC. */
    @Value("${killrvideo.grpc-server.port: 305700}")
    protected int defaultPort;
    
    /** {@inheritDoc} */
    public String getServiceName() {
        return VideoCatalogServiceGrpc.VIDEOCATALOG_SERVICE_NAME;
    }
    
    /** {@inheritDoc} */
    public ServerServiceDefinition getService() {
        return videoCatalogService.bindService();
    }

    /** {@inheritDoc} */
    public int getDefaultPort() {
        return defaultPort;
    }
    
}
