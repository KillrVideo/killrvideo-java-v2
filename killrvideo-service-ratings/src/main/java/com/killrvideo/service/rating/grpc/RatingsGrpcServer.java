package com.killrvideo.service.rating.grpc;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.killrvideo.grpc.AbstractSingleServiceGrpcServer;

import io.grpc.ServerServiceDefinition;

/**
 * Startup a GRPC server on expected port and register all services.
 *
 * @author DataStax Developer Advocates team.
 */
@Component
public class RatingsGrpcServer extends AbstractSingleServiceGrpcServer {

    @Autowired
    private RatingsServiceGrpc ratingService;
    
    /** Listening Port for GRPC. */
    @Value("${killrvideo.grpc-server.port: 30100}")
    protected int defaultPort;
    
    /** {@inheritDoc} */
    public String getServiceName() {
        return RatingsServiceGrpc.RATINGS_SERVICE_NAME;
    }
    
    /** {@inheritDoc} */
    public ServerServiceDefinition getService() {
        return ratingService.bindService();
    }

    /** {@inheritDoc} */
    public int getDefaultPort() {
        return defaultPort;
    }
    
}
