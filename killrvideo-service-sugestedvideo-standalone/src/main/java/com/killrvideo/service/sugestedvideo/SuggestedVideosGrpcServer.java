package com.killrvideo.service.sugestedvideo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.killrvideo.grpc.AbstractSingleServiceGrpcServer;
import com.killrvideo.service.sugestedvideo.grpc.SuggestedVideosServiceGrpc;

import io.grpc.ServerServiceDefinition;

/**
 * Startup a GRPC server on expected port and register all services.
 *
 * @author DataStax Developer Advocates team.
 */
@Component
public class SuggestedVideosGrpcServer extends AbstractSingleServiceGrpcServer {

    @Autowired
    private SuggestedVideosServiceGrpc sugestedVideoService;
    
    /** Listening Port for GRPC. */
    @Value("${killrvideo.grpc-server.port: 30500}")
    protected int defaultPort;
    
    /** {@inheritDoc} */
    public String getServiceName() {
        return SuggestedVideosServiceGrpc.SUGESTEDVIDEOS_SERVICE_NAME;
    }
    
    /** {@inheritDoc} */
    public ServerServiceDefinition getService() {
        return sugestedVideoService.bindService();
    }

    /** {@inheritDoc} */
    public int getDefaultPort() {
        return defaultPort;
    }
    
}
