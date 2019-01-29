package com.killrvideo.service.user;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.killrvideo.grpc.AbstractSingleServiceGrpcServer;
import com.killrvideo.service.user.grpc.UserManagementServiceGrpc;

import io.grpc.ServerServiceDefinition;

/**
 * Startup a GRPC server on expected port and register all services.
 *
 * @author DataStax Developer Advocates team.
 */
@Component
public class UserManagementGrpcServer extends AbstractSingleServiceGrpcServer {

    @Autowired
    private UserManagementServiceGrpc userManagementService;
    
    /** Listening Port for GRPC. */
    @Value("${killrvideo.grpc-server.port: 305600}")
    protected int defaultPort;
    
    /** {@inheritDoc} */
    public String getServiceName() {
        return UserManagementServiceGrpc.USER_SERVICE_NAME;
    }
    
    /** {@inheritDoc} */
    public ServerServiceDefinition getService() {
        return userManagementService.bindService();
    }

    /** {@inheritDoc} */
    public int getDefaultPort() {
        return defaultPort;
    }
    
}
