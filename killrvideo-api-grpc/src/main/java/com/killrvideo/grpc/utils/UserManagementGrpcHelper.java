package com.killrvideo.grpc.utils;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.time.Instant;
import java.util.Date;
import java.util.UUID;

import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import com.killrvideo.dse.model.User;
import com.killrvideo.messaging.MessagingDao;

import io.grpc.stub.StreamObserver;
import killrvideo.common.CommonTypes;
import killrvideo.user_management.UserManagementServiceOuterClass.CreateUserRequest;
import killrvideo.user_management.UserManagementServiceOuterClass.GetUserProfileRequest;
import killrvideo.user_management.UserManagementServiceOuterClass.UserProfile;
import killrvideo.user_management.UserManagementServiceOuterClass.VerifyCredentialsRequest;
import killrvideo.user_management.UserManagementServiceOuterClass.VerifyCredentialsResponse;

/**
 * Helper and mappers for DAO <=> GRPC Communications
 *
 * @author DataStax Evangelist Team
 */
@Component
public class UserManagementGrpcHelper extends AbstractGrpcHelper {
    
    /** Inter-service communication channel (messaging). */
    @Autowired
    private MessagingDao messagingDao;
    
    /**
     * Validate create user.
     *
     * @param logger
     * @param request
     * @param streamObserver
     */
    public void validateGrpcRequest_createUser(Logger logger,  CreateUserRequest request, StreamObserver<?> streamObserver) {
        final StringBuilder errorMessage = initErrorString(request);
        boolean isValid = true;
        if (request.getUserId() == null || isBlank(request.getUserId().getValue())) {
            errorMessage.append("\t\tuser id should be provided for create user request\n");
            isValid = false;
        }
        if (isBlank(request.getPassword())) {
            errorMessage.append("\t\tpassword should be provided for create user request\n");
            isValid = false;
        }
        if (isBlank(request.getEmail())) {
            errorMessage.append("\t\temail should be provided for create user request\n");
            isValid = false;
        }
        Assert.isTrue(validate(logger, streamObserver, errorMessage, isValid), "Invalid parameter for 'createUser'");
    }
    
    public void validateGrpcRequest_VerifyCredentials(Logger logger, VerifyCredentialsRequest request, StreamObserver<?> streamObserver) {
        final StringBuilder errorMessage = initErrorString(request);
        boolean isValid = true;

        if (isBlank(request.getEmail())) {
            errorMessage.append("\t\temail should be provided for verify credentials request\n");
            isValid = false;
        }

        if (isBlank(request.getPassword())) {
            errorMessage.append("\t\tpassword should be provided for verify credentials request\n");
            isValid = false;
        }
        Assert.isTrue(validate(logger, streamObserver, errorMessage, isValid), "Invalid parameter for 'verifyCredentials'");
    }
    
    public void validateGrpcRequest_getUserProfile(Logger logger, GetUserProfileRequest request, StreamObserver<?> streamObserver) {
        final StringBuilder errorMessage = initErrorString(request);
        boolean isValid = true;
        if (request.getUserIdsCount() > 20) {
            errorMessage.append("\t\tcannot get more than 20 user profiles at once for get user profile request\n");
            isValid = false;
        }
        for (CommonTypes.Uuid uuid : request.getUserIdsList()) {
            if (uuid == null || isBlank(uuid.getValue())) {
                errorMessage.append("\t\tprovided UUID values cannot be null or blank for get user profile request\n");
                isValid = false;
            }
        }
        Assert.isTrue(validate(logger, streamObserver, errorMessage, isValid), "Invalid parameter for 'getUserProfile'");
    }

    
    public User mapUserRequest2User(CreateUserRequest grpcReq) {
        User user = new User();
        user.setEmail(grpcReq.getEmail());
        user.setCreatedAt(new Date());
        user.setFirstname(grpcReq.getFirstName());
        user.setLastname(grpcReq.getLastName());
        user.setUserid(UUID.fromString(grpcReq.getUserId().getValue()));
        return user;
    }
    
    public UserProfile mapUserToGrpcUserProfile(User user) {
       return UserProfile.newBuilder()
                    .setEmail(user.getEmail())
                    .setFirstName(user.getFirstname())
                    .setLastName(user.getLastname())
                    .setUserId(GrpcMapper.uuidToUuid(user.getUserid()))
                    .build();
    }
    
    public VerifyCredentialsResponse mapResponseVerifyCredentials(UUID userid) {
        return VerifyCredentialsResponse.newBuilder().setUserId(GrpcMapper.uuidToUuid(userid)).build();
    }
    
    /**
     * Publish comment to message bus. 
     * 
     * @param request
     * @param commentCreationDate
     */
    public void publishNewUserEvent(CreateUserRequest grpcReq, Instant commentCreationDate) {
        messagingDao.publishEvent(mapUserRequest2User(grpcReq));
    }
    
}
