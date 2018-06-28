package com.killrvideo.grpc.services;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.killrvideo.core.error.ErrorEvent;
import com.killrvideo.core.utils.HashUtils;
import com.killrvideo.dse.dao.UserDseDao;
import com.killrvideo.dse.model.User;
import com.killrvideo.dse.model.UserCredentials;
import com.killrvideo.grpc.utils.UserManagementGrpcHelper;
import com.killrvideo.messaging.MessagingDao;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import killrvideo.user_management.UserManagementServiceGrpc.UserManagementServiceImplBase;
import killrvideo.user_management.UserManagementServiceOuterClass.CreateUserRequest;
import killrvideo.user_management.UserManagementServiceOuterClass.CreateUserResponse;
import killrvideo.user_management.UserManagementServiceOuterClass.GetUserProfileRequest;
import killrvideo.user_management.UserManagementServiceOuterClass.GetUserProfileResponse;
import killrvideo.user_management.UserManagementServiceOuterClass.VerifyCredentialsRequest;
import killrvideo.user_management.UserManagementServiceOuterClass.VerifyCredentialsResponse;

/**
 * Create or update users.
 *
 * @author DataStax Evangelist Team
 */
@Service
public class UserManagementGrpcService extends UserManagementServiceImplBase {
    
    /** Loger for that class. */
    private static Logger LOGGER = LoggerFactory.getLogger(UserManagementGrpcService.class);
    
    @Autowired
    private UserManagementGrpcHelper helper;
    
    @Autowired
    private MessagingDao messagingDao;
    
    @Autowired
    private UserDseDao userDseDao;
    
     /** {@inheritDoc} */
    @Override
    public void createUser(
            final CreateUserRequest grpcReq, 
            final StreamObserver<CreateUserResponse> grpcResObserver) {
      
        // Validate Parameters
        helper.validateGrpcRequest_createUser(LOGGER, grpcReq, grpcResObserver);
        
        // Stands as stopwatch for logging and messaging 
        final Instant starts = Instant.now();
        
        // Mapping GRPC => Domain (Dao)
        User user = helper.mapUserRequest2User(grpcReq);
        final String hashedPassword = HashUtils.hashPassword(grpcReq.getPassword().trim());
        
         // Invoke DAO Async
        userDseDao.createUserAsync(user, hashedPassword).whenComplete((result, error) -> {
            if (error != null ) {
                helper.traceError(LOGGER, "createUser", starts, error);
                messagingDao.publishExceptionEvent(new ErrorEvent(grpcReq, error), error);
                grpcResObserver.onError(Status.INVALID_ARGUMENT.augmentDescription(error.getMessage())
                               .asRuntimeException());
            } else {
                helper.traceSuccess(LOGGER, "createUser", starts);
                helper.publishNewUserEvent(grpcReq, starts);
                grpcResObserver.onNext(CreateUserResponse.newBuilder().build());
                grpcResObserver.onCompleted();
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public void verifyCredentials(
            final VerifyCredentialsRequest grpcReq, 
            final StreamObserver<VerifyCredentialsResponse> grpcResObserver) {
        
        // Validate Parameters
        helper.validateGrpcRequest_VerifyCredentials(LOGGER, grpcReq, grpcResObserver);
        
        // Stands as stopwatch for logging and messaging 
        final Instant starts = Instant.now();
        
        // Mapping GRPC => Domain (Dao)
        String email = grpcReq.getEmail();
        
        // Invoke Async
        CompletableFuture<UserCredentials> futureCredential = userDseDao.getUserCredentialAsync(email);
        
        // Map back as GRPC (if correct invalid credential otherwize)
        futureCredential.whenComplete((credential, error) -> {
            if (error != null ) {
                helper.traceError(LOGGER, "verifyCredentials", starts, error);
                messagingDao.publishExceptionEvent(new ErrorEvent(grpcReq, error), error);
                if (!HashUtils.isPasswordValid(grpcReq.getPassword(), credential.getPassword())) {
                    grpcResObserver.onError(Status.INVALID_ARGUMENT
                                   .withDescription("Email address or password are not correct").asRuntimeException());
                } else {
                    grpcResObserver.onError(Status.INTERNAL.withCause(error).asRuntimeException());
                }
            } else {
                helper.traceSuccess(LOGGER, "verifyCredentials", starts);
                grpcResObserver.onNext(helper.mapResponseVerifyCredentials(credential.getUserid()));
                grpcResObserver.onCompleted();
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public void getUserProfile(
            final GetUserProfileRequest grpcReq, 
            final StreamObserver<GetUserProfileResponse> grpcResObserver) {
        
        // Validate Parameters
        helper.validateGrpcRequest_getUserProfile(LOGGER, grpcReq, grpcResObserver);
        
        // Stands as stopwatch for logging and messaging 
        final Instant starts = Instant.now();
        
        // No user id provided, still a valid confition
        final GetUserProfileResponse.Builder builder = GetUserProfileResponse.newBuilder();
        if (grpcReq.getUserIdsCount() == 0 || CollectionUtils.isEmpty(grpcReq.getUserIdsList())) {
            helper.traceSuccess(LOGGER, "getUserProfile", starts);
            grpcResObserver.onNext(builder.build());
            grpcResObserver.onCompleted();
            LOGGER.debug("No user id provided");
            return;
        } else {
            
            // Mapping GRPC => Domain (Dao)
            List < UUID > listOfUserId = Arrays.asList(grpcReq
                    .getUserIdsList()
                    .stream()
                    .map(uuid -> UUID.fromString(uuid.getValue()))
                    .toArray(size -> new UUID[size]));
            
            // Execute Async
            CompletableFuture<List<User>> userListFuture = userDseDao.getUserProfilesAsync(listOfUserId);
            
            // Mapping back to GRPC objects
            userListFuture.whenComplete((users, error) -> {
                if (error != null ) {
                    helper.traceError(LOGGER, "getUserProfile", starts, error);
                    messagingDao.publishExceptionEvent(new ErrorEvent(grpcReq, error), error);
                    grpcResObserver.onError(Status.INTERNAL.withCause(error).asRuntimeException());
                } else {
                    helper.traceSuccess(LOGGER, "getUserProfile", starts);
                    users.stream().map(helper::mapUserToGrpcUserProfile).forEach(builder::addProfiles);    
                    grpcResObserver.onNext(builder.build());
                    grpcResObserver.onCompleted();
                }
            });
        }
        
    }
}
