package com.killrvideo.test.it;

import com.killrvideo.KillrvideoServicesGrpcClient;

import killrvideo.user_management.UserManagementServiceOuterClass.VerifyCredentialsRequest;
import killrvideo.user_management.UserManagementServiceOuterClass.VerifyCredentialsResponse;

public class VideoCatalogIntegrationTest {
    
    public static void main(String[] args) {
        KillrvideoServicesGrpcClient client = 
                new KillrvideoServicesGrpcClient("localhost", 8899);
        
        /*
        SubmitYouTubeVideoRequest myNewVideoYoutube = SubmitYouTubeVideoRequest.newBuilder()
                .addTags("Cassandra")
                .setDescription("MyVideo")
                .setName(" My Sample Video")
                .setUserId(GrpcMappingUtils.uuidToUuid(UUID.randomUUID()))
                .setVideoId(GrpcMappingUtils.uuidToUuid(UUID.randomUUID()))
                .setYouTubeVideoId("EBMriswzd94")
                .build();
        client.videoCatalogServiceGrpcClient.submitYouTubeVideo(myNewVideoYoutube);
        */
        
        VerifyCredentialsRequest creRequest = VerifyCredentialsRequest.newBuilder()
                .setEmail("a.a@a.com")
                .setPassword("aaa")
                .build();
               
        VerifyCredentialsResponse res = client.userServiceGrpcClient.verifyCredentials(creRequest);
        System.out.println(res.getUserId());
        
        
    }

}
