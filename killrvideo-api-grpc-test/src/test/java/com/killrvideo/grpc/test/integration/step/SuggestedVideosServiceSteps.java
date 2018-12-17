package com.killrvideo.grpc.test.integration.step;

import static com.killrvideo.grpc.utils.GrpcMapper.uuidToUuid;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.Then;
import killrvideo.ratings.RatingsServiceOuterClass.RateVideoRequest;
import killrvideo.suggested_videos.SuggestedVideosService.GetSuggestedForUserRequest;
import killrvideo.suggested_videos.SuggestedVideosService.GetSuggestedForUserResponse;

/**
 * Integration testing.
 *
 * @author DataStax evangelist team.
 */
public class SuggestedVideosServiceSteps extends AbstractSteps {

    @Before("@suggested_videos_scenarios")
    public void init() {
       truncateAllTablesFromKillrVideoKeyspace();
    }
    
    @After("@suggested_videos_scenarios")
    public void cleanup() {
        truncateAllTablesFromKillrVideoKeyspace();
    }
   
    @Then("target (video\\d+) should have related : (.*)")
    public void getRelatedVideos(String sourceVideo, List<String> expectedRelatedVideos) throws InterruptedException {

        /** video1 is part of dataSet ? */
        assertThat(testDatasetVideos)
                .as("%s is unknown, please specify videoXXX where XXX is a digit")
                .containsKey(sourceVideo);
        
        /** you expect something. */
        assertThat(expectedRelatedVideos)
                .as("Expected related videos should not be empty")
                .isNotEmpty();
       
    }
    
    @Then("(user\\d+) who likes (video\\d+) should be suggested: (.*)")
    public void getSuggestedVideosForUser(String user, String sourceVideo, List<String> expectedRelatedVideos) {
        
        /** video1 is part of dataSet ? */
        assertThat(testDatasetVideos)
                .as("%s is unknown, please specify videoXXX where XXX is a digit")
                .containsKey(sourceVideo);
        
        assertThat(testDatasetUsers)
                .as("%s is unknown, please specify userXXX where XXX is a digit")
                .containsKey(user);
        
        /** you expect something. */
        assertThat(expectedRelatedVideos)
                .as("Expected related videos should not be empty")
                .isNotEmpty();
        
        /** executing the 'liking' operation. */
        grpcClient.getRatingService().rateVideo(RateVideoRequest.newBuilder().setRating(5)
                .setUserId(uuidToUuid(testDatasetUsers.get(user)))
                .setVideoId(uuidToUuid(testDatasetVideos.get(sourceVideo).id))
                .build());
        
        /** Suggestion request. */
        GetSuggestedForUserRequest request = GetSuggestedForUserRequest.newBuilder()
                .setPageSize(100)
                .setUserId(uuidToUuid(testDatasetUsers.get(user)))
                .build();
        final GetSuggestedForUserResponse response = 
                grpcClient.getSuggestedVideoService().getSuggestedForUser(request);

        assertThat(response)
                .as("Cannot find any related videos for source %s ", sourceVideo)
                .isNotNull();
    }
}
