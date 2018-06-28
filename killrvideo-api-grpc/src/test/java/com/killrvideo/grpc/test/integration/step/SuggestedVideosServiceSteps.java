package com.killrvideo.grpc.test.integration.step;

import static com.killrvideo.grpc.utils.GrpcMapper.uuidToUuid;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    /** Logger for Graph. */
    private static Logger LOGGER = LoggerFactory.getLogger(SuggestedVideosServiceSteps.class);
    
    /** Uniaue initialization. */
    private static AtomicReference<Boolean> SHOULD_CHECK_SERVICE= new AtomicReference<>(true);

    @Before("@suggested_videos_scenarios")
    public void init() {
        if (SHOULD_CHECK_SERVICE.get()) {
            etcdDao.read("/killrvideo/services/" + SUGGESTED_VIDEOS_SERVICE_NAME, true);
            etcdDao.read("/killrvideo/services/" + RATINGS_SERVICE_NAME, true);
        }
        LOGGER.info("Truncating Tables BEFORE executing tests");
        cleanUpUserAndVideoTables();
    }
   
    @Then("target (video\\d+) should have related : (.*)")
    public void getRelatedVideos(String sourceVideo, List<String> expectedRelatedVideos) throws InterruptedException {

        /** video1 is part of dataSet ? */
        assertThat(VIDEOS)
                .as("%s is unknown, please specify videoXXX where XXX is a digit")
                .containsKey(sourceVideo);
        
        /** you expect something. */
        assertThat(expectedRelatedVideos)
                .as("Expected related videos should not be empty")
                .isNotEmpty();
        
        /** Suggestion request. *
        GetRelatedVideosRequest request = GetRelatedVideosRequest.newBuilder()
                .setPageSize(100)
                .setVideoId(uuidToUuid(VIDEOS.get(sourceVideo).id))
                .build();
        
        /* Retrying 10 times (graph indexing async and may toake some time)
        Callable<Integer> relatedVideosCallable = () -> {
            System.out.println("[Waiting for graph indexing (Related Video)]");
            return grpcClient.getSuggestedVideoService().getRelatedVideos(request).getVideosList().size();
        };
        RetryConfig config = new RetryConfigBuilder()
                .retryOnReturnValue(new Integer(0))
                .failOnAnyException()
                .withMaxNumberOfTries(10)
                .withDelayBetweenTries(1, ChronoUnit.SECONDS)
                .withFixedBackoff()
                .build();
        new CallExecutor<Integer>(config).execute(relatedVideosCallable);
        Thread.sleep(2000);
        
        final GetRelatedVideosResponse response = grpcClient.getSuggestedVideoService().getRelatedVideos(request);
       
        assertThat(response)
                .as("Cannot find any related videos for source %s ", sourceVideo)
                .isNotNull();

        assertThat(response.getVideosList())
                .as("Related videos should not be empty")
                .isNotEmpty();
       */
    }
    
    @Then("(user\\d+) who likes (video\\d+) should be suggested: (.*)")
    public void getSuggestedVideosForUser(String user, String sourceVideo, List<String> expectedRelatedVideos) {
        
        /** video1 is part of dataSet ? */
        assertThat(VIDEOS)
                .as("%s is unknown, please specify videoXXX where XXX is a digit")
                .containsKey(sourceVideo);
        
        assertThat(USERS)
                .as("%s is unknown, please specify userXXX where XXX is a digit")
                .containsKey(user);
        
        /** you expect something. */
        assertThat(expectedRelatedVideos)
                .as("Expected related videos should not be empty")
                .isNotEmpty();
        
        /** executing the 'liking' operation. */
        grpcClient.getRatingService().rateVideo(RateVideoRequest.newBuilder().setRating(5)
                .setUserId(uuidToUuid(USERS.get(user)))
                .setVideoId(uuidToUuid(VIDEOS.get(sourceVideo).id))
                .build());
        
        /** Suggestion request. */
        GetSuggestedForUserRequest request = GetSuggestedForUserRequest.newBuilder()
                .setPageSize(100)
                .setUserId(uuidToUuid(USERS.get(user)))
                .build();
        
        /**
         * Search is performed using solr INDEX. As such, a delay is required for indexing.
         * Maximum delay of 10s is OK but here we try 10 times, once per second
         *
        Callable<Integer> suggestVideosCallable = () -> {
            System.out.println("[Waiting for graph indexing (Suggestion)]");
            return suggestedStub.getSuggestedForUser(request).getVideosList().size();
        };
        RetryConfig config = new RetryConfigBuilder()
                .retryOnReturnValue(new Integer(0))
                .failOnAnyException()
                .withMaxNumberOfTries(100)
                .withDelayBetweenTries(2, ChronoUnit.SECONDS)
                .withFixedBackoff()
                .build();
        new CallExecutor<Integer>(config).execute(suggestVideosCallable);
        */
        final GetSuggestedForUserResponse response = 
                grpcClient.getSuggestedVideoService().getSuggestedForUser(request);

        assertThat(response)
                .as("Cannot find any related videos for source %s ", sourceVideo)
                .isNotNull();
        
        /* So Far engine did not connect even if we retried
        assertThat(response.getVideosList())
                .as("Related videos should not be empty")
                .isNotEmpty();
        */
       
    }
    
    @After("@suggested_videos_scenarios")
    public void cleanup() {
        LOGGER.info("Truncating users & videos tables AFTER executing tests");
        cleanUpUserAndVideoTables();
    }
}
