package com.killrvideo.grpc.test.integration.step;

import static com.killrvideo.grpc.utils.GrpcMapper.uuidToUuid;
import static org.assertj.core.api.Assertions.assertThat;

import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import killrvideo.ratings.RatingsServiceOuterClass.GetRatingRequest;
import killrvideo.ratings.RatingsServiceOuterClass.GetRatingResponse;
import killrvideo.ratings.RatingsServiceOuterClass.GetUserRatingRequest;
import killrvideo.ratings.RatingsServiceOuterClass.GetUserRatingResponse;
import killrvideo.ratings.RatingsServiceOuterClass.RateVideoRequest;
import killrvideo.ratings.RatingsServiceOuterClass.RateVideoResponse;

/**
 * Testing Rating services
 */
public class RatingServiceSteps extends AbstractSteps {
    
    @Before("@ratings_scenarios")
    public void init() {
        truncateAllTablesFromKillrVideoKeyspace();
    }

    @After("@ratings_scenarios")
    public void cleanup() {
        truncateAllTablesFromKillrVideoKeyspace();
    }

    @When("(user\\d) rates (video\\d) (\\d) stars")
    public void rateVideo(String user, String video, int starNumber) {

        assertThat(testDatasetVideos)
                .as("%s is unknown, please specify videoXXX where XXX is a digit")
                .containsKeys(video);

        assertThat(testDatasetUsers)
                .as("%s is unknown, please specify userXXX where XXX is a digit")
                .containsKey(user);

        assertThat(starNumber)
                .as("Rating star number should be between 1 and 5 included")
                .isBetween(1, 5);

        RateVideoRequest request = RateVideoRequest
                .newBuilder()
                .setRating(starNumber)
                .setUserId(uuidToUuid(testDatasetUsers.get(user)))
                .setVideoId(uuidToUuid(testDatasetVideos.get(video).id))
                .build();

        final RateVideoResponse response = grpcClient.getRatingService().rateVideo(request);

        assertThat(response)
                .as("Cannot rate %s for %s with %s stars", video, user, starNumber)
                .isNotNull();
    }

    @Then("(video\\d) has (\\d+) ratings and total (\\d+) stars")
    public void getRatings(String video, long expectedRatingCount, long expectedTotalStars) {

        assertThat(testDatasetVideos)
                .as("%s is unknown, please specify videoXXX where XXX is a digit")
                .containsKeys(video);

        assertThat(expectedRatingCount)
                .as("Rating count %s for %s should be positive", expectedRatingCount, video)
                .isGreaterThanOrEqualTo(0L);

        assertThat(expectedTotalStars)
                .as("Total stars count %s for %s should be positive", expectedTotalStars, video)
                .isGreaterThanOrEqualTo(0L);

        GetRatingRequest request = GetRatingRequest
                .newBuilder()
                .setVideoId(uuidToUuid(testDatasetVideos.get(video).id))
                .build();

        final GetRatingResponse response = grpcClient.getRatingService().getRating(request);

        assertThat(response)
                .as("Cannot find rating for %s", video)
                .isNotNull();

        assertThat(response.getRatingsCount())
                .as("Rating count for %s should be %s", video, expectedRatingCount)
                .isEqualTo(expectedRatingCount);

        assertThat(response.getRatingsTotal())
                .as("Total stars for %s should be %s", video, expectedTotalStars)
                .isEqualTo(expectedTotalStars);
    }

    @Then("(user\\d) rating for (video\\d) has (\\d+) stars")
    public void getUserRatings(String user, String targetVideo, int expectedTotalStars) {

        assertThat(testDatasetVideos)
                .as("%s is unknown, please specify videoXXX where XXX is a digit")
                .containsKeys(targetVideo);


        assertThat(testDatasetUsers)
                .as("%s is unknown, please specify userXXX where XXX is a digit")
                .containsKey(user);

        assertThat(expectedTotalStars)
                .as("Total stars count %s for %s should be positive", expectedTotalStars, targetVideo)
                .isGreaterThanOrEqualTo(0);

        GetUserRatingRequest request = GetUserRatingRequest
                .newBuilder()
                .setUserId(uuidToUuid(testDatasetUsers.get(user)))
                .setVideoId(uuidToUuid(testDatasetVideos.get(targetVideo).id))
                .build();

        final GetUserRatingResponse response = grpcClient.getRatingService().getUserRating(request);

        assertThat(response)
                .as("Cannot find rating for %s and %s", targetVideo, user)
                .isNotNull();

        assertThat(response.getRating())
                .as("Total stars for %s by %s should be %s", targetVideo, user, expectedTotalStars)
                .isEqualTo(expectedTotalStars);
    }
}
