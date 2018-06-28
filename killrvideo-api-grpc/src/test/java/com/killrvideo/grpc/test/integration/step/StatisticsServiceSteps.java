package com.killrvideo.grpc.test.integration.step;

import static com.killrvideo.grpc.utils.GrpcMapper.uuidToUuid;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.killrvideo.dse.utils.DseUtils;

import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Then;
import killrvideo.statistics.StatisticsServiceOuterClass.GetNumberOfPlaysRequest;
import killrvideo.statistics.StatisticsServiceOuterClass.GetNumberOfPlaysRequest.Builder;
import killrvideo.statistics.StatisticsServiceOuterClass.GetNumberOfPlaysResponse;
import killrvideo.statistics.StatisticsServiceOuterClass.PlayStats;
import killrvideo.statistics.StatisticsServiceOuterClass.RecordPlaybackStartedRequest;
import killrvideo.statistics.StatisticsServiceOuterClass.RecordPlaybackStartedResponse;

public class StatisticsServiceSteps extends AbstractSteps {

    private static Logger LOGGER = LoggerFactory.getLogger(RatingServiceSteps.class);
    private static AtomicReference<Boolean> SHOULD_CHECK_SERVICE= new AtomicReference<>(true);

    @Before("@stats_scenarios")
    public void init() {
        if (SHOULD_CHECK_SERVICE.get()) {
            etcdDao.read("/killrvideo/services/" + STATISTICS_SERVICE_NAME, true);
        }
        LOGGER.info("Truncating users, videos & statistics tables BEFORE executing tests");
        cleanUpUserAndVideoTables();
        DseUtils.truncate(dseSession, "video_playback_stats");
    }

    @After("@stats_scenarios")
    public void cleanup() {
        LOGGER.info("Truncating users, videos & statistics tables AFTER executing tests");
        cleanUpUserAndVideoTables();
        DseUtils.truncate(dseSession, "video_playback_stats");
    }

    @And("(video\\d) is watched (\\d+) times")
    public void recordPlayback(String video, int playbackCount) {

        assertThat(VIDEOS)
                .as("%s is unknown, please specify videoXXX where XXX is a digit")
                .containsKeys(video);

        assertThat(playbackCount)
                .as("Playback count should be strictly positive")
                .isGreaterThan(0);

        RecordPlaybackStartedRequest request;
        RecordPlaybackStartedResponse response;

        for(int i=1; i<= playbackCount; i++) {

            request = RecordPlaybackStartedRequest
                    .newBuilder()
                    .setVideoId(uuidToUuid(VIDEOS.get(video).id))
                    .build();

            response = grpcClient.getStatisticService().recordPlaybackStarted(request);

            assertThat(response)
                    .as("Cannot record playback for %s", video)
                    .isNotNull();
        }
    }

    @Then("(.*) statistics shows (.*) plays")
    public void getVideosStatistics(List<String> videos, List<Long> expectedPlaybackCountsList) {

        final int videosCount = videos.size();
        final int playbackListSize = expectedPlaybackCountsList.size();
        final String listOfVideos = String.join(", ", videos);
        final String listOfStatistics = String.join(", ", expectedPlaybackCountsList.stream().map(x -> x.toString()).collect(toList()));

        assertThat(VIDEOS)
                .as("%s is unknown, please specify videoXXX where XXX is a digit")
                .containsKeys(videos.toArray(new String[videosCount]));


        assertThat(videosCount)
                .as("The list of playback count %s should have same length as videos %s",
                        listOfStatistics,
                        listOfVideos)
                .isEqualTo(playbackListSize);


        Map<String, Long> videoPlayBackStats = IntStream.rangeClosed(0, videosCount-1)
                .boxed()
                .collect(toMap(videos::get, expectedPlaybackCountsList::get));


        final Builder builder = GetNumberOfPlaysRequest.newBuilder();

        videos.forEach(video -> builder.addVideoIds(uuidToUuid(VIDEOS.get(video).id)));

        final GetNumberOfPlaysRequest request = builder.build();

        final GetNumberOfPlaysResponse response = grpcClient.getStatisticService().getNumberOfPlays(request);

        assertThat(response)
                .as("Cannot find playback statistics for %s", listOfVideos)
                .isNotNull();

        assertThat(response.getStatsList())
                .as("Cannot find playback statistics for %s", listOfVideos)
                .hasSize(videosCount);

        final Map<String, Long> expectedVideosPlayBackStats = response.getStatsList()
                .stream()
                .collect(toMap(x -> VIDEOS_BY_ID.get(UUID.fromString(x.getVideoId().getValue())),
                        PlayStats::getViews));

        assertThat(expectedVideosPlayBackStats)
                .as("Playback statistics for %s are not %s", listOfVideos, listOfStatistics)
                .isEqualTo(videoPlayBackStats);

    }
}
