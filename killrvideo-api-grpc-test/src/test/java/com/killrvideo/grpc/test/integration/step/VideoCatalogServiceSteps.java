package com.killrvideo.grpc.test.integration.step;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import com.datastax.driver.core.Row;
import com.killrvideo.grpc.test.integration.dto.CucumberVideoDetails;
import com.killrvideo.grpc.test.integration.dto.VideoNameById;
import com.killrvideo.grpc.utils.GrpcMapper;

import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetLatestVideoPreviewsRequest;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetLatestVideoPreviewsResponse;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetUserVideoPreviewsRequest;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetUserVideoPreviewsResponse;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetVideoPreviewsRequest;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetVideoPreviewsResponse;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetVideoRequest;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetVideoResponse;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.SubmitYouTubeVideoRequest;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.SubmitYouTubeVideoResponse;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.VideoPreview;

/**
 * Suggestion engine.
 * 
 * @author DataStax evangelist team.
 */
public class VideoCatalogServiceSteps extends AbstractSteps {
   
    @Before("@video_scenarios")
    public void init() {
       truncateAllTablesFromKillrVideoKeyspace();
    }

    @After("@video_scenarios")
    public void cleanup() {
        truncateAllTablesFromKillrVideoKeyspace();
    }
    
    @When("^(user\\d) submit Youtube videos:$")
    public void createVideos(String user, List<CucumberVideoDetails> videos) throws Exception {
        for (CucumberVideoDetails video : videos) {
            assertThat(video.tags)
                    .as("There should be at least one tag provided for %s", video.id)
                    .isNotEmpty();
            final SubmitYouTubeVideoRequest request = SubmitYouTubeVideoRequest
                    .newBuilder()
                    .setName(video.name)
                    .setVideoId(GrpcMapper.uuidToUuid(testDatasetVideos.get(video.id).id))
                    .setUserId(GrpcMapper.uuidToUuid(testDatasetUsers.get(user)))
                    .setDescription(video.description)
                    .setYouTubeVideoId(video.url)
                    .addAllTags(Arrays.asList(video.tags.split(",")))
                    .build();
            final SubmitYouTubeVideoResponse response = grpcClient.getVideoCatalogService().submitYouTubeVideo(request);
            assertThat(response).as("Cannot create %s for %s", video.id, user).isNotNull();
        }
    }
    
    @Then("I can retrieve (video\\d) by id")
    public void getVideoById(String video) {
        assertThat(testDatasetVideos).as("%s is unknown, please specify videoXXX where XXX is a digit").containsKey(video);
        final VideoNameById videoNameById = testDatasetVideos.get(video);

        GetVideoRequest request = GetVideoRequest
                .newBuilder()
                .setVideoId(GrpcMapper.uuidToUuid(videoNameById.id))
                .build();

        final GetVideoResponse response = grpcClient.getVideoCatalogService().getVideo(request);

        assertThat(response)
                .as("Cannot find %s", video)
                .isNotNull();

        assertThat(response.getName())
                .as("Cannot find %s", video)
                .isEqualTo(videoNameById.name);
    }

    @Then("I can get preview of: (.*)")
    public void getVideosPreview(List<String> expectedVideos) {
        final GetVideoPreviewsRequest.Builder builder = GetVideoPreviewsRequest.newBuilder();
        for (String video : expectedVideos) {
            assertThat(testDatasetVideos)
                    .as("%s is unknown, please specify videoXXX where XXX is a digit")
                    .containsKey(video);
            builder.addVideoIds(GrpcMapper.uuidToUuid(testDatasetVideos.get(video).id));
        }

        final GetVideoPreviewsResponse response = grpcClient.getVideoCatalogService().getVideoPreviews(builder.build());

        assertThat(response)
                .as("Cannot get previews for %s", String.join(", ", expectedVideos))
                .isNotNull();

        final int expectedVideoCount = expectedVideos.size();

        assertThat(response.getVideoPreviewsList())
                .as("Cannot get previews for %s", String.join(", ", expectedVideos))
                .hasSize(expectedVideoCount);

        assertThat(response.getVideoPreviewsList().stream().map(VideoPreview::getName).collect(toList()))
                .as("Cannot get previews for %s", String.join(", ", expectedVideos))
                .containsExactly(expectedVideos.stream().map(x -> testDatasetVideos.get(x).name).collect(toList()).toArray(new String[expectedVideoCount]));
    }

    @Then("latest videos preview contains: (.*)")
    public void getLatestVideosPreview(List<String> expectedVideos) {
        final int expectedVideoCount = expectedVideos.size();
        assertThat(testDatasetVideos)
                .as("%s is unknown, please specify videoXXX where XXX is a digit")
                .containsKeys(expectedVideos.toArray(new String[expectedVideoCount]));

        GetLatestVideoPreviewsRequest request = GetLatestVideoPreviewsRequest
                .newBuilder()
                .setPageSize(100)
                .build();

        final GetLatestVideoPreviewsResponse response = grpcClient.getVideoCatalogService().getLatestVideoPreviews(request);

        assertThat(response)
                .as("Cannot get latest videos preview for %s", String.join(" ,", expectedVideos))
                .isNotNull();

        assertThat(response.getVideoPreviewsList())
                .as("Cannot get latest videos preview for %s", String.join(" ,", expectedVideos))
                .hasSize(expectedVideoCount);

        assertThat(response.getVideoPreviewsList().stream().map(VideoPreview::getName).collect(toList()))
                .as("Cannot get latest videos preview for %s", String.join(" ,", expectedVideos))
                .containsExactly(expectedVideos.stream().map(x -> testDatasetVideos.get(x).name).collect(toList()).toArray(new String[expectedVideoCount]));
    }

    @Then("(user\\d) videos preview contains: (.*)")
    public void getUserVideosPreview(String user, List<String> expectedVideos) {

        final int expectedVideoCount = expectedVideos.size();

        assertThat(testDatasetVideos)
                .as("%s is unknown, please specify videoXXX where XXX is a digit")
                .containsKeys(expectedVideos.toArray(new String[expectedVideoCount]));

        assertThat(testDatasetUsers)
                .as("%s is unknown, please specify userXXX where XXX is a digit")
                .containsKey(user);

        GetUserVideoPreviewsRequest request = GetUserVideoPreviewsRequest
                .newBuilder()
                .setUserId(GrpcMapper.uuidToUuid(testDatasetUsers.get(user)))
                .setPageSize(100)
                .build();

        final GetUserVideoPreviewsResponse response = grpcClient.getVideoCatalogService().getUserVideoPreviews(request);

        assertThat(response)
                .as("Cannot get latest videos preview for %s", String.join(" ,", expectedVideos))
                .isNotNull();

        assertThat(response.getVideoPreviewsList())
                .as("Cannot get latest videos preview for %s", String.join(" ,", expectedVideos))
                .hasSize(expectedVideoCount);

        assertThat(response.getVideoPreviewsList().stream().map(VideoPreview::getName).collect(toList()))
                .as("Cannot get latest videos preview for %s", String.join(" ,", expectedVideos))
                .containsExactly(expectedVideos.stream().map(x -> testDatasetVideos.get(x).name).collect(toList()).toArray(new String[expectedVideoCount]));
    }

    @Then("latest videos preview starting from (video\\d) contains: (.*)")
    public void getLatestVideosPreviewWithStartVideoId(String startVideo, List<String> videos) {
        final int expectedVideoCount = videos.size();
        assertThat(testDatasetVideos)
                .as("%s is unknown, please specify videoXXX where XXX is a digit")
                .containsKeys(videos.toArray(new String[expectedVideoCount]));
        final UUID startVideoId = testDatasetVideos.get(startVideo).id;
        final Row row = dseSession.execute(findVideoByIdPs.bind(startVideoId)).one();

        assertThat(row)
                .as("Cannot load %s info", startVideo)
                .isNotNull();

        final Date startVideoAddedDate = row.getTimestamp("added_date");
        assertThat(startVideoAddedDate)
                .as("Cannot find added_date for %s", startVideo)
                .isNotNull();


        GetLatestVideoPreviewsRequest request = GetLatestVideoPreviewsRequest
                .newBuilder()
                .setStartingVideoId(GrpcMapper.uuidToUuid(startVideoId))
                .setStartingAddedDate(GrpcMapper.dateToTimestamp(startVideoAddedDate))
                .setPageSize(2)
                .build();

        final GetLatestVideoPreviewsResponse response = grpcClient.getVideoCatalogService().getLatestVideoPreviews(request);

        assertThat(response)
                .as("Cannot get latest videos preview for %s", String.join(" ,", videos))
                .isNotNull();

        assertThat(response.getVideoPreviewsList())
                .as("Cannot get latest videos preview for %s", String.join(" ,", videos))
                .hasSize(expectedVideoCount);

        assertThat(response.getVideoPreviewsList().stream().map(VideoPreview::getName).collect(toList()))
                .as("Cannot get latest videos preview for %s", String.join(" ,", videos))
                .containsExactly(videos.stream().map(x -> testDatasetVideos.get(x).name).collect(toList()).toArray(new String[expectedVideoCount]));
    }

    @Then("latest videos preview at page (\\d) contains: (.*)")
    public void getLatestVideosPreviewWithPaging(int pageNumber, List<String> expectedVideos) {
        final int expectedVideoCount = expectedVideos.size();
        assertThat(testDatasetVideos)
                .as("%s is unknown, please specify videoXXX where XXX is a digit")
                .containsKeys(expectedVideos.toArray(new String[expectedVideoCount]));

        Optional<String> pagingState = Optional.empty();
        for (int i=1; i<pageNumber; i++) {
            pagingState = fetchLatestVideosPages(pagingState);
        }

        GetLatestVideoPreviewsRequest request = GetLatestVideoPreviewsRequest
                .newBuilder()
                .setPagingState(pagingState.get())
                .setPageSize(3)
                .build();

        final GetLatestVideoPreviewsResponse response = grpcClient.getVideoCatalogService().getLatestVideoPreviews(request);

        assertThat(response)
                .as("Cannot get latest videos preview for %s", String.join(" ,", expectedVideos))
                .isNotNull();

        assertThat(response.getVideoPreviewsList())
                .as("Cannot get latest videos preview for %s", String.join(" ,", expectedVideos))
                .hasSize(expectedVideoCount);

        assertThat(response.getVideoPreviewsList().stream().map(VideoPreview::getName).collect(toList()))
                .as("Cannot get latest videos preview for %s", String.join(" ,", expectedVideos))
                .containsExactly(expectedVideos.stream().map(x -> testDatasetVideos.get(x).name).collect(toList()).toArray(new String[expectedVideoCount]));
    }

    @Then("(user\\d) videos preview starting from (video\\d) contains: (.*)")
    public void getUserVideosPreviewWithStartVideoId(String user, String startVideo, List<String> expectedVideos) {

        final int expectedVideoCount = expectedVideos.size();

        assertThat(testDatasetVideos)
                .as("%s is unknown, please specify videoXXX where XXX is a digit")
                .containsKeys(expectedVideos.toArray(new String[expectedVideoCount]));

        assertThat(testDatasetUsers)
                .as("%s is unknown, please specify userXXX where XXX is a digit")
                .containsKey(user);


        final UUID startVideoId = testDatasetVideos.get(startVideo).id;
        final Row row = dseSession.execute(findVideoByIdPs.bind(startVideoId)).one();

        assertThat(row)
                .as("Cannot load %s info", startVideo)
                .isNotNull();

        final Date startVideoAddedDate = row.getTimestamp("added_date");
        assertThat(startVideoAddedDate)
                .as("Cannot find added_date for %s", startVideo)
                .isNotNull();

        GetUserVideoPreviewsRequest request = GetUserVideoPreviewsRequest
                .newBuilder()
                .setUserId(GrpcMapper.uuidToUuid(testDatasetUsers.get(user)))
                .setStartingVideoId(GrpcMapper.uuidToUuid(startVideoId))
                .setStartingAddedDate(GrpcMapper.dateToTimestamp(startVideoAddedDate))
                .setPageSize(2)
                .build();

        final GetUserVideoPreviewsResponse response = grpcClient.getVideoCatalogService().getUserVideoPreviews(request);

        assertThat(response)
                .as("Cannot get latest videos preview for %s", String.join(" ,", expectedVideos))
                .isNotNull();

        assertThat(response.getVideoPreviewsList())
                .as("Cannot get latest videos preview for %s", String.join(" ,", expectedVideos))
                .hasSize(expectedVideoCount);

        assertThat(response.getVideoPreviewsList().stream().map(VideoPreview::getName).collect(toList()))
                .as("Cannot get latest videos preview for %s", String.join(" ,", expectedVideos))
                .containsExactly(expectedVideos.stream().map(x -> testDatasetVideos.get(x).name).collect(toList()).toArray(new String[expectedVideoCount]));
    }

    @Then("(user\\d) videos preview at page (\\d) contains: (.*)")
    public void getUserVideosPreviewWithPagingState(String user, int pageNumber, List<String> expectedVideos) {
        final int expectedVideoCount = expectedVideos.size();

        assertThat(testDatasetVideos)
                .as("%s is unknown, please specify videoXXX where XXX is a digit")
                .containsKeys(expectedVideos.toArray(new String[expectedVideoCount]));

        assertThat(testDatasetUsers)
                .as("%s is unknown, please specify userXXX where XXX is a digit")
                .containsKey(user);

        Optional<String> pagingState = Optional.empty();
        for (int i=1; i<pageNumber; i++) {
            pagingState = fetchUserVideosPages(user, pagingState);
        }

        GetUserVideoPreviewsRequest request = GetUserVideoPreviewsRequest
                .newBuilder()
                .setUserId(GrpcMapper.uuidToUuid(testDatasetUsers.get(user)))
                .setPagingState(pagingState.get())
                .setPageSize(2)
                .build();

        final GetUserVideoPreviewsResponse response = grpcClient.getVideoCatalogService().getUserVideoPreviews(request);

        assertThat(response)
                .as("Cannot get %s videos preview for %s", user, String.join(" ,", expectedVideos))
                .isNotNull();
        assertThat(response.getVideoPreviewsList())
                .as("Cannot get %s videos preview for %s", user, String.join(" ,", expectedVideos))
                .hasSize(expectedVideoCount);
        assertThat(response.getVideoPreviewsList().stream().map(VideoPreview::getName).collect(toList()))
                .as("Cannot get %s videos preview for %s", user, String.join(" ,", expectedVideos))
                .containsExactly(expectedVideos.stream().map(x -> testDatasetVideos.get(x).name).collect(toList()).toArray(new String[expectedVideoCount]));
    }

    private Optional<String> fetchLatestVideosPages(Optional<String> pagingState) {
        GetLatestVideoPreviewsRequest request;
        if (pagingState.isPresent()) {
            request = GetLatestVideoPreviewsRequest
                    .newBuilder()
                    .setPagingState(pagingState.get())
                    .setPageSize(3)
                    .build();

        } else {
            request = GetLatestVideoPreviewsRequest
                    .newBuilder()
                    .setPageSize(3)
                    .build();
        }

        final GetLatestVideoPreviewsResponse response = grpcClient.getVideoCatalogService().getLatestVideoPreviews(request);

        assertThat(response)
                .as("Cannot fetch latest videos with fetch size == 3")
                .isNotNull();

        assertThat(response.getVideoPreviewsList())
                .as("Cannot fetch latest videos with fetch size == 3")
                .hasSize(3);

        assertThat(response.getPagingState())
                .as("There is no latest videos remaining for next page")
                .isNotEmpty();

        return Optional.of(response.getPagingState());
    }

    private Optional<String> fetchUserVideosPages(String user, Optional<String> pagingState) {
        GetUserVideoPreviewsRequest request;
        if (pagingState.isPresent()) {
            request = GetUserVideoPreviewsRequest
                    .newBuilder()
                    .setUserId(GrpcMapper.uuidToUuid(testDatasetUsers.get(user)))
                    .setPagingState(pagingState.get())
                    .setPageSize(2)
                    .build();

        } else {
            request = GetUserVideoPreviewsRequest
                    .newBuilder()
                    .setUserId(GrpcMapper.uuidToUuid(testDatasetUsers.get(user)))
                    .setPageSize(2)
                    .build();
        }

        final GetUserVideoPreviewsResponse response = grpcClient.getVideoCatalogService().getUserVideoPreviews(request);

        assertThat(response)
                .as("Cannot fetch %s videos with fetch size == 2", user)
                .isNotNull();

        assertThat(response.getVideoPreviewsList())
                .as("Cannot fetch %s videos with fetch size == 2", user)
                .hasSize(2);

        assertThat(response.getPagingState())
                .as("There is no %s videos remaining for next page", user)
                .isNotEmpty();

        return Optional.of(response.getPagingState());
    }
    
}
