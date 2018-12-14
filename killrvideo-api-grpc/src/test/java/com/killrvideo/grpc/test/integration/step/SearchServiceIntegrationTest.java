package com.killrvideo.grpc.test.integration.step;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evanlennick.retry4j.CallExecutor;
import com.evanlennick.retry4j.config.RetryConfig;
import com.evanlennick.retry4j.config.RetryConfigBuilder;

import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.Then;
import killrvideo.search.SearchServiceOuterClass.GetQuerySuggestionsRequest;
import killrvideo.search.SearchServiceOuterClass.GetQuerySuggestionsResponse;
import killrvideo.search.SearchServiceOuterClass.SearchResultsVideoPreview;
import killrvideo.search.SearchServiceOuterClass.SearchVideosRequest;
import killrvideo.search.SearchServiceOuterClass.SearchVideosResponse;

/**
 * Allow to test Search Services.
 * <ol>
 *  <li>
 * @author DataStax evangelist team.
 */
public class SearchServiceIntegrationTest extends AbstractSteps {

    /** Logger for Test.*/
    private static Logger LOGGER = LoggerFactory.getLogger(SearchServiceIntegrationTest.class);
    
    /** Boolean to keep track of testing services. */
    private static AtomicReference<Boolean> SHOULD_CHECK_SERVICE = new AtomicReference<>(true);

    @Before("@search_scenarios")
    public void init() {
        if (SHOULD_CHECK_SERVICE.get()) {
            etcdDao.read("/killrvideo/services/" + SEARCH_SERVICE_NAME, true);
        }
        
        LOGGER.info("Truncating users & videos BEFORE executing tests");
        cleanUpUserAndVideoTables();
    }

    @After("@search_scenarios")
    public void cleanup() {
        LOGGER.info("Truncating users & videos tables AFTER executing tests");
        cleanUpUserAndVideoTables();
    }

    @Then("searching videos with tag (.+) gives: (.*)")
    public void searchVideosByTag(String tag, List<String> expectedVideos) {
        // Checking parameters
        final int expectedVideoCount = expectedVideos.size();
        assertThat(VIDEOS)
                .as("%s is unknown, please specify videoXXX where XXX is a digit")
                .containsKeys(expectedVideos.toArray(new String[expectedVideoCount]));
        assertThat(tag)
                .as("A non-empty tag should be provided for video searching")
                .isNotEmpty();
        
        SearchVideosRequest request = SearchVideosRequest
                .newBuilder()
                .setQuery(tag)
                .setPageSize(100).build();

        /**
         * Search is performed using solr INDEX. As such, a delay is required for indexing.
         * Maximum delay of 10s is OK but here we try 10 times, once per second
         */
        Callable<Integer> searchTags = () -> {
            System.out.println("[Waiting for solr to index tags]");
            return grpcClient.getSearchService().searchVideos(request).getVideosList().size();
        };
        RetryConfig config = new RetryConfigBuilder()
                .retryOnReturnValue(Integer.valueOf(0))
                .failOnAnyException()
                .withMaxNumberOfTries(10)
                .withDelayBetweenTries(2, ChronoUnit.SECONDS)
                .withFixedBackoff()
                .build();
        new CallExecutor<Integer>(config).execute(searchTags);

        final SearchVideosResponse response = grpcClient.getSearchService().searchVideos(request);
        
        assertThat(response).as("Find 0 video with tag %s", tag).isNotNull();
        assertThat(response.getVideosList())
                .as("There should be %s videos having tag %s", expectedVideoCount, tag)
                .hasSize(expectedVideoCount);
        assertThat(response.getVideosList().stream()
                    .map(SearchResultsVideoPreview::getVideoId)
                    .map(x -> VideoCatalogServiceSteps.VIDEOS_BY_ID.get(UUID.fromString(x.getValue())))
                    .collect(Collectors.toList()))
                .as("Found videos with tag %s do not match %s", tag, String.join(", ", expectedVideos))
                .containsAll(expectedVideos);
    }
    
    @Then("^I should be suggested tags (.*) for the word (.+)$")
    public void getTagsSuggestion(List<String> expectedTags, String word) {

        assertThat(expectedTags)
                .as("Please provide expected tags for word %s", word)
                .isNotEmpty();

        assertThat(word)
                .as("Cannot get tags suggestion for empty word")
                .isNotEmpty();

        GetQuerySuggestionsRequest request = GetQuerySuggestionsRequest
                .newBuilder()
                .setQuery(word)
                .setPageSize(100)
                .build();

        /**
         * Search is performed using solr INDEX. As such, a delay is required for indexing.
         * Maximum delay of 10s is OK but here we try 10 times, once per second
         */
        Callable<Integer> searchSuggestions = () -> {
            System.out.println("[Waiting for solr to index tags]");
            return grpcClient.getSearchService().getQuerySuggestions(request).getSuggestionsList().size();
        };
        RetryConfig config = new RetryConfigBuilder()
                .retryOnReturnValue(Integer.valueOf(0))
                .failOnAnyException()
                .withMaxNumberOfTries(10)
                .withDelayBetweenTries(2, ChronoUnit.SECONDS)
                .withFixedBackoff()
                .build();
        new CallExecutor<Integer>(config).execute(searchSuggestions);
        
        final GetQuerySuggestionsResponse response = grpcClient.getSearchService().getQuerySuggestions(request);

        assertThat(response)
                .as("Cannot find tags suggestions for word %s", word)
                .isNotNull();

        final List<String> suggestionsList = response.getSuggestionsList();
        
        assertThat(suggestionsList)
                .as("Cannot find tags suggestions for word %s", word)
                .isNotEmpty();

        assertThat(suggestionsList)
                .as("The suggested tags %s do not match the expected tags %s",
                        String.join(", ", suggestionsList),
                        String.join(", ", expectedTags))
                .containsExactly(expectedTags.toArray(new String[expectedTags.size()]));
    }
}
