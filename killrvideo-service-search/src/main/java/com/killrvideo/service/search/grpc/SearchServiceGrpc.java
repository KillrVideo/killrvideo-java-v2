package com.killrvideo.service.search.grpc;

import static com.killrvideo.service.search.grpc.SearchServiceGrpcValidator.validateGrpcRequest_GetQuerySuggestions;
import static com.killrvideo.service.search.grpc.SearchServiceGrpcValidator.validateGrpcRequest_SearchVideos;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.killrvideo.dse.dto.ResultListPage;
import com.killrvideo.dse.dto.Video;
import com.killrvideo.messaging.dao.MessagingDaoInMemory;
import com.killrvideo.service.search.dao.SearchDseDao;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import killrvideo.search.SearchServiceGrpc.SearchServiceImplBase;
import killrvideo.search.SearchServiceOuterClass.GetQuerySuggestionsRequest;
import killrvideo.search.SearchServiceOuterClass.GetQuerySuggestionsResponse;
import killrvideo.search.SearchServiceOuterClass.SearchVideosRequest;
import killrvideo.search.SearchServiceOuterClass.SearchVideosResponse;

/**
 * Service SEARCG.
 *
 * @author DataStax advocates Team
 */
@Service
public class SearchServiceGrpc extends SearchServiceImplBase {

    /** Loger for that class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SearchServiceGrpc.class);
    
    /** Identifier for search service. */
    public static final String SEARCH_SERVICE_NAME = "SearchService";
    
    @Autowired
    private SearchDseDao dseSearchDao;
    
    @Autowired
    private MessagingDaoInMemory messagingDao;
   
    /** {@inheritDoc} */
    @Override
    public void searchVideos(SearchVideosRequest grpcReq, StreamObserver<SearchVideosResponse> grpcResObserver) {
        
        // Validate Parameters
        validateGrpcRequest_SearchVideos(LOGGER, grpcReq, grpcResObserver);
        
        // Stands as stopwatch for logging and messaging 
        final Instant starts = Instant.now();
        
        // Mapping GRPC => Domain (Dao)
        String           searchQuery = grpcReq.getQuery();
        int              searchPageSize = grpcReq.getPageSize();
        Optional<String> searchPagingState = Optional.ofNullable(grpcReq.getPagingState()).filter(StringUtils::isNotBlank);
        
        // Invoke DAO Async
        CompletableFuture<ResultListPage<Video>> futureDao = 
                dseSearchDao.searchVideosAsync(searchQuery, searchPageSize,searchPagingState);
        
        // Map Result back to GRPC
        futureDao.whenComplete((resultPage, error) -> {
          if (error == null) {
              traceSuccess("searchVideos", starts);
              final SearchVideosResponse.Builder builder = SearchVideosResponse.newBuilder();
              builder.setQuery(grpcReq.getQuery());
              resultPage.getPagingState().ifPresent(builder::setPagingState);
              resultPage.getResults().stream()
                        .map(SearchServiceGrpcMapper::maptoResultVideoPreview)
                        .forEach(builder::addVideos);
              grpcResObserver.onNext(builder.build());
              grpcResObserver.onCompleted();
              
           } else {
              traceError("searchVideos", starts, error);
              messagingDao.sendErrorEvent(SEARCH_SERVICE_NAME, error);
              grpcResObserver.onError(Status.INTERNAL.withCause(error).asRuntimeException());
           }
        });
    }

    /** {@inheritDoc} */
    @Override
    public void getQuerySuggestions(GetQuerySuggestionsRequest grpcReq, StreamObserver<GetQuerySuggestionsResponse> grpcResObserver) {
        
        // Validate Parameters
        validateGrpcRequest_GetQuerySuggestions(LOGGER, grpcReq, grpcResObserver);
        
        // Stands as stopwatch for logging and messaging 
        final Instant starts = Instant.now();
        
        // Mapping GRPC => Domain (Dao)
        String           searchQuery = grpcReq.getQuery();
        int              searchPageSize = grpcReq.getPageSize();
        
        // Invoke Dao (Async)
        CompletableFuture<TreeSet<String>> futureDao = 
                dseSearchDao.getQuerySuggestionsAsync(searchQuery, searchPageSize);
        
        // Mapping back to GRPC beans
        futureDao.whenComplete((suggestionSet, error) -> {
                        
          if (error == null) {
              traceSuccess("getQuerySuggestions", starts);
              final GetQuerySuggestionsResponse.Builder builder = GetQuerySuggestionsResponse.newBuilder();
              builder.setQuery(grpcReq.getQuery());
              builder.addAllSuggestions(suggestionSet);
              grpcResObserver.onNext(builder.build());
              grpcResObserver.onCompleted();
          } else {
              traceError("getQuerySuggestions", starts, error);
              messagingDao.sendErrorEvent(SEARCH_SERVICE_NAME, error);
              grpcResObserver.onError(Status.INTERNAL.withCause(error).asRuntimeException());
          }             
        });
    }
    
    /**
     * Utility to TRACE.
     *
     * @param method
     *      current operation
     * @param start
     *      timestamp for starting
     */
    private void traceSuccess(String method, Instant starts) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("End successfully '{}' in {} millis", method, Duration.between(starts, Instant.now()).getNano()/1000);
        }
    }
    
    /**
     * Utility to TRACE.
     *
     * @param method
     *      current operation
     * @param start
     *      timestamp for starting
     */
    private void traceError(String method, Instant starts, Throwable t) {
        LOGGER.error("An error occured in {} after {}", method, Duration.between(starts, Instant.now()), t);
    }

}