package com.killrvideo.grpc.services;

import java.time.Instant;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.killrvideo.core.error.ErrorEvent;
import com.killrvideo.dse.dao.SearchDseDao;
import com.killrvideo.dse.dao.dto.ResultListPage;
import com.killrvideo.dse.model.Video;
import com.killrvideo.grpc.utils.SearchGrpcHelper;
import com.killrvideo.messaging.MessagingDao;

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
 * @author DataStax Evangelist Team
 */
@Service
public class SearchGrpcService extends SearchServiceImplBase {

    /** Loger for that class. */
    private static Logger LOGGER = LoggerFactory.getLogger(RatingsGrpcService.class);
    
    @Autowired
    private SearchDseDao dseSearchDao;
    
    @Autowired
    private MessagingDao messagingDao;
    
    @Autowired
    private SearchGrpcHelper helper;
    
    /** {@inheritDoc} */
    @Override
    public void searchVideos(SearchVideosRequest grpcReq, StreamObserver<SearchVideosResponse> grpcResObserver) {
        
        // Validate Parameters
        helper.validateGrpcRequest_SearchVideos(LOGGER, grpcReq, grpcResObserver);
        
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
              helper.traceSuccess(LOGGER, "searchVideos", starts);
              final SearchVideosResponse.Builder builder = SearchVideosResponse.newBuilder();
              builder.setQuery(grpcReq.getQuery());
              resultPage.getPagingState().ifPresent(builder::setPagingState);
              resultPage.getResults().stream()
                        .map(helper::maptoResultVideoPreview)
                        .forEach(builder::addVideos);
              grpcResObserver.onNext(builder.build());
              grpcResObserver.onCompleted();
              
           } else {
               
              helper.traceError(LOGGER, "commentOnVideo", starts, error);
              messagingDao.publishExceptionEvent(new ErrorEvent(grpcReq, error), error);
              grpcResObserver.onError(Status.INTERNAL.withCause(error).asRuntimeException());
           }
        });
    }

    /** {@inheritDoc} */
    @Override
    public void getQuerySuggestions(GetQuerySuggestionsRequest grpcReq, StreamObserver<GetQuerySuggestionsResponse> grpcResObserver) {
        
        // Validate Parameters
        helper.validateGrpcRequest_GetQuerySuggestions(LOGGER, grpcReq, grpcResObserver);
        
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
              
              helper.traceSuccess(LOGGER, "getQuerySuggestions", starts);
              final GetQuerySuggestionsResponse.Builder builder = GetQuerySuggestionsResponse.newBuilder();
              builder.setQuery(grpcReq.getQuery());
              builder.addAllSuggestions(suggestionSet);
              grpcResObserver.onNext(builder.build());
              grpcResObserver.onCompleted();
          } else {
              
              helper.traceError(LOGGER, "commentOnVideo", starts, error);
              messagingDao.publishExceptionEvent(new ErrorEvent(grpcReq, error), error);
              grpcResObserver.onError(Status.INTERNAL.withCause(error).asRuntimeException());
          }             
        });
    }

}