package com.killrvideo.grpc.utils;

import static org.apache.commons.lang3.StringUtils.isBlank;

import org.slf4j.Logger;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import com.killrvideo.dse.model.Video;

import io.grpc.stub.StreamObserver;
import killrvideo.search.SearchServiceOuterClass.GetQuerySuggestionsRequest;
import killrvideo.search.SearchServiceOuterClass.SearchResultsVideoPreview;
import killrvideo.search.SearchServiceOuterClass.SearchResultsVideoPreview.Builder;
import killrvideo.search.SearchServiceOuterClass.SearchVideosRequest;

/**
 * Helper and mappers for DAO <=> GRPC Communications
 *
 * @author DataStax Evangelist Team
 */
@Component
public class SearchGrpcHelper extends AbstractGrpcHelper {
    
    public void validateGrpcRequest_SearchVideos(Logger logger, SearchVideosRequest request, StreamObserver<?> streamObserver) {
        final StringBuilder errorMessage = initErrorString(request);
        boolean isValid = true;
        if (isBlank(request.getQuery())) {
            errorMessage.append("\t\tquery string should be provided for search videos request\n");
            isValid = false;
        }
        if (request.getPageSize() <= 0) {
            errorMessage.append("\t\tpage size should be strictly positive for search videos request\n");
            isValid = false;
        }
        Assert.isTrue(validate(logger, streamObserver, errorMessage, isValid), "Invalid parameter for 'searchVideos'");
    }
    
    public void validateGrpcRequest_GetQuerySuggestions(Logger logger, GetQuerySuggestionsRequest request, StreamObserver<?> streamObserver) {
        final StringBuilder errorMessage = initErrorString(request);
        boolean isValid = true;
        if (isBlank(request.getQuery())) {
            errorMessage.append("\t\tquery string should be provided for get video suggestions request\n");
            isValid = false;
        }
        if (request.getPageSize() <= 0) {
            errorMessage.append("\t\tpage size should be strictly positive for get video suggestions request\n");
            isValid = false;
        }
        Assert.isTrue(validate(logger, streamObserver, errorMessage, isValid), 
                "Invalid parameter for 'getQuerySuggestions'");
    }
    
    /**
     * Mapping to generated GPRC beans (Search result special).
     */
    public SearchResultsVideoPreview maptoResultVideoPreview(Video v) {
        Builder builder = SearchResultsVideoPreview.newBuilder();
        builder.setName(v.getName());
        builder.setVideoId(GrpcMapper.uuidToUuid(v.getVideoid()));
        builder.setUserId(GrpcMapper.uuidToUuid(v.getUserid()));
        if (v.getPreviewImageLocation() != null)  {
            builder.setPreviewImageLocation(v.getPreviewImageLocation());
        }
        if (v.getAddedDate() != null)  {
            builder.setAddedDate(GrpcMapper.dateToTimestamp(v.getAddedDate()));
        }
        return builder.build();
    }

    
}
