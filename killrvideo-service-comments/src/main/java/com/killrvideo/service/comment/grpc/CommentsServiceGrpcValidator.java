package com.killrvideo.service.comment.grpc;

import static org.apache.commons.lang3.StringUtils.isBlank;

import org.slf4j.Logger;
import org.springframework.util.Assert;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import killrvideo.comments.CommentsServiceOuterClass.CommentOnVideoRequest;
import killrvideo.comments.CommentsServiceOuterClass.GetUserCommentsRequest;
import killrvideo.comments.CommentsServiceOuterClass.GetVideoCommentsRequest;

/**
 * GRPC Requests Validation Utility class : Implements controls before use request and throw
 * errors if parameter are invalid.
 *
 * @author DataStax Developer Advocates team.
 */
public class CommentsServiceGrpcValidator {
    
    /**
     * Hide constructor.
     */
    private CommentsServiceGrpcValidator() {
    }
    
    /**
     * Validate comment On video comment query.
     * 
     * @param request
     *      current GRPC Request
     * @param streamObserver
     *      response async
     * @return
     *      true if the query is valid
     */
    public static void validateGrpcRequestCommentOnVideo(Logger logger, CommentOnVideoRequest request, StreamObserver<?> streamObserver) {
        StringBuilder errorMessage = initErrorString(request);
        boolean isValid = 
                  notEmpty(!request.hasUserId()    || isBlank(request.getUserId().getValue()),  "userId",  "video request",errorMessage) &&
                  notEmpty(!request.hasVideoId()   || isBlank(request.getVideoId().getValue()), "videoId", "video request",errorMessage) &&
                  notEmpty(!request.hasCommentId() || isBlank(request.getCommentId().getValue()), "commentId", "video request",errorMessage) &&
                  notEmpty(isBlank(request.getComment()), "comment", "video request",errorMessage);
        Assert.isTrue(validate(logger, streamObserver, errorMessage, isValid), "Invalid parameter for 'commentOnVideo'");
    }
    
    /**
     * Validate get video comment query.
     * 
     * @param request
     *      current GRPC Request
     * @param streamObserver
     *      response async
     * @return
     *      true if the query is valid
     */
    public static void validateGrpcRequestGetVideoComment(Logger logger, GetVideoCommentsRequest request, StreamObserver<?> streamObserver) {
        final StringBuilder errorMessage = initErrorString(request);
        boolean isValid = true;
        if (!request.hasVideoId() || isBlank(request.getVideoId().getValue())) {
            errorMessage.append("\t\tvideo id should be provided for get video comment request\n");
            isValid = false;
        }
        if (request.getPageSize() <= 0) {
            errorMessage.append("\t\tpage size should be strictly positive for get video comment request");
            isValid = false;
        }
        Assert.isTrue(validate(logger, streamObserver, errorMessage, isValid), "Invalid parameter for 'getVideoComments'");
    }
    
    /**
     * Validate get user comment query.
     * 
     * @param request
     *      current GRPC Request
     * @param streamObserver
     *      response async
     * @return
     *      true if the query is valid
     */
    public static void validateGrpcRequest_GetUserComments(Logger logger, GetUserCommentsRequest request, StreamObserver<?> streamObserver) {
        final StringBuilder errorMessage = initErrorString(request);
        boolean isValid = 
                  notEmpty(!request.hasUserId() || isBlank(request.getUserId().getValue()),  "userId",  "comment request",errorMessage) &&
                  positive(request.getPageSize() <= 0,  "page size",  "comment request",errorMessage);
        Assert.isTrue(validate(logger, streamObserver, errorMessage, isValid), "Invalid parameter for 'getUserComments'");
    }
    
    /**
     * Init error builder.
     *  
     * @param request
     *      current request
     * @return
     *      current error message
     */
    protected static StringBuilder initErrorString(Object request) {
        return new StringBuilder("Validation error for '" + request.toString() + "' : \n");
    }
    
   /**
    * Deduplicate condition evaluation.
    *
    * @param assertion
    *      current condition
    * @param fieldName
    *      fieldName to evaluate
    * @param request
    *      GRPC reauest
    * @param errorMessage
    *      concatenation of error messages
    * @return
    */
    protected static boolean notEmpty(boolean assertion, String fieldName, String request, StringBuilder errorMessage) {
       if (assertion) {
           errorMessage.append("\t\t");
           errorMessage.append(fieldName);
           errorMessage.append("should be provided for comment on ");
           errorMessage.append(request);
           errorMessage.append("\n");
       }
       return !assertion;
   }
   
    /**
     * Add error message if assertion is violated.
     * 
     * @param assertion
     *      current assertion
     * @param fieldName
     *      current field name
     * @param request
     *      current request
     * @param errorMessage
     *      current error message
     * @return
     *      if the correction is OK.
     */
    protected static boolean positive(boolean assertion, String fieldName, String request, StringBuilder errorMessage) {
        if (assertion) {
            errorMessage.append("\t\t");
            errorMessage.append(fieldName);
            errorMessage.append("should be strictly positive for ");
            errorMessage.append(request);
            errorMessage.append("\n");
        }
        return !assertion;
    }

    /**
     * Utility to validate Grpc Input.
     *
     * @param streamObserver
     *      grpc observer
     * @param errorMessage
     *      error mressage
     * @param isValid
     *      validation of that
     * @return
     *      ok
     */
    protected static boolean validate(Logger logger, StreamObserver<?> streamObserver, StringBuilder errorMessage, boolean isValid) {
        if (isValid) {
            return true;
        } else {
            final String description = errorMessage.toString();
            logger.error(description);
            streamObserver.onError(Status.INVALID_ARGUMENT.withDescription(description).asRuntimeException());
            streamObserver.onCompleted();
            return false;
        }
    }
    
}
