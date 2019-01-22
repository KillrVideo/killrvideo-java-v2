package com.killrvideo.utils;

import org.slf4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class ValidationUtils {
    
    /**
     * Init error builder.
     *  
     * @param request
     *      current request
     * @return
     *      current error message
     */
    public static StringBuilder initErrorString(Object request) {
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
    public static boolean notEmpty(boolean assertion, String fieldName, String request, StringBuilder errorMessage) {
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
    public static boolean positive(boolean assertion, String fieldName, String request, StringBuilder errorMessage) {
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
    public static boolean validate(Logger logger, StreamObserver<?> streamObserver, StringBuilder errorMessage, boolean isValid) {
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
