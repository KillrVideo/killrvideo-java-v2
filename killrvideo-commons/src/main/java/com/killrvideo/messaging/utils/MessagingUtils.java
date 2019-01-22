package com.killrvideo.messaging.utils;

import java.util.Arrays;
import java.util.StringJoiner;

import com.google.protobuf.Timestamp;

import killrvideo.common.CommonEvents.ErrorEvent;

/**
 * Utility class for messaging
 *
 * @author Cedrick LUNVEN (@clunven)
 */
public class MessagingUtils {
    
    /**
     * Hide constructor
     */
    private MessagingUtils() {}
     
    /**
     * Creation GRPC CUSTOMN EXCEPTION
     *
     * @param t
     *      grpc exception
     * @return
     */
    public static ErrorEvent mapError(Throwable t) {
        return killrvideo.common.CommonEvents.ErrorEvent.newBuilder()
                .setErrorMessage(t.getMessage())
                .setErrorClassname(t.getClass().getName())
                .setErrorStack(mergeStackTrace(t))
                .setErrorTimestamp(Timestamp.newBuilder())
                .build();
    }
    
    public static ErrorEvent mapCustomError(String customError) {
        return killrvideo.common.CommonEvents.ErrorEvent.newBuilder()
                .setErrorMessage(customError)
                .setErrorClassname(Exception.class.getName())
                .setErrorTimestamp(Timestamp.newBuilder())
                .build();
    }
    
    /**
     * Dump a stacktrace in a String,
     * 
     * @param throwable
     *      current exception raised by the program
     * @return
     *      merged stack trace.
     */
    private static final String mergeStackTrace(Throwable throwable) {
        StringJoiner joiner = new StringJoiner("\n\t", "\n", "\n");
        joiner.add(throwable.getMessage());
        Arrays.asList(throwable.getStackTrace()).forEach(stackTraceElement -> joiner.add(stackTraceElement.toString()));
        return joiner.toString();
    }
}
