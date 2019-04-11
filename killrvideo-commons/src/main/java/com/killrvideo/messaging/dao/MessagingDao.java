package com.killrvideo.messaging.dao;

import static com.killrvideo.messaging.utils.MessagingUtils.mapCustomError;
import static com.killrvideo.messaging.utils.MessagingUtils.mapError;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Interface to work with Events.
 *
 * @author Cedrick LUNVEN (@clunven)
 */
public interface MessagingDao {
    
    /**
     * Will send an event to target destination.
     *
     * @param targetDestination
     *           adress of destination : queue, topic, shared memory (className).
     * @param event
     *          event serialized as binary
     */
    CompletableFuture<Object> sendEvent(String targetDestination, Object event);
    
    /** 
     * Channel to send errors.
     */
    String getErrorDestination();
    
    /**
     * Send errors to bus.
     * 
     * @param serviceName
     * @param param
     * @param t
     */
    default CompletableFuture<Object> sendErrorEvent(String serviceName, Throwable t) {
        return sendEvent(getErrorDestination(), mapError(t));
    }
   
    /**
     * Send error event to bus.
     */
    default Future<? extends Object> sendErrorEvent(String serviceName, String customError) {
        return sendEvent(getErrorDestination(), mapCustomError(customError));
    }
    
}
