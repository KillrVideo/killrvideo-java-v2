package com.killrvideo.messaging.dao;

import static com.killrvideo.messaging.utils.MessagingUtils.mapCustomError;
import static com.killrvideo.messaging.utils.MessagingUtils.mapError;

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
    void sendEvent(String targetDestination, Object event);
    
    /** Channel to send errors. */
    String getErrorDestination();
    
    /**
     * Send errors to bus.
     * 
     * @param serviceName
     * @param param
     * @param t
     */
    default void sendErrorEvent(String serviceName, Throwable t) {
        sendEvent(getErrorDestination(), mapError(t));
    }
   
    /**
     * Send error event to bus.
     */
    default void sendErrorEvent(String serviceName, String customError) {
        sendEvent(getErrorDestination(), mapCustomError(customError));
    }
    
}
