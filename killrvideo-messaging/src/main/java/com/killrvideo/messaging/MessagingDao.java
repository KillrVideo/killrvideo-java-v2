package com.killrvideo.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.google.common.eventbus.EventBus;
import com.killrvideo.core.error.ErrorEvent;

/**
 * Wrapping any kind of messages.
 *
 * @author DataStax evangelist team.
 */
@Repository
public class MessagingDao {
    
    /** Loger for that class. */
    private static Logger LOGGER = LoggerFactory.getLogger(MessagingDao.class);
    
    @Autowired
    private EventBus eventBus;
    
    /**
     * Publish error Message.
     *
     * @param request
     *      current request to serialize
     * @param error
     *      raised exception during treatment
     */
    public void publishExceptionEvent(Object request, Throwable error) {
        LOGGER.error("Exception commenting on video {} }", error);
        eventBus.post(new ErrorEvent(request, error));
    }
    
    /**
     * Publish nessage to BUS.
     * 
     * @param obj
     *      current object
     */
    public void publishEvent(Object obj) {
        eventBus.post(obj);
    }
    
    /**
     * Register class to BUS.
     * 
     * @param obj
     *      current object
     */
    public void register(Object o) {
        eventBus.register(o);
    }
    
    /**
     * UnRegister class to BUS.
     * 
     * @param obj
     *      current object
     */
    public void unRegister(Object o) {
        eventBus.unregister(o);
    }
    
}
