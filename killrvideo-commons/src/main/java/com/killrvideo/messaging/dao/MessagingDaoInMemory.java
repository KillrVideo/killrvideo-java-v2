package com.killrvideo.messaging.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.google.common.eventbus.EventBus;

/**
 * Wrapping any kind of messages.
 *
 * @author DataStax Developer Advocates team.
 */
@Repository("killrvideo.dao.messaging.memory")
public class MessagingDaoInMemory implements MessagingDao {
    
    /** Loger for that class. */
    private static Logger LOGGER = LoggerFactory.getLogger(MessagingDaoInMemory.class);
    
    @Autowired
    private EventBus eventBus;
    
    /** {@inheritDoc} */
    @Override
    public void sendEvent(String targetDestination, Object event) {
        if (event != null) {
            LOGGER.info("Publishing eventtype{} to destination {} ", event.getClass().getName(), targetDestination);
            eventBus.post(event);
        }
    }

    @Override
    public String getErrorDestination() {
        return "ERROR";
    }   
    
}
