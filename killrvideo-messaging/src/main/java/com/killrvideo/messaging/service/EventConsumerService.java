package com.killrvideo.messaging.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.eventbus.Subscribe;
import com.killrvideo.dse.dao.SuggestedVideosDseDao;
import com.killrvideo.dse.model.User;
import com.killrvideo.dse.model.Video;
import com.killrvideo.dse.model.VideoRatingByUser;

@Service
public class EventConsumerService {
    
    /** Logger for DAO. */
    private static final Logger LOGGER = LoggerFactory.getLogger(EventConsumerService.class);

    @Autowired
    private SuggestedVideosDseDao suggestedVideoDao;
    
    /**
     * Subscription on guava BUS.
     *
     * @param youTubeVideoAdded
     *      new video added
     */
    @Subscribe
    public void consumeNewVideoEvent(Video youTubeVideoAdded) {
        LOGGER.debug("New video event: Updating Recommendation Graph");
        suggestedVideoDao.updateGraphNewVideo(youTubeVideoAdded);
    }

    /**
     * Subscription on guava BUS.
     * Make @Subscribe subscriber magic happen anytime a user is created from
     * UserManagementService.createUser() with a call to eventBus.post().
     * We use this to create entries in our graph database for use with our
     * SuggestedVideos recommendation service which is why this exists here.
     * 
     * @param userAdded
     *      new user added
     */
    @Subscribe
    public void consumeNewUserEvent(User userAdded) {
        LOGGER.debug("New user event: Updating Recommendation Graph");
        suggestedVideoDao.updateGraphNewUser(userAdded);
        
    }
    
    @Subscribe
    public void consumeNewVideoRatingEvent(VideoRatingByUser videoRating) {
        LOGGER.debug("New vide rating event: Updating Recommendation Graph");
        suggestedVideoDao.updateGraphNewUserRating(videoRating);
        
    }
}
