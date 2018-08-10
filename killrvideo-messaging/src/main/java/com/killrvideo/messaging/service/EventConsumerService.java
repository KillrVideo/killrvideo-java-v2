package com.killrvideo.messaging.service;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.eventbus.Subscribe;
import com.killrvideo.dse.dao.SuggestedVideosDseDao;
import com.killrvideo.dse.model.User;
import com.killrvideo.dse.model.Video;
import com.killrvideo.dse.model.VideoRatingByUser;
import com.killrvideo.messaging.MessagingDao;

@Service
public class EventConsumerService {
    
    /** Logger for DAO. */
    private static final Logger LOGGER = LoggerFactory.getLogger(EventConsumerService.class);

    @Autowired
    private SuggestedVideosDseDao suggestedVideoDao;
    
    @Autowired
    private MessagingDao messagingDao;
    
    @PostConstruct
    public void startListening() {
        messagingDao.register(this);
    }
    
    /**
     * Subscription on guava BUS.
     *
     * @param youTubeVideoAdded
     *      new video added
     */
    @Subscribe
    public void onYouTubeVideoCreation(Video youTubeVideoAdded) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[NewVideoEvent] Processing for video {} ", youTubeVideoAdded.getVideoid());
        }
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
    public void onUserCreation(User userAdded) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[NewUserEvent] Processing for user {} ", userAdded.getUserid());
        }
        LOGGER.debug("New user event: Updating Recommendation Graph");
        suggestedVideoDao.updateGraphNewUser(userAdded);
    }
    
    @Subscribe
    public void onVideoRating(VideoRatingByUser videoRating) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[NewRatingEvent] Processing video {} user {} rating {}", 
                    videoRating.getVideoid(), videoRating.getUserid(), videoRating.getRating());
        }
        suggestedVideoDao.updateGraphNewUserRating(videoRating);
    }
}
