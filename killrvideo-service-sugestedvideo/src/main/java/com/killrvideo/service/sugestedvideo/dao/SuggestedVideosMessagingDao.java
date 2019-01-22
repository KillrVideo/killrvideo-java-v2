package com.killrvideo.service.sugestedvideo.dao;

import static com.killrvideo.service.sugestedvideo.grpc.SuggestedVideosServiceGrpcMapper.mapVideoAddedtoVideoDTO;

import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.UUID;
import java.util.stream.StreamSupport;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import com.google.protobuf.InvalidProtocolBufferException;
import com.killrvideo.utils.GrpcMappingUtils;

import killrvideo.ratings.events.RatingsEvents.UserRatedVideo;
import killrvideo.user_management.events.UserManagementEvents.UserCreated;
import killrvideo.video_catalog.events.VideoCatalogEvents.YouTubeVideoAdded;

@Repository("killrvideo.rating.dao.messaging")
public class SuggestedVideosMessagingDao {
    
    /** Loger for that class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SuggestedVideosMessagingDao.class);
    
    @Autowired
    private SuggestedVideosDseDao sugestedVideoDseDao;
    
    // --- Video Rating ---
    
    @Value("${killrvideo.messaging.kafka.topics.videoRated : topic-kv-videoRating}")
    private String topicVideoRated;
    
    @Autowired
    @Qualifier("kafka.consumer.videoRating")
    private KafkaConsumer<String, byte[]> consumerVideoRatingProtobuf;
   
    // --- User Creation ---
    
    @Value("${killrvideo.messaging.kafka.topics.userCreated : topic-kv-userCreation}")
    private String topicUserCreated;
    
    @Autowired
    @Qualifier("kafka.consumer.userCreating")
    private KafkaConsumer<String, byte[]> consumerUserCreatedProtobuf;

    // -- Video Creation ---
    
    @Value("${killrvideo.messaging.kafka.topics.youTubeVideoAdded : topic-kv-videoCreation}")
    private String topicVideoCreated;
    
    @Autowired
    @Qualifier("kafka.consumer.videoCreating")
    private KafkaConsumer<String, byte[]> consumerVideoCreatedProtobuf;
    
    @PostConstruct
    public void registerConsumerVideoRating() {
        LOGGER.info("Start consuming events from topic '{}' ..", topicVideoRated);
        consumerVideoRatingProtobuf.subscribe(Collections.singletonList(topicVideoRated));
        StreamSupport.stream(consumerVideoRatingProtobuf.poll(Duration.ofSeconds(2L)).spliterator(), false)
                     .map(ConsumerRecord::value)
                     .forEach(this::onVideoRatingMessage);
    }
    
    @PostConstruct
    public void registerConsumerUserCreated() {
        LOGGER.info("Start consuming events from topic '{}' ..", topicUserCreated);
        consumerUserCreatedProtobuf.subscribe(Collections.singletonList(topicUserCreated));
        StreamSupport.stream(consumerUserCreatedProtobuf.poll(Duration.ofSeconds(2L)).spliterator(), false)
                     .map(ConsumerRecord::value)
                     .forEach(this::onUserCreatedMessage);
    }
    
    @PostConstruct
    public void registerConsumerYoutubeVideoAdded() {
        LOGGER.info("Start consuming events from topic '{}' ..", topicVideoCreated);
        consumerVideoCreatedProtobuf.subscribe(Collections.singletonList(topicVideoCreated));
        StreamSupport.stream(consumerVideoCreatedProtobuf.poll(Duration.ofSeconds(2L)).spliterator(), false)
                     .map(ConsumerRecord::value)
                     .forEach(this::onYoutubeVideoAddedMessage);
    }
    
    public void onVideoRatingMessage(byte[] payload) {
        try {
            UserRatedVideo userVideoRated = UserRatedVideo.parseFrom(payload);
            String videoId = userVideoRated.getVideoId().getValue();
            UUID   userId  = UUID.fromString(userVideoRated.getUserId().getValue());
            int rating     = userVideoRated.getRating();
            sugestedVideoDseDao.updateGraphNewUserRating(videoId, userId, rating);
        } catch (InvalidProtocolBufferException e) {
            LOGGER.error("Cannot parse message expecting object " + UserCreated.class.getName(), e);
        }
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
    public void onUserCreatedMessage(byte[] payload) {
        try {
            // Parse binary from
            UserCreated userCreationMessage = UserCreated.parseFrom(payload);
            final UUID userId       = UUID.fromString(userCreationMessage.getUserId().getValue());
            final Date userCreation = GrpcMappingUtils.timestampToDate(userCreationMessage.getTimestamp());
            final String email      = userCreationMessage.getEmail();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[NewUserEvent] Processing for user {} ", userId);
            }
            sugestedVideoDseDao.updateGraphNewUser(userId, email, userCreation);
        } catch (InvalidProtocolBufferException e) {
            LOGGER.error("Cannot parse message expecting object " + UserCreated.class.getName(), e);
        }
    }
    
    /**
     * Processing incoming message.
     */
    public void onYoutubeVideoAddedMessage(byte[] payload) {
        try {
            // Marshall binary to Protobuf Stub
            YouTubeVideoAdded videoAdded = YouTubeVideoAdded.parseFrom(payload);
            // Convert Stub to Dto, dao must not be related to interface GRPC
            sugestedVideoDseDao.updateGraphNewVideo(mapVideoAddedtoVideoDTO(videoAdded));
        } catch (InvalidProtocolBufferException e) {
            LOGGER.error("Cannot parse message expecting object " + UserCreated.class.getName(), e);
        }   
    }
}
