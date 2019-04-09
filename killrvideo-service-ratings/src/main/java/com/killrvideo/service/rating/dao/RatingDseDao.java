package com.killrvideo.service.rating.dao;

import static com.datastax.driver.core.querybuilder.QueryBuilder.update;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.mapping.Mapper;
import com.killrvideo.dse.dao.DseDaoSupport;
import com.killrvideo.service.rating.dto.VideoRating;
import com.killrvideo.service.rating.dto.VideoRatingByUser;
import com.killrvideo.utils.FutureUtils;

/**
 * Implementations of operation for Videos.
 *
 * @author DataStax Developer Advocates team.
 */
@Repository("killrvideo.rating.dao.dse")
public class RatingDseDao extends DseDaoSupport {

	/** Logger for that class. */
    private static Logger LOGGER = LoggerFactory.getLogger(RatingDseDao.class);
    
    /** Dse Data Model concerns. */
    public static final String TABLENAME_VIDEOS_RATINGS         = "video_ratings";
    public static final String TABLENAME_VIDEOS_RATINGS_BYUSER  = "video_ratings_by_user";
   
    /** Mapper to ease queries. */
    protected Mapper < VideoRating >       mapperVideoRating;
    protected Mapper < VideoRatingByUser > mapperVideoRatingByUser;
    
    /** Precompile statements to speed up queries. */
    private PreparedStatement updateRating;
       
    /**
     * Default constructor.
     */
    public RatingDseDao() {
        super();
    }
    
    /**
     * Allow explicit intialization for test purpose.
     */
    public RatingDseDao(DseSession dseSession) {
        super(dseSession);
    }

    /** {@inheritDoc} */
    @Override
    protected void initialize() {
        // Mapper
        mapperVideoRating        = mappingManager.mapper(VideoRating.class);
        mapperVideoRatingByUser  = mappingManager.mapper(VideoRatingByUser.class);
        
        // Prepare requests
        String videoRatingsTableName   = mapperVideoRating.getTableMetadata().getName();
        String videoRatingsKeyspace    = mapperVideoRating.getTableMetadata().getKeyspace().getName();
        
        RegularStatement updateStatement = update(videoRatingsKeyspace, videoRatingsTableName)
            .with(QueryBuilder.incr(VideoRating.COLUMN_RATING_COUNTER))
            .and(QueryBuilder.incr(VideoRating.COLUMN_RATING_TOTAL, QueryBuilder.bindMarker()))
            .where(QueryBuilder.eq(VideoRating.COLUMN_VIDEOID, QueryBuilder.bindMarker()));
        updateRating = dseSession.prepare(updateStatement);
        updateRating.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    }
    
    /**
     * Create a rating.
     *
     * @param videoId
     *      current videoId
     * @param userId
     *      current userid
     * @param rating
     *      current rating
     */
    public CompletableFuture<Void> rateVideo( UUID videoId, UUID userId, Integer rating) {
        
        // Param validations
        assertNotNull("rateVideo", "videoId", videoId);
        assertNotNull("rateVideo", "userId", userId);
        assertNotNull("rateVideo", "rating", rating);
        
        // Create Queries
        BoundStatement statement = updateRating.bind()
                .setLong(VideoRating.COLUMN_RATING_TOTAL, rating)
                .setUUID(VideoRating.COLUMN_VIDEOID,      videoId);
        
        VideoRatingByUser entity = new VideoRatingByUser(videoId, userId, rating);
        
        // Logging at DEBUG
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Rating {} on video {} for user {}", rating, videoId, userId);
        }
        
        /**
         * Here, instead of using logged batch, we can insert both mutations asynchronously
         * In case of error, we log the request into the mutation error log for replay later
         * by another micro-service
         *
         * Something else to notice is I am using both a prepared statement with executeAsync()
         * and a call to the mapper's saveAsync() methods.  I could have kept things uniform
         * and stuck with both prepared/bind statements, but I wanted to illustrate the combination
         * and use the mapper for the second statement because it is a simple save operation with no
         * options, increments, etc...  A key point is in the case you see below both statements are actually
         * prepared, the first one I did manually in a more traditional sense and in the second one the
         * mapper will prepare the statement for you automagically.
         */
        return CompletableFuture.allOf(
                FutureUtils.asCompletableFuture(dseSession.executeAsync(statement)),
                // asCompletableFuture(dseSession.executeAsync(mapperVideoRatingByUser.saveQuery(entity))),
                FutureUtils.asCompletableFuture(mapperVideoRatingByUser.saveAsync(entity)));
    }
    
    /**
     * VideoId matches the partition key set in the VideoRating class.
     * 
     * @param videoId
     *      unique identifier for video.
     * @return
     *      find rating
     */
    public CompletableFuture< Optional < VideoRating > > findRating(UUID videoId) {
        assertNotNull("findRating", "videoId", videoId);
        return FutureUtils.asCompletableFuture(mapperVideoRating.getAsync(videoId)).thenApplyAsync(Optional::ofNullable);
    }
    
    /**
     * Find rating from videoid and userid.
     *
     * @param videoId
     *      current videoId
     * @param userid
     *      current user unique identifier.
     * @return
     *      video rating is exist.
     */
    public CompletableFuture< Optional < VideoRatingByUser > > findUserRating(UUID videoId, UUID userid) {
        assertNotNull("findUserRating", "videoId", videoId);
        assertNotNull("findUserRating", "userid", userid);
        return FutureUtils
                    .asCompletableFuture(mapperVideoRatingByUser.getAsync(videoId, userid))
                    .thenApplyAsync(Optional::ofNullable);
    }
  
}
