package com.killrvideo.dse.model;

import java.io.Serializable;
import java.util.UUID;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Pojo representing DTO for table 'video_ratings'.
 *
 * @author DataStax evangelist team.
 */
@Table(keyspace = SchemaConstants.KILLRVIDEO_KEYSPACE, name = SchemaConstants.TABLENAME_VIDEOS_RATINGS)
public class VideoRating implements Serializable {

    /** Serial. */
    private static final long serialVersionUID = -8874199914791405808L;
    
    /** Column names in the DB. */
    public static final String COLUMN_RATING_COUNTER = "rating_counter";
    public static final String COLUMN_RATING_TOTAL   = "rating_total";
    public static final String COLUMN_VIDEOID        = "videoid";

    @PartitionKey
    private UUID videoid;

    @Column(name = COLUMN_RATING_COUNTER)
    private Long ratingCounter;

    @Column(name = COLUMN_RATING_TOTAL)
    private Long ratingTotal;

    /**
     * Getter for attribute 'videoid'.
     *
     * @return
     *       current value of 'videoid'
     */
    public UUID getVideoid() {
        return videoid;
    }

    /**
     * Setter for attribute 'videoid'.
     * @param videoid
     * 		new value for 'videoid '
     */
    public void setVideoid(UUID videoid) {
        this.videoid = videoid;
    }

    /**
     * Getter for attribute 'ratingCounter'.
     *
     * @return
     *       current value of 'ratingCounter'
     */
    public Long getRatingCounter() {
        return ratingCounter;
    }

    /**
     * Setter for attribute 'ratingCounter'.
     * @param ratingCounter
     * 		new value for 'ratingCounter '
     */
    public void setRatingCounter(Long ratingCounter) {
        this.ratingCounter = ratingCounter;
    }

    /**
     * Getter for attribute 'ratingTotal'.
     *
     * @return
     *       current value of 'ratingTotal'
     */
    public Long getRatingTotal() {
        return ratingTotal;
    }

    /**
     * Setter for attribute 'ratingTotal'.
     * @param ratingTotal
     * 		new value for 'ratingTotal '
     */
    public void setRatingTotal(Long ratingTotal) {
        this.ratingTotal = ratingTotal;
    }

}
