package com.killrvideo.dse.model;

import java.io.Serializable;
import java.util.UUID;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Pojo representing DTO for table 'video_playback_stats'.
 *
 * @author DataStax evangelist team.
 */
@Table(keyspace = SchemaConstants.KILLRVIDEO_KEYSPACE, name = SchemaConstants.TABLENAME_PLAYBACK_STATS)
public class VideoPlaybackStats implements Serializable {

    /** Serial. */
    private static final long serialVersionUID = -8636413035520458200L;
    
    /** COLUNMNS NAMES. */
    public static final String COLUMN_VIDEOID   = "videoid";
    public static final String COLUMN_VIEWS     = "views";

    @PartitionKey
    private UUID videoid;

    /**
     * "views" column is a counter type in the underlying DSE database.  As of driver version 3.2 there
     * is no "@Counter" annotation that I know of.  No worries though, just use the incr() function
     * while using the QueryBuilder.  Something similar to with(QueryBuilder.incr("views")).
     */
    @Column
    private Long views;

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
     * Getter for attribute 'views'.
     *
     * @return
     *       current value of 'views'
     */
    public Long getViews() {
        return views;
    }

    /**
     * Setter for attribute 'views'.
     * @param views
     * 		new value for 'views '
     */
    public void setViews(Long views) {
        this.views = views;
    }
    
    
}
