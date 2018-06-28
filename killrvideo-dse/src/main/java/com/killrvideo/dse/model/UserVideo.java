package com.killrvideo.dse.model;

import java.util.Date;
import java.util.UUID;

import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Pojo representing DTO for table 'user_videos'
 *
 * @author DataStax evangelist team.
 */
@Table(keyspace = SchemaConstants.KILLRVIDEO_KEYSPACE, name = SchemaConstants.TABLENAME_USER_VIDEOS)
public class UserVideo extends VideoPreview {

    /** Serial. */
    private static final long serialVersionUID = -4689177834790056936L;
    
    /** Column names in the DB. */
    public static final String COLUMN_USERID = "userid";
    
    @PartitionKey
    private UUID userid;

    /**
     * Deafult Constructor allowing reflection.
     */
    public UserVideo() {}

    /**
     * Constructor without preview.
     */
    public UserVideo(UUID userid, UUID videoid, String name, Date addedDate) {
        this(userid, videoid, name, null, addedDate);
    }

    /**
     * Full set constructor.
     */
    public UserVideo(UUID userid, UUID videoid, String name, String previewImageLocation, Date addedDate) {
        super(name, previewImageLocation, addedDate, videoid);
        this.userid = userid;
    }

    /**
     * Getter for attribute 'userid'.
     *
     * @return
     *       current value of 'userid'
     */
    public UUID getUserid() {
        return userid;
    }

    /**
     * Setter for attribute 'userid'.
     * @param userid
     * 		new value for 'userid '
     */
    public void setUserid(UUID userid) {
        this.userid = userid;
    }
    
    
}
