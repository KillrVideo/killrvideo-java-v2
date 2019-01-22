package com.killrvideo.service.video.dto;

import java.util.Date;
import java.util.UUID;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.killrvideo.model.CommonConstants;
import com.killrvideo.service.video.dao.VideoCatalogDseDao;

/**
 * Pojo representing DTO for table 'latest_videos'
 *
 * @author DataStax Developer Advocates team.
 */
@Table(keyspace = CommonConstants.KILLRVIDEO_KEYSPACE, 
    name = VideoCatalogDseDao.TABLENAME_LATEST_VIDEOS)
public class LatestVideo extends VideoPreview {

    /** Serial. */
   private static final long serialVersionUID = -8527565276521920973L;

    /** Column names in the DB. */
    public static final String COLUMN_USERID   = "userid";
    public static final String COLUMN_YYYYMMDD = "yyyymmdd";
    
    @PartitionKey
    private String yyyymmdd;

    @Column
    private UUID userid;

    /**
     * Default constructor.
     */
    public LatestVideo() {}

    /**
     * Constructor with all parameters.
     */
    public LatestVideo(String yyyymmdd, UUID userid, UUID videoid, String name, String previewImageLocation, Date addedDate) {
        super(name, previewImageLocation, addedDate, videoid);
        this.yyyymmdd = yyyymmdd;
        this.userid = userid;
    }
    
    /**
     * Getter for attribute 'yyyymmdd'.
     *
     * @return
     *       current value of 'yyyymmdd'
     */
    public String getYyyymmdd() {
        return yyyymmdd;
    }

    /**
     * Setter for attribute 'yyyymmdd'.
     * @param yyyymmdd
     * 		new value for 'yyyymmdd '
     */
    public void setYyyymmdd(String yyyymmdd) {
        this.yyyymmdd = yyyymmdd;
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
