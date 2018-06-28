package com.killrvideo.dse.test.embedded;

import static org.junit.jupiter.api.Assertions.assertAll;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import com.killrvideo.dse.dao.VideoCatalogDseDao;
import com.killrvideo.dse.dao.dto.CustomPagingState;
import com.killrvideo.dse.dao.dto.LatestVideosPage;
import com.killrvideo.dse.model.LatestVideo;
import com.killrvideo.dse.model.Video;
import com.killrvideo.dse.test.AbstractTestEmbedded;

@DisplayName("Unit testing VideoCatalogDao with Embedded Cassandra")
public class VideoCatalogTest extends AbstractTestEmbedded {
    
    // ¯\_(ツ)_/¯
    private VideoCatalogDseDao videoDao; 
    
    @BeforeEach
    public void initDAO() {
        if (videoDao == null) {
            connectKeyspace(KILLRVIDEO_KEYSPACE);
            videoDao = new VideoCatalogDseDao(dseSession);
        }
    }
    
    @Test
    @DisplayName("When inserting Video into empty tables you got 1 record")
    public void testVideo() throws Exception {
        // Given : Tables exist and are empty
        assertAll("Table videos",
           () -> assertTableExist(KILLRVIDEO_KEYSPACE, TABLENAME_VIDEOS),
           () -> assertTableIsEmpty(KILLRVIDEO_KEYSPACE, TABLENAME_VIDEOS)
        );
        assertAll("Table latest_videos",
           () -> assertTableExist(KILLRVIDEO_KEYSPACE, TABLENAME_LATEST_VIDEOS),
           () -> assertTableIsEmpty(KILLRVIDEO_KEYSPACE, TABLENAME_LATEST_VIDEOS)
        );
        assertAll("Table user_videos",
           () -> assertTableExist(KILLRVIDEO_KEYSPACE, TABLENAME_USER_VIDEOS),
           () -> assertTableIsEmpty(KILLRVIDEO_KEYSPACE, TABLENAME_USER_VIDEOS)
        );
        
        // When : I save a youtube video 
        Video video = new Video("Distributed Data Show Episode 40 - Feature flags");
        video.setAddedDate(new Date());
        video.setVideoid(UUIDs.timeBased());
        video.setDescription("Feature Flags, also named Feature Toggle is a software development pattern...");
        video.setLocation("Bui0up9jAo4");
        video.setLocationType(0);
        video.setUserid(UUID.randomUUID());
        video.setPreviewImageLocation("//img.youtube.com/vi/"+ video.getLocation() + "/hqdefault.jpg");
        video.setTags(new HashSet<>(Arrays.asList("feature", "distributed")));
        videoDao.insertVideo(video);

        // Then : Tables have been updated accordingly
        assertCountItemInTable(1, KILLRVIDEO_KEYSPACE, TABLENAME_VIDEOS);
        assertCountItemInTable(1, KILLRVIDEO_KEYSPACE, TABLENAME_USER_VIDEOS);
        assertCountItemInTable(1, KILLRVIDEO_KEYSPACE, TABLENAME_LATEST_VIDEOS);
    }
    
    @Test
    @DisplayName("Search for latest videos ╯°□°）╯")
    public void getLatestVideo() throws Exception {
        // Given : Table are loaded with a few videos
        int nb = 11;
        for(int idx =0;idx <nb;idx++) {
            Video video = new Video("Video Generated " + idx);
            video.setAddedDate(new Date());video.setVideoid(UUIDs.timeBased());
            video.setLocation("Bui0up9jAo" + nb);
            video.setLocationType(0);
            video.setUserid(UUID.randomUUID());
            videoDao.insertVideo(video);
        }
        assertCountItemInTable(nb, KILLRVIDEO_KEYSPACE, TABLENAME_LATEST_VIDEOS);
        
        // When getLatest
        int recordNeeded = 10;
        final Optional<Date> startDate = Optional.of(new Date(System.currentTimeMillis()-24*3600));
        final Optional<UUID> startVid  = Optional.empty();
        CustomPagingState cps = new CustomPagingState()
                .currentBucket(0).cassandraPagingState(null)
                .listOfBuckets(LongStream.rangeClosed(0L, 7L).boxed()
                .map(Instant.now().atZone(ZoneId.systemDefault())::minusDays)
                .map(x -> x.format(VideoCatalogDseDao.DATEFORMATTER))
                .collect(Collectors.toList()));
        LatestVideosPage myPage = videoDao.getLatestVideoPreviews(cps, recordNeeded, startDate, startVid);
        for (LatestVideo video : myPage.getListOfPreview()) {
            System.out.println(video.getName());
        }
        
        // Get Expected page
    }

}
