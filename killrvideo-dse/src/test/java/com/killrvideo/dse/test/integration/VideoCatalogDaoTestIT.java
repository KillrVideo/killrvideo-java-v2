package com.killrvideo.dse.test.integration;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.junit.Ignore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.killrvideo.dse.dao.VideoCatalogDseDao;
import com.killrvideo.dse.dao.dto.CustomPagingState;
import com.killrvideo.dse.dao.dto.LatestVideosPage;
import com.killrvideo.dse.model.LatestVideo;
import com.killrvideo.dse.test.AbstractTest;

/**
 * Sample class to work woth cassandra.
 *
 * @author DataStax evangelist team.
 */
@Ignore
public class VideoCatalogDaoTestIT extends AbstractTest {
    
	// Where to look : ¯\_(ツ)_/¯
    protected String getContactPointAdress()         { return "localhost"; }
    protected int    getContactPointPort()           { return 9042;        }
    protected ConsistencyLevel getConsistencyLevel() { return ConsistencyLevel.QUORUM; }
    
    protected VideoCatalogDseDao videoDao;
    
    @BeforeEach
    public void initDAO() {
        if (videoDao == null) {
            connectKeyspace(KILLRVIDEO_KEYSPACE);
            videoDao = new VideoCatalogDseDao(dseSession);
        }
    }
    
    @Test
    public void getLatestVideos() throws Exception {
        int recordNeeded = 20;
        final Optional<Date> startDate = Optional.empty();
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
        
    }
}
