package com.killrvideo.dse.test.integration;

import java.util.Optional;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.killrvideo.dse.dao.SearchDseDao;
import com.killrvideo.dse.dao.dto.ResultListPage;
import com.killrvideo.dse.model.Video;
import com.killrvideo.dse.test.AbstractTest;

/** 
 * Testing search service.
 */
//@Ignore
public class SearchDaoTestIT extends AbstractTest {
    
	// Where to look : ¯\_(ツ)_/¯
    protected String getContactPointAdress()         { return "localhost"; }
    protected int    getContactPointPort()           { return 9042;        }
    protected ConsistencyLevel getConsistencyLevel() { return ConsistencyLevel.QUORUM; }
   
    protected SearchDseDao searchDao;
    
    @BeforeEach
    public void initDAO() {
        if (searchDao == null) {
            connectKeyspace(KILLRVIDEO_KEYSPACE);
            searchDao = new SearchDseDao(dseSession);
        }
    }
    
    @Test
    public void getSuggestedTag() throws Exception {
    	TreeSet<String> tags = searchDao.getQuerySuggestions("ca", 5);
    	Assert.assertNotNull(tags);
    	Assert.assertTrue(tags.contains("cassandra"));
    	System.out.println("getSuggestedTag:" + tags);
    }
    
    @Test
    public void getSuggestedTagAsync() throws Exception {
    	TreeSet<String> tags = searchDao.getQuerySuggestionsAsync("ca", 5).get();
    	Thread.sleep(1000);
    	Assert.assertNotNull(tags);
    	Assert.assertTrue(tags.contains("cassandra"));
    	System.out.println("getSuggestedTagAsync:" + tags);
    }
    
    @Test
    public void searchVideos() throws Exception {
    	ResultListPage<Video> videos = searchDao.searchVideos("cassandra", 5, Optional.empty());
    	Assert.assertNotNull(videos);
    	System.out.println(videos);
    }
    
}
