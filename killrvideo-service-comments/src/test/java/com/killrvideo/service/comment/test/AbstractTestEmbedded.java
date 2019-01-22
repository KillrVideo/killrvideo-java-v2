package com.killrvideo.service.comment.test;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import com.datastax.driver.core.ConsistencyLevel;

/**
 * Support class for test.
 *
 * @author DataStax Developer Advocates team.
 */
public abstract class AbstractTestEmbedded extends AbstractTest {

    // Embedded Settings
    protected String getContactPointAdress()         { return "127.0.0.1"; }
    protected int    getContactPointPort()           { return 9142;        }
    protected ConsistencyLevel getConsistencyLevel() { return ConsistencyLevel.ONE; }
    
    @BeforeAll
    public static void startCassandra() throws Exception {
    }
    
    @BeforeEach
    public void createKeySpace() throws Exception {
        //DseUtils.executeCQLFile(dseSession, "keyspace-killrvideo.cql");
    	
   	}
    
    // --- Will execute your tests here -------
    
    @AfterEach
    public void dropKeyspaces() throws Exception {
    }
    
    @AfterAll
    public static void stopCassandra() {
        // Not require anymore to clea 
        // EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
    }
  
   
}
