package com.killrvideo.dse.test;

import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import com.datastax.driver.core.ConsistencyLevel;

/**
 * Support class for test.
 *
 * @author DataStax evangelist team.
 */
public abstract class AbstractTestEmbedded extends AbstractTest {

    // Embedded Settings
    protected String getContactPointAdress()         { return "127.0.0.1"; }
    protected int    getContactPointPort()           { return 9142;        }
    protected ConsistencyLevel getConsistencyLevel() { return ConsistencyLevel.ONE; }
    
    @BeforeAll
    public static void startCassandra() throws Exception {
    	EmbeddedCassandraServerHelper.startEmbeddedCassandra(15000);
    }
    
    @BeforeEach
    public void createKeySpace() throws Exception {
        //DseUtils.executeCQLFile(dseSession, "keyspace-killrvideo.cql");
    	CQLDataLoader cqlDataLoader = new CQLDataLoader(dseSession);
    	cqlDataLoader.load(new ClassPathCQLDataSet("killrvideo.cql"));
   	}
    
    // --- Will execute your tests here -------
    
    @AfterEach
    public void dropKeyspaces() throws Exception {
    	EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }
    
    @AfterAll
    public static void stopCassandra() {
        // Not require anymore to clea 
        // EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
    }
  
   
}
