package com.killrvideo.dse.test.integration;

import java.util.UUID;

import org.junit.Ignore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.killrvideo.core.utils.HashUtils;
import com.killrvideo.dse.dao.UserDseDao;
import com.killrvideo.dse.model.User;
import com.killrvideo.dse.test.AbstractTest;

/**
 * Direct integartion test of DAO (no need for API)
 *
 * @author DataStax Evangelist Team
 */
@Ignore
public class UserDseDaoTestIT extends AbstractTest {
    
    protected String getContactPointAdress()         { return "localhost";             }
    protected int    getContactPointPort()           { return 9042;                    }
    protected ConsistencyLevel getConsistencyLevel() { return ConsistencyLevel.QUORUM; }
    
    protected UserDseDao userDao;
    
    @BeforeEach
    public void initDAO() {
        if (userDao == null) {
            connectKeyspace(KILLRVIDEO_KEYSPACE);
            userDao = new UserDseDao(dseSession);
        }
    }
    
    @Test
    public void createUserAsync() throws Exception {
        User myNewUser = new User();
        myNewUser.setEmail("b.b@b.com");
        myNewUser.setFirstname("b");
        myNewUser.setLastname("b");
        myNewUser.setUserid(UUID.randomUUID());
        userDao.createUserAsync(myNewUser, HashUtils.hashPassword("bb"))
               .whenComplete((rs, errors) -> {
                   System.out.println("rs:" + rs);
                   System.out.println("errors:" + errors);
               });
        Thread.sleep(1000);
    }
    
    
  
}
