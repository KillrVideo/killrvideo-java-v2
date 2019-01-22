package com.killrvideo.test;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.xqbase.etcd4j.EtcdClientException;

@RunWith(JUnitPlatform.class)
@ExtendWith(SpringExtension.class)
@TestPropertySource(locations="/killrvideo-test.properties")
//@ContextConfiguration(classes= {EtcdDao.class})
@DisplayName("Testing resgistration do ETCD")
public class EtcdDaoTest {
    
    //Autowired
    //public EtcdDao etcdDao;
    
    @BeforeAll
    public static void initEnv() {
        // I want to ovveride the DOCKER_IP as I am not in the Docker Cluster (Unit testing)
        System.setProperty("KILLRVIDEO_DOCKER_IP", "localhost");
    }
    
    @Test
    public void testConnection() throws EtcdClientException {
       //etcdDao.registerServiceEndpoint("CommentsService", "localhost" , 33101);
       //etcdDao.registerServiceEndpoint("CommentsService", "localhost", 33102);
       //etcdDao.registerServiceEndpoint("CommentsService", "localhost", 33103);
       //System.out.println(etcdDao.listServiceEndpoints("CommentsService"));
       //System.out.println(etcdDao.getMaxPort("CommentsService", "localhost"));
       //etcdDao.unRegisterServiceEnpoint("CommentsService", "localhost:33101");
    }

}
