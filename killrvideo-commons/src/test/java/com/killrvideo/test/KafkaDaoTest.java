package com.killrvideo.test;

import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.killrvideo.discovery.ServiceDiscoveryDaoEtcd;
import com.killrvideo.messaging.conf.KafkaConfiguration;
import com.killrvideo.messaging.dao.ErrorProcessor;
import com.killrvideo.messaging.dao.MessagingDaoKafka;

import killrvideo.video_catalog.events.VideoCatalogEvents.YouTubeVideoAdded;

@RunWith(JUnitPlatform.class)
@ExtendWith(SpringExtension.class)
@TestPropertySource(locations="/killrvideo-test.properties")
@ContextConfiguration(classes= {
        ErrorProcessor.class, ServiceDiscoveryDaoEtcd.class, 
        KafkaConfiguration.class, MessagingDaoKafka.class})
public class KafkaDaoTest {
    
    @Autowired
    public MessagingDaoKafka kafkaDao;
    
    @BeforeAll
    public static void initEnv() {
        // I want to ovveride the DOCKER_IP as I am not in the Docker Cluster (Unit testing)
        System.setProperty("KILLRVIDEO_DOCKER_IP", "localhost");
    }
    
    @Test
    public void testSendmessage() throws Exception {
        System.out.println("Before");
        Future<RecordMetadata> future = kafkaDao.sendEvent("topic-kv-videoCreation", YouTubeVideoAdded.newBuilder().build());
        future.get();
        System.out.println("After");
    }

}
