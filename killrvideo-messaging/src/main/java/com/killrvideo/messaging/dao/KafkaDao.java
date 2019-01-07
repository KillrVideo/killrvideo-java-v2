package com.killrvideo.messaging.dao;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.databind.JsonNode;

@Repository
public class KafkaDao {
    
    @Autowired
    @Qualifier("producer.json")
    protected KafkaProducer<String, JsonNode> jsonProducer;
    
    /**
     * JSON PRODUCER.
     * 
     * @param jsonMsg
     */
    public void sendJsonMessage(ProducerRecord<String, JsonNode> jsonMsg) {
        jsonProducer.send(jsonMsg);
    }
}
