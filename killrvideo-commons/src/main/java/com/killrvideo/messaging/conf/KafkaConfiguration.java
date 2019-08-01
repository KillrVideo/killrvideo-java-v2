package com.killrvideo.messaging.conf;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import com.killrvideo.conf.KillrVideoConfiguration;
import com.killrvideo.discovery.ServiceDiscoveryDao;

/**
 * Use Kafka to exchange messages between services. 
 *
 * @author Cedrick LUNVEN (@clunven) *
 */
@Configuration
@Profile(KillrVideoConfiguration.PROFILE_MESSAGING_KAFKA)
public class KafkaConfiguration {
    
    /** Name of service in ETCD. */
    public static final String SERVICE_KAFKA = "kafka";
    
    /** Default CQL listening port. */
    public static final int DEFAULT_PORT = 8082;
    
    /** Kafka Server to be used. */
    private String kafkaServer;
    
    @Value("${kafka.ack: 1 }")
    private String producerAck;
    
    @Value("${kafka.consumerGroup: killrvideo }")
    private String consumerGroup;
    
    @Autowired
    private ServiceDiscoveryDao discoveryDao;
    
    /**
     * Should we init connection with ETCD or direct.
     *
     * @return
     *      target kafka adress
     */
    private String getKafkaServerConnectionUrl() {
        if (null == kafkaServer) {
            kafkaServer = String.join(",", discoveryDao.lookup(SERVICE_KAFKA));  
        } 
        return kafkaServer;
    }
    
    @Bean("kafka.producer")
    public KafkaProducer<String, byte[]> jsonProducer() {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG,      getKafkaServerConnectionUrl());
        props.put(KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ACKS_CONFIG,                   producerAck);
        return new KafkaProducer<String, byte[]>(props);
    }

    @Bean("kafka.consumer.videoRating")
    public KafkaConsumer<String, byte[]> videoRatingConsumer() {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG,        getKafkaServerConnectionUrl());
        props.put(GROUP_ID_CONFIG,                 consumerGroup);
        props.put(KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<String,byte[]>(props);
    }
    
    @Bean("kafka.consumer.userCreating")
    public KafkaConsumer<String, byte[]> userCreatingConsumer() {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG,        getKafkaServerConnectionUrl());
        props.put(GROUP_ID_CONFIG,                 consumerGroup);
        props.put(KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<String,byte[]>(props);
    }
    
    @Bean("kafka.consumer.videoCreating")
    public KafkaConsumer<String, byte[]> videoCreatingConsumer() {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG,        getKafkaServerConnectionUrl());
        props.put(GROUP_ID_CONFIG,                 consumerGroup);
        props.put(KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<String,byte[]>(props);
    }
    
    @Bean("kafka.consumer.error")
    public KafkaConsumer<String, byte[]> errorConsumer() {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG,        getKafkaServerConnectionUrl());
        props.put(GROUP_ID_CONFIG,                 consumerGroup);
        props.put(KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<String,byte[]>(props);
    }
    
}
