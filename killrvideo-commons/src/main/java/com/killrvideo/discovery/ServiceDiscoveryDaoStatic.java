package com.killrvideo.discovery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.killrvideo.conf.KillrVideoConfiguration;

/**
 * There is no explicit access to 
 * 
 * @author Cedrick LUNVEN (@clunven)
 */
@Component("killrvideo.discovery.network")
@Profile(KillrVideoConfiguration.PROFILE_DISCOVERY_STATIC)
public class ServiceDiscoveryDaoStatic implements ServiceDiscoveryDao {
   
    /** Initialize dedicated connection to ETCD system. */
    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscoveryDaoStatic.class);
    
    @Value("${killrvideo.discovery.service.kafka: kafka}")
    private String kafkaServiceName;
    
    @Value("${killrvideo.discovery.static.kafka.port: 8082}")
    private int kafkaPort;
    
    @Value("${killrvideo.discovery.static.kafka.brokers}")
    private String kafkaBrokers;
    @Value("#{environment.KILLRVIDEO_KAFKA_BROKERS}")
    private Optional<String> kafkaBrokersEnvVar;
    
    @Value("${killrvideo.discovery.service.cassandra: cassandra}")
    private String cassandraServiceName;
    
    @Value("${killrvideo.discovery.static.cassandra.port: 9042}")
    private int cassandraPort;
    
    @Value("${killrvideo.discovery.static.cassandra.contactPoints}")
    private String cassandraContactPoints;
    @Value("#{environment.KILLRVIDEO_DSE_CONTACTPOINTS}")
    private Optional<String> cassandraContactPointsEnvVar;
   
    /** {@inheritDoc} */
    @Override
    public List<String> lookup(String serviceName) {
        List< String > endPointList = new ArrayList<>();
        LOGGER.info(" + Lookup for key '{}':", serviceName);
        if (kafkaServiceName.equalsIgnoreCase(serviceName)) {
        	if (!kafkaBrokersEnvVar.isEmpty() && !kafkaBrokersEnvVar.get().isBlank()) {
        	    cassandraContactPoints = kafkaBrokersEnvVar.get();
        		LOGGER.info(" + Reading broker from KILLRVIDEO_KAFKA_BROKERS");
        	}
        	Arrays.asList(kafkaBrokers.split(",")).stream()
        		  .forEach(ip -> endPointList.add(ip + ":" + kafkaPort));

        } else if (cassandraServiceName.equalsIgnoreCase(serviceName)) {
        	// Explicit overwriting of contact points from env var
        	// Better than default spring : simpler
        	if (!cassandraContactPointsEnvVar.isEmpty() && !cassandraContactPointsEnvVar.get().isBlank()) {
        		cassandraContactPoints = cassandraContactPointsEnvVar.get();
        		LOGGER.info(" + Reading contactPoints from KILLRVIDEO_DSE_CONTACTPOINTS");
        	}
        	Arrays.asList(cassandraContactPoints.split(","))
        	      .stream()
        		  .forEach(ip -> endPointList.add(ip + ":" + cassandraPort));
        }
        LOGGER.info(" + Endpoints retrieved '{}':", endPointList);
        return endPointList;
    }
    
    /** {@inheritDoc} */
    @Override
    public String register(String serviceName, String hostName, int portNumber) {
        // Do nothing in k8s service are registered through DNS
        return serviceName;
    }

    /** {@inheritDoc} */
    @Override
    public void unregister(String serviceName) {}

    /** {@inheritDoc} */
    @Override
    public void unregisterEndpoint(String serviceName, String hostName, int portNumber) {}

}
