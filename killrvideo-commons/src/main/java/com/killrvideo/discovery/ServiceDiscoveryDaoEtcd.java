package com.killrvideo.discovery;

import java.net.URI;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.evanlennick.retry4j.CallExecutor;
import com.evanlennick.retry4j.config.RetryConfig;
import com.evanlennick.retry4j.config.RetryConfigBuilder;
import com.killrvideo.conf.KillrVideoConfiguration;
import com.xqbase.etcd4j.EtcdClient;
import com.xqbase.etcd4j.EtcdClientException;
import com.xqbase.etcd4j.EtcdNode;

/**
 * Hanle operation arount ETCD (connection, read, write).
 * 
 * @author DataStax Developer Advocates Team
 */
@Component("killrvideo.discovery.etcd")
@Profile(KillrVideoConfiguration.PROFILE_DISCOVERY_ETCD)
public class ServiceDiscoveryDaoEtcd implements ServiceDiscoveryDao {

    /** Initialize dedicated connection to ETCD system. */
    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscoveryDaoEtcd.class);
    
    /** Namespace. */
    public static String KILLRVIDEO_SERVICE_NAMESPACE = "/killrvideo/services/";
   
    @Value("${killrvideo.discovery.etcd.host: 10.0.75.1}")
    private String etcdServerHost;
   
    @Value("${killrvideo.discovery.etcd.port: 2379}")
    private int etcdServerPort;
    
    @Value("${killrvideo.discovery.etcd.maxNumberOfTries: 10}")
    private int maxNumberOfTriesEtcd;
    
    @Value("${killrvideo.discovery.etcd.delayBetweenTries: 2}")
    private int delayBetweenTriesEtcd;
     
    /** Native client. */
    private EtcdClient etcdClient;
   
    @PostConstruct
    public void connect() {
        final String etcdUrl = String.format("http://%s:%d", etcdServerHost, etcdServerPort);
        LOGGER.info("Initialize connection to ETCD Server:");
        LOGGER.info(" + Connecting to '{}'", etcdUrl);
        etcdClient = new EtcdClient(URI.create(etcdUrl));
        waitForEtcd();
        LOGGER.info(" + [OK] Connection established.");
    }
    
    /**
     * Read from ETCD using a retry mecanism.
     *
     * @param key
     *      current key to look in ETCD.
     * @param required
     *      key is required if not returning empty list
     * @return
     */
    private void waitForEtcd() {
        final AtomicInteger atomicCount = new AtomicInteger(1);
        Callable<List <EtcdNode>> getKeyFromEtcd = () -> {
            try {
                List <EtcdNode> nodes = etcdClient.listDir("/");
                if ((nodes == null || nodes.isEmpty())) {
                    throw new IllegalStateException("/ is required in ETCD but not yet present");
                }
                return nodes;
            } catch (EtcdClientException e) {
                throw new IllegalStateException("Cannot Access ETCD Server : " + e.getMessage());
            }
        };
        RetryConfig etcdRetryConfig = new RetryConfigBuilder()
                .retryOnAnyException()
                .withMaxNumberOfTries(maxNumberOfTriesEtcd)
                .withDelayBetweenTries(delayBetweenTriesEtcd, ChronoUnit.SECONDS)
                .withFixedBackoff()
                .build();
        new CallExecutor<List <EtcdNode>>(etcdRetryConfig)
                .afterFailedTry(s -> { 
                    LOGGER.info("Attempt #{}/{} : ETCD is not ready (retry in {}s)", 
                             atomicCount.getAndIncrement(), maxNumberOfTriesEtcd, delayBetweenTriesEtcd); })
                .onFailure(s -> {
                    LOGGER.error("ETCD is not ready after {} attempts, exiting", maxNumberOfTriesEtcd);
                    System.err.println("ETCD is not ready after " + maxNumberOfTriesEtcd + " attempts, exiting now.");
                    System.exit(500);
                 })
                .execute(getKeyFromEtcd).getResult()
                .stream().map(node -> node.value)
                .collect(Collectors.toList());
    }
    
    /**
     * Give a service name like 'CommentServices' look for Directory at namespace killrvideo
     * and list value (keys are generated)
     * 
     * @param serviceName
     *      unique service name
     * @return
     *      list of values
     */
    public List < String > lookup(String serviceName) {
        List< String > endPointList = new ArrayList<>();
        String serviceDirectoryKey = KILLRVIDEO_SERVICE_NAMESPACE + serviceName + "/";
        LOGGER.info(" List endpoints for key '{}':", serviceDirectoryKey);
        try {
            List< EtcdNode > existingNodes = etcdClient.listDir(serviceDirectoryKey);
            if (existingNodes != null) {
                endPointList = existingNodes
                        .stream()
                        .map(node -> node.value)
                        .collect(Collectors.toList());
            }
        } catch (EtcdClientException e) {}
        LOGGER.info(" + [OK] Endpoints retrieved '{}':", endPointList);
        return endPointList;
    }
    
    /**
     * Given a servicename and a host give the latest port if exist. This will be used for
     *      
     * @param serviceName
     *      target service
     * @return
     */
    public synchronized Optional<Integer> lookupServicePorts(String serviceName, String hostName) {
        int targetPort = -1;
        LOGGER.info("Accessing last port for endpoint with same host");
        for (String endpoint : lookup(serviceName)) {
            String[] endpointChunks = endpoint.split(":");
            int endPointPort = Integer.valueOf(endpointChunks[1]);
            String endPointHost = endpointChunks[0];
            if (hostName.equalsIgnoreCase(endPointHost)) {
                if (endPointPort > targetPort) {
                    targetPort = endPointPort;
                    LOGGER.info(" + Found {}", targetPort);
                }
            }
        } ;
        return (targetPort == -1) ? Optional.empty() : Optional.of(targetPort);
    }
    
    /** {@inheritDoc} */
    public String register(String serviceName, String hostName, int portNumber) {
        String serviceDirectoryKey = KILLRVIDEO_SERVICE_NAMESPACE + serviceName.trim() + "/";
        String endPoint = hostName + ":" + portNumber;
        try {
            try {
                LOGGER.info("Register endpoint '{}' for key '{}':", endPoint, serviceDirectoryKey);
                etcdClient.createDir(serviceDirectoryKey, null, false);
                LOGGER.info(" + Dir '{}' has been created", serviceDirectoryKey);
            } catch (EtcdClientException e) {
                LOGGER.info(" + Dir '{}' already exist", serviceDirectoryKey);
            }
            List< EtcdNode > existingNodes = etcdClient.listDir(serviceDirectoryKey);
            if (existingNodes != null) {
                Optional <EtcdNode> existingEndpoint = existingNodes
                          .stream().filter(p -> p.value.equalsIgnoreCase(endPoint))
                          .findFirst();
                // Return existing key
                if (existingEndpoint.isPresent()) {
                    LOGGER.info(" + [OK] Endpoint '{}' already exist", endPoint);
                    return existingEndpoint.get().key;
                }
            }
            // Create new Key
            String serviceKey = serviceDirectoryKey + UUID.randomUUID().toString();
            etcdClient.set(serviceKey, endPoint);
            LOGGER.info(" + [OK] Endpoint registered with key '{}'", serviceKey);
            return serviceKey;
        } catch (EtcdClientException e) {
            throw new IllegalStateException("Cannot register services into ETCD", e);
        }
    }

    /** {@inheritDoc} */
    public void unregisterEndpoint(String serviceName, String hostName, int portNumber) {
        String serviceDirectoryKey = KILLRVIDEO_SERVICE_NAMESPACE + serviceName + "/";
        String endPoint = hostName + ":" + portNumber;
        try {
            LOGGER.info("Unregister endpoint '{}' for key '{}':", endPoint, serviceDirectoryKey);
            List< EtcdNode > existingNodes = etcdClient.listDir(serviceDirectoryKey);
            Optional <EtcdNode> existingEndpoint = Optional.empty();
            if (existingNodes != null) {
                existingEndpoint = existingNodes
                        .stream().filter(p -> p.value.equalsIgnoreCase(endPoint))
                        .findFirst();
            }
            if (existingEndpoint.isPresent()) {
                etcdClient.delete(existingEndpoint.get().key);
                LOGGER.info(" + [OK] Endpoint has been deleted (key={})", existingEndpoint.get().key);
            } else {
                LOGGER.info(" + [OK] This endpoint does not exist");
            }
        } catch (EtcdClientException e) {
            throw new IllegalStateException("Cannot register services into ETCD", e);
        }
    }
    
    /** {@inheritDoc} */
    public void unregister(String serviceName) {
        String serviceDirectoryKey = KILLRVIDEO_SERVICE_NAMESPACE + serviceName + "/";
        try {
            LOGGER.info("Delete dir  '{}'", serviceDirectoryKey);
            etcdClient.deleteDir("/killrvideo/services/" + serviceName, true);
            LOGGER.info(" + [OK] Directory has been deleted");
        } catch (EtcdClientException e) {
            LOGGER.info(" + [OK] Directory did not exist");
        }
    }
    
}
