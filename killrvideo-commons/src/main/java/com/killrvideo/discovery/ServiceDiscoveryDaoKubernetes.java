package com.killrvideo.discovery;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * There is no explicit access to 
 * 
 * @author Cedrick LUNVEN (@clunven)
 */
public class ServiceDiscoveryDaoKubernetes implements ServiceDiscoveryDao {
   
    /** Initialize dedicated connection to ETCD system. */
    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscoveryDaoKubernetes.class);
    
    /** {@inheritDoc} */
    @Override
    public List<String> lookupService(String serviceName) {
        List< String > endPointList = new ArrayList<>();
        LOGGER.info(" List endpoints for key '{}':", serviceName);
        // Will have a single endPoint
        return endPointList;
    }
    
    /** {@inheritDoc} */
    @Override
    public String registerService(String serviceName, String hostName, int portNumber) {
        // Do nothing in k8s service are registered through DNS
        return serviceName;
    }

    /** {@inheritDoc} */
    @Override
    public void unRegisterService(String serviceName) {}

    /** {@inheritDoc} */
    @Override
    public void unRegisterService(String serviceName, String hostName, int portNumber) {}

}
