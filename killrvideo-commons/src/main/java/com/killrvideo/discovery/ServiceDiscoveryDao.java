package com.killrvideo.discovery;

import java.util.List;

/**
 * Work with service registry (ETCD, Consul..)
 * 
 * @author Cedrick LUNVEN (@clunven)
 */
public interface ServiceDiscoveryDao {
   
    /**
     * Register new endpoint for a service.
     * @param serviceName
     *      unique service identifier
     * @param hostName
     *      current hostname
     * @param portNumber
     *      current port number
     * @return
     *      service key (service name + namespace)
     */
    String register(String serviceName, String hostName, int portNumber);
    
    /**
     * List endpoints available for a service.
     *
     * @param serviceName
     *      service identifier
     * @return
     *      list of endpoints like hostname1:port1, hostname2:port2
     */
    List < String > lookup(String serviceName);
    
    /**
     * Unregister all endpoints for a service.
     *
     * @param serviceName
     *      service unique identifier
     */
    void unregister(String serviceName);
    
    /**
     * Unregister one endpoint for a service.
     *
     * @param serviceName
     *      service unique identifier
     * @param hostName
     *      current hostname
     * @param portNumber
     *      current port number
     */
    void unregisterEndpoint(String serviceName, String hostName, int portNumber);
    
}
