package com.killrvideo.etcd;

public interface ServiceDiscoveryDao {
    
    int pickServicePort(String serviceName, String applicationHost);

}
