package com.killrvideo.core.conf;

import javax.validation.Validation;
import javax.validation.Validator;

import org.ff4j.FF4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * Configuration for KillrVideo application leveraging on DSE, ETCD and any external source.
 *
 * @author DataStax evangelist team.
 */
@Configuration
@PropertySource("classpath:killrvideo.properties")
public class KillrVideoConfiguration {
    
    @Value("${killrvideo.application.name:KillrVideo}")
    private String applicationName;
    
    @Value("${killrvideo.application.instance.id: 0}")
    private int applicationInstanceId;
    
    @Value("#{environment.KILLRVIDEO_HOST_IP ?: '10.0.75.1'}")
    private String applicationHost;
    
    @Bean
    public Validator getBeanValidator() {
        return Validation.buildDefaultValidatorFactory().getValidator();
    }

    /** 
     * Define Feature Toggle: So far there is no permanent store. Configuration is load
     * from XML and hold in memory. To persist states define here the store you like, probable
     * Cassandra here.
     **/
    @Bean
    public FF4j getFF4j() {
        FF4j ff4j = new FF4j("ff4j-conf.xml");
        ff4j.audit(true);
        ff4j.autoCreate(true);
        return ff4j;
    }
    
    /**
     * Getter for attribute 'applicationName'.
     *
     * @return
     *       current value of 'applicationName'
     */
    public String getApplicationName() {
        return applicationName;
    }

    /**
     * Getter for attribute 'applicationInstanceId'.
     *
     * @return
     *       current value of 'applicationInstanceId'
     */
    public int getApplicationInstanceId() {
        return applicationInstanceId;
    }

    /**
     * Getter for attribute 'applicationHost'.
     *
     * @return
     *       current value of 'applicationHost'
     */
    public String getApplicationHost() {
        return applicationHost;
    }

}
