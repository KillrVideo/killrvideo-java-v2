package com.killrvideo.conf;

import javax.validation.Validation;
import javax.validation.Validator;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for KillrVideo application leveraging on DSE, ETCD and any external source.
 *
 * @author DataStax Developer Advocates team.
 */
@Configuration
public class KillrVideoConfiguration {
    
    @Value("#{environment.KILLRVIDEO_HOST_IP ?: '10.0.75.1'}")
    private String applicationHost;

    @Value("${application.name: KillrVideo}")
    private String applicationName;
    
    @Bean
    public Validator getBeanValidator() {
        return Validation.buildDefaultValidatorFactory().getValidator();
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
     * Getter for attribute 'applicationHost'.
     *
     * @return
     *       current value of 'applicationHost'
     */
    public String getApplicationHost() {
        return applicationHost;
    }

}
