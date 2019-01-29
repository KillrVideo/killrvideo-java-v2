package com.killrvideo.service.search;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;

/**
 * Killrvideo-services module will merge all service in a single runtime.
 * Still if you want to run Comment Service as StandAlone use this class.
 * 
 * @author Cedrick LUNVEN (@clunven)
 */
@ComponentScan(basePackages="com.killrvideo")
@EnableAutoConfiguration
public class SearchServiceStandAloneApp {

    /**
     * As SpringBoot application, this is the "main" class
     */
    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(SearchServiceStandAloneApp.class);
        app.run(args);
    }
    
}