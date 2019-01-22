package com.killrvideo.messaging.conf;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.eventbus.EventBus;

/**
 * Store all configuration related to Messagging.
 *
 * @author DataStax Developer Advocates team.
 */
@Configuration
public class MessagingConfiguration {
    
    /** Event Bus. */
    private static final String EVENT_BUS_KILLRVIODEO = "killrvideo_event_bus";
    
    @Value("${killrvideo.messaging.inmemory.threadpool.min.threads:5}")
    private int minThreads;
    
    @Value("${killrvideo.messaging.inmemory.max.threads:10}")
    private int maxThreads;
    
    @Value("${killrvideo.messaging.inmemory.ttlThreads:60}")
    private int threadsTTLSeconds;
    
    @Value("${killrvideo.messaging.inmemory.queueSize:1000}")
    private int threadPoolQueueSize;
    
    @Bean
    public EventBus createEventBus() {
        return new EventBus(EVENT_BUS_KILLRVIODEO);
    }
    
    /**
     * Initialize the threadPool.
     *
     * @return
     *      current executor for this
     */
    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService threadPool() {
        return new ThreadPoolExecutor(getMinThreads(), getMaxThreads(), 
                getThreadsTTLSeconds(), TimeUnit.SECONDS, 
                new LinkedBlockingQueue<>(getThreadPoolQueueSize()), 
                new KillrVideoThreadFactory());
    }
    
    /**
     * Getter for attribute 'minThreads'.
     *
     * @return
     *       current value of 'minThreads'
     */
    public int getMinThreads() {
        return minThreads;
    }

    /**
     * Getter for attribute 'maxThreads'.
     *
     * @return
     *       current value of 'maxThreads'
     */
    public int getMaxThreads() {
        return maxThreads;
    }

    /**
     * Getter for attribute 'threadsTTLSeconds'.
     *
     * @return
     *       current value of 'threadsTTLSeconds'
     */
    public int getThreadsTTLSeconds() {
        return threadsTTLSeconds;
    }

    /**
     * Getter for attribute 'threadPoolQueueSize'.
     *
     * @return
     *       current value of 'threadPoolQueueSize'
     */
    public int getThreadPoolQueueSize() {
        return threadPoolQueueSize;
    }


}
