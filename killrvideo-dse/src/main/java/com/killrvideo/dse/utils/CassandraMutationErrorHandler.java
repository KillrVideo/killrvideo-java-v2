package com.killrvideo.dse.utils;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.eventbus.Subscribe;
import com.killrvideo.core.error.ErrorEvent;

/**
 * Catch exceptions and create a {@link CassandraMutationError} in EventBus.
 *
 * @author DataStax evangelist team.
 */
@Component
public class CassandraMutationErrorHandler {

    /** LOGGER for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(CassandraMutationErrorHandler.class);
    
    @Value("${killrvideo.cassandra.mutation-error-log: /tmp/killrvideo-mutation-errors.log}")
    private String mutationErrorLog;
    
    private PrintWriter errorLogFile;

    @PostConstruct
    public void openErrorLogFile() throws FileNotFoundException {
        this.errorLogFile = new PrintWriter(getMutationErrorLog());
    }

    /**
     * Here we just record the original Grpc request so that we can replay
     * them later.
     *
     * An alternative impl can just push the request to a message queue or
     * event bus so that it can be handled by another micro-service
     *
     */
    @Subscribe
    public void handle(ErrorEvent mutationError) {
        final String errorLog = mutationError.buildErrorLog();
        LOGGER.debug(String.format("Recording mutation error %s", errorLog));
        errorLogFile.append(errorLog).append("\n***********************\n");
        errorLogFile.flush();
    }

    /**
     * Closing log file.
     */
    @PreDestroy
    public void closeErrorLogFile() {
        this.errorLogFile.close();
    }
    
    /**
     * Getter for attribute 'mutationErrorLog'.
     *
     * @return
     *       current value of 'mutationErrorLog'
     */
    public String getMutationErrorLog() {
        return mutationErrorLog;
    }

}
