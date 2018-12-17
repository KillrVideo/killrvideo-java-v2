package com.killrvideo.grpc.test.integration.dto;

/**
 * POJO for tests.
 **/
public class CucumberVideoDetails {

    public final String id;
    public final String name;
    public final String description;
    public final String tags;
    public final String url;

    public CucumberVideoDetails(String id, String name, String description, String tags, String url) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.tags = tags;
        this.url = url;
    }
}
