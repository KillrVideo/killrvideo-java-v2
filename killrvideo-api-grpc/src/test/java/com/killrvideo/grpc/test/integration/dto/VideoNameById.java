package com.killrvideo.grpc.test.integration.dto;

import java.util.UUID;

public class VideoNameById {

    public final UUID id;
    
    public final String name;

    public VideoNameById(UUID id, String name) {
        this.id = id;
        this.name = name;
    }

}