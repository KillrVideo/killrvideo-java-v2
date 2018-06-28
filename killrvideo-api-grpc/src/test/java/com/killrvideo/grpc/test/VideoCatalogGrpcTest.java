package com.killrvideo.grpc.test;

import org.junit.Test;

import com.killrvideo.grpc.client.KillrVideoGrpcClient;

import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetLatestVideoPreviewsRequest;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetLatestVideoPreviewsResponse;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.VideoPreview;

/**
 * Sample Operation on GRPC.
 * 
 * @author DataStax Evangelist Team
 */
public class VideoCatalogGrpcTest {
    
    @Test
    public void testGetLatestVideos() throws Exception {
        
        //-->
        KillrVideoGrpcClient killrVideoClient = new KillrVideoGrpcClient("localhost", 8899);
        // <--
        
        // Request
        GetLatestVideoPreviewsRequest req = 
                GetLatestVideoPreviewsRequest.newBuilder().setPageSize(5).build();
        
        GetLatestVideoPreviewsResponse res = 
                killrVideoClient.getVideoCatalogService().getLatestVideoPreviews(req);

        // Async
        Thread.sleep(1000);
        
        System.out.println("VideoCatalog.getLatestVideoPreviews() : " + res.getVideoPreviewsCount() + " video(s) found.");
        for(int i=0;i< res.getVideoPreviewsCount() ;i++) {
            VideoPreview vp = res.getVideoPreviews(i);
            System.out.println("-> #" + i + " " + vp.getName() + " - " + vp.getVideoId());
        }
    }

}
