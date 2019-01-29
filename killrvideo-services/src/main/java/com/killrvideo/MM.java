package com.killrvideo;

import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetLatestVideoPreviewsRequest;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.GetLatestVideoPreviewsResponse;
import killrvideo.video_catalog.VideoCatalogServiceOuterClass.VideoPreview;

public class MM {

    
    public static void main(String[] args) throws InterruptedException {
        KillrvideoServicesGrpcClient killrVideoClient = new KillrvideoServicesGrpcClient("10.0.75.1", 30700);
        // Request
        GetLatestVideoPreviewsRequest req = 
                GetLatestVideoPreviewsRequest.newBuilder().setPageSize(5).build();
        GetLatestVideoPreviewsResponse res = 
                killrVideoClient.videoCatalogServiceGrpcClient.getLatestVideoPreviews(req);
        // Async
        Thread.sleep(1000);
        System.out.println("VideoCatalog.getLatestVideoPreviews() : " + res.getVideoPreviewsCount() + " video(s) found.");
        for(int i=0;i< res.getVideoPreviewsCount() ;i++) {
            VideoPreview vp = res.getVideoPreviews(i);
            System.out.println("-> #" + i + " " + vp.getName() + " - " + vp.getVideoId());
        }
    }
}
