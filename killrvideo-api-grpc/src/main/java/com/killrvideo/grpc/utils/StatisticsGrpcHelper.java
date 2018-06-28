package com.killrvideo.grpc.utils;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import com.killrvideo.dse.model.VideoPlaybackStats;

import io.grpc.stub.StreamObserver;
import killrvideo.common.CommonTypes;
import killrvideo.common.CommonTypes.Uuid;
import killrvideo.statistics.StatisticsServiceOuterClass.GetNumberOfPlaysRequest;
import killrvideo.statistics.StatisticsServiceOuterClass.GetNumberOfPlaysResponse;
import killrvideo.statistics.StatisticsServiceOuterClass.PlayStats;
import killrvideo.statistics.StatisticsServiceOuterClass.RecordPlaybackStartedRequest;

/**
 * Helper and mappers for DAO <=> GRPC Communications
 *
 * @author DataStax Evangelist Team
 */
@Component
public class StatisticsGrpcHelper extends AbstractGrpcHelper {
    
    public void validateGrpcRequest_GetNumberPlays(Logger logger, GetNumberOfPlaysRequest request, StreamObserver<?> streamObserver) {
        final StringBuilder errorMessage = initErrorString(request);
        boolean isValid = true;
        if (request.getVideoIdsCount() <= 0) {
            errorMessage.append("\t\tvideo ids should be provided for get number of plays request\n");
            isValid = false;
        }
        if (request.getVideoIdsCount() > 20) {
            errorMessage.append("\t\tcannot do a get more than 20 videos at once for get number of plays request\n");
            isValid = false;
        }
        for (CommonTypes.Uuid uuid : request.getVideoIdsList()) {
            if (uuid == null || isBlank(uuid.getValue())) {
                errorMessage.append("\t\tprovided UUID values cannot be null or blank for get number of plays request\n");
                isValid = false;
            }
        }
        Assert.isTrue(validate(logger, streamObserver, errorMessage, isValid), "Invalid parameter for 'getNumberPlays'");
    }
    
    public void validateGrpcRequest_RecordPlayback(Logger logger, RecordPlaybackStartedRequest request, StreamObserver<?> streamObserver) {
        final StringBuilder errorMessage = initErrorString(request);
        boolean isValid = true;

        if (request.getVideoId() == null || isBlank(request.getVideoId().getValue())) {
            errorMessage.append("\t\tvideo id should be provided for record playback started request\n");
            isValid = false;
        }
        Assert.isTrue(validate(logger, streamObserver, errorMessage, isValid), "Invalid parameter for 'recordPlaybackStarted'");
    }    
    
    public GetNumberOfPlaysResponse buildGetNumberOfPlayResponse(GetNumberOfPlaysRequest grpcReq, List<VideoPlaybackStats> videoList) {
        final Map<Uuid, PlayStats> result = videoList.stream()
                .filter(x -> x != null)
                .map(this::mapVideoPlayBacktoPlayStats)
                .collect(Collectors.toMap(x -> x.getVideoId(), x -> x));

        final GetNumberOfPlaysResponse.Builder builder = GetNumberOfPlaysResponse.newBuilder();
        for (Uuid requestedVideoId : grpcReq.getVideoIdsList()) {
            if (result.containsKey(requestedVideoId)) {
                builder.addStats(result.get(requestedVideoId));
            } else {
                builder.addStats(PlayStats
                        .newBuilder()
                        .setVideoId(requestedVideoId)
                        .setViews(0L)
                        .build());
            }
        }
        return builder.build();
    }
    
    /**
     * Mapping to generated GPRC beans.
     */
    private PlayStats mapVideoPlayBacktoPlayStats(VideoPlaybackStats v) {
        return PlayStats.newBuilder()
                .setVideoId(GrpcMapper.uuidToUuid(v.getVideoid()))
                .setViews(Optional.ofNullable(v.getViews()).orElse(0L)).build();
    }
    

}
