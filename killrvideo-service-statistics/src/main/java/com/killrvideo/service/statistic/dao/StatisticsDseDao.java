package com.killrvideo.service.statistic.dao;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.mapping.Mapper;
import com.killrvideo.dse.dao.DseDaoSupport;
import com.killrvideo.service.statistic.dto.VideoPlaybackStats;
import com.killrvideo.utils.FutureUtils;

/**
 * Implementations of operation for Videos.
 *
 * @author DataStax Developer Advocates team.
 */
@Repository
public class StatisticsDseDao extends DseDaoSupport {

    /** Table Names. */
    public static final String TABLENAME_PLAYBACK_STATS = "video_playback_stats";
    
    /** Mapper to ease queries. */
    protected  Mapper< VideoPlaybackStats > mappervideoPlaybackStats;
    
    /** Precompile statements to speed up queries. */
    private PreparedStatement incrRecordPlayBacks;
    
    /**
     * Default constructor.
     */
    public StatisticsDseDao() {
        super();
    }
    
    /**
     * Allow explicit intialization for test purpose.
     */
    public StatisticsDseDao(DseSession dseSession) {
        super(dseSession);
    }

    /** {@inheritDoc} */
    @Override
    protected void initialize() {
        
        mappervideoPlaybackStats = mappingManager.mapper(VideoPlaybackStats.class);
        
        // use incr() call to increment my counter field 
        // https://docs.datastax.com/en/developer/java-driver/3.2/faq/#how-do-i-increment-counters-with-query-builder
        String keyspacePlayback  = mappervideoPlaybackStats.getTableMetadata().getKeyspace().getName();
        String tableNamePlayback = mappervideoPlaybackStats.getTableMetadata().getName();
        RegularStatement queryIncPaylBack = QueryBuilder
                .update(keyspacePlayback, tableNamePlayback)
                .with(QueryBuilder.incr(VideoPlaybackStats.COLUMN_VIEWS))
                .where(QueryBuilder.eq(VideoPlaybackStats.COLUMN_VIDEOID, QueryBuilder.bindMarker()));
        incrRecordPlayBacks = dseSession.prepare(queryIncPaylBack);
        incrRecordPlayBacks.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    }
    
    /**
     * Increment counter in DB (Async).
     *
     * @param videoId
     *      current videoid.
     */
    public CompletableFuture<Void> recordPlaybackStartedAsync(UUID videoId) {
        Assert.notNull(videoId, "videoid is required to update statistics");
        BoundStatement bound = incrRecordPlayBacks.bind().setUUID(VideoPlaybackStats.COLUMN_VIDEOID, videoId);
        return FutureUtils.asCompletableFuture(dseSession.executeAsync(bound)).<Void>thenApply(c -> null);
    }
    
    /**
     * Search for each videoid.
     *
     * @param listOfVideoIds
     *      list of EXISTING videoid
     * @return
     *      future for the list
     */
    public CompletableFuture<List<VideoPlaybackStats>> getNumberOfPlaysAsync(List<UUID> listOfVideoIds) {
        Assert.notNull(listOfVideoIds, "videoid list cannot be null");
        
        // Create a future for each entry
        final List<CompletableFuture<VideoPlaybackStats>> futureList = listOfVideoIds.stream()
                      .map(mappervideoPlaybackStats::getAsync)
                      .map(FutureUtils::asCompletableFuture)
                      .collect(Collectors.toList());

        // List <Future> => Future<List> ! Amazing
        return CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]))
                                .thenApply(v -> futureList.stream()
                                                          .map(CompletableFuture::join)
                                                          .collect(Collectors.toList()));
    }        
  
}
