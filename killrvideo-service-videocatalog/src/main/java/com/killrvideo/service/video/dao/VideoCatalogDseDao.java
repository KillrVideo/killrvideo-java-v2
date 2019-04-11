package com.killrvideo.service.video.dao;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.Result;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.killrvideo.dse.dao.DseDaoSupport;
import com.killrvideo.dse.dto.CustomPagingState;
import com.killrvideo.dse.dto.ResultListPage;
import com.killrvideo.dse.dto.Video;
import com.killrvideo.service.video.dto.LatestVideo;
import com.killrvideo.service.video.dto.LatestVideosPage;
import com.killrvideo.service.video.dto.UserVideo;
import com.killrvideo.utils.FutureUtils;

/**
 * Implementations of operation for Videos.
 *
 * @author DataStax Developer Advocates team.
 */
@Repository
public class VideoCatalogDseDao extends DseDaoSupport {

    /** Constants. */
    public static final int     MAX_DAYS_IN_PAST_FOR_LATEST_VIDEOS = 7;
    public static final int     LATEST_VIDEOS_TTL_SECONDS          = MAX_DAYS_IN_PAST_FOR_LATEST_VIDEOS * 24 * 3600;
    public static final Pattern PARSE_LATEST_PAGING_STATE          = Pattern.compile("((?:[0-9]{8}_){7}[0-9]{8}),([0-9]),(.*)");
    
    /** Formatting date. */
    public static final SimpleDateFormat  SDF           = new SimpleDateFormat("yyyyMMdd");
    public static final DateTimeFormatter DATEFORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
    
    /** Table Name of Latest Video. */
    public static final String TABLENAME_LATEST_VIDEOS = "latest_videos";
    public static final String TABLENAME_USER_VIDEOS   = "user_videos";
    
    /** Loger for that class. */
    private static Logger LOGGER = LoggerFactory.getLogger(VideoCatalogDseDao.class);
    
    /** Mapper. */
    private Mapper< Video >       videoMapper;
    private Mapper< UserVideo >   userVideosMapper;
    private Mapper< LatestVideo > latestVideosMapper;
    
    /** Related table name. */
    private String videoTableName;
    
    /** Related keyspace name. */
    private String videoKeyspace;
    
    /** Mapper. */
    
    /** Related table name. */
    private String userVideoTableName;
    
    /** Related keyspace name. */
    private String userVideoKeyspace;
    
    /** Mapper. */
    
    /** Related table name. */
    private String latestVideoTableName;
    
    /** Related keyspace name. */
    private String latestVideoKeyspace;
    
    /** Prepare Statements 'insertYoutubeVideo'. */
    private PreparedStatement submitYouTubeVideo_insertVideo;
    private PreparedStatement submitYouTubeVideo_insertUserVideo;
    private PreparedStatement submitYouTubeVideo_insertLatestVideo;
    
    /** Prepare Statements 'getLatestVideso'. */
    private PreparedStatement latestVideoPreview_startingPointPrepared;
    private PreparedStatement latestVideoPreview_noStartingPointPrepared;
    
    /** Prepare Statements 'getUserVideo'. */
    protected PreparedStatement userVideoPreview_startingPointPrepared;
    protected PreparedStatement userVideoPreview_noStartingPointPrepared;
    
    /**
     * Default constructor.
     */
    public VideoCatalogDseDao() {
        super();
    }
    
    /**
     * Allow explicit intialization for test purpose.
     */
    public VideoCatalogDseDao(DseSession dseSession) {
        super(dseSession);
    }
    
    /**
     * Set the following up in PostConstruct because 1) we have to
     * wait until after dependency injection for these to work,
     * and 2) we only want to load the prepared statements once at
     * the start of the service.  From here the prepared statements should
     * be cached on our Cassandra nodes.
     *
     * Note I am not using QueryBuilder with bindmarker() for these
     * statements.  This is not a value judgement, just a different way of doing it.
     * Take a look at some of the other services to see QueryBuilder.bindmarker() examples.
     */
    @PostConstruct
    protected void initialize () {
        latestVideosMapper   =  mappingManager.mapper(LatestVideo.class);
        latestVideoTableName = latestVideosMapper.getTableMetadata().getName() ;
        latestVideoKeyspace  = latestVideosMapper.getTableMetadata().getKeyspace().getName();
        latestVideoPreview_startingPointPrepared = dseSession.prepare(
                "SELECT * " +
                "FROM " +  latestVideoKeyspace + "." + latestVideoTableName + " " +
                "WHERE yyyymmdd = :ymd " +
                "AND (added_date, videoid) <= (:ad, :vid)");
        latestVideoPreview_startingPointPrepared.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        latestVideoPreview_noStartingPointPrepared = dseSession.prepare(
                    "SELECT * " +
                    "FROM " + latestVideoKeyspace + "." + latestVideoTableName + " " +
                    "WHERE yyyymmdd = :ymd ");
        latestVideoPreview_noStartingPointPrepared.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        
        userVideosMapper = mappingManager.mapper(UserVideo.class);
        userVideoTableName = userVideosMapper.getTableMetadata().getName();
        userVideoKeyspace  = userVideosMapper.getTableMetadata().getKeyspace().getName();
        prepareStatementsUserVideo();
        
        videoMapper = mappingManager.mapper(Video.class);
        videoTableName = videoMapper.getTableMetadata().getName();
        videoKeyspace  = videoMapper.getTableMetadata().getKeyspace().getName();
        prepareStatementsInsertVideo();
    }
    
    /**
     * Insert a VIDEO in the DB.
     */
    public void insertVideo(Video v) {
        dseSession.execute(createStatementInsertVideo(v));
    }
    
    /**
     * Build the first paging state if one does not already exist and return an object containing 3 elements
     * representing the initial state (List<String>, Integer, String).
     * @return CustomPagingState
     */
    public CustomPagingState buildFirstCustomPagingState() {
        return new CustomPagingState()
                .currentBucket(0)
                .cassandraPagingState(null)
                .listOfBuckets(LongStream.rangeClosed(0L, 7L).boxed()
                        .map(Instant.now().atZone(ZoneId.systemDefault())::minusDays)
                        .map(x -> x.format(VideoCatalogDseDao.DATEFORMATTER))
                        .collect(Collectors.toList()));
    }
    
    /**
     * Insert a VIDEO in the DB (ASYNC).
     */
    public CompletableFuture<Void> insertVideoAsync(Video v) {
        CompletableFuture<Void> cfv = new CompletableFuture<>();
        Futures.addCallback(dseSession.executeAsync(createStatementInsertVideo(v)), new FutureCallback<ResultSet>() {
            // Propagation exception to handle it in the EXPOSITION LAYER. 
            public void onFailure(Throwable ex) { cfv.completeExceptionally(ex); }
            
            // Insertion return Void and we can put null in the complete
            public void onSuccess(ResultSet rs) { cfv.complete(null); }
        });
        return cfv;
    }
    
    public CompletableFuture<Video> getVideoById(UUID videoid) {
        return FutureUtils.asCompletableFuture(videoMapper.getAsync(videoid));
    }
    
    public CompletableFuture<List<Video>> getVideoPreview(List<UUID> listofVideoId) {
        Assert.notNull(listofVideoId, "videoid list cannot be null");
        
        // Create a future for each entry
        final List<CompletableFuture<Video>> futureList = listofVideoId.stream()
                      .map(videoMapper::getAsync)
                      .map(FutureUtils::asCompletableFuture)
                      .collect(Collectors.toList());

        // List <Future> => Future<List> ! Amazing
        return CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]))
                                .thenApply(v -> futureList.stream()
                                                          .map(CompletableFuture::join)
                                                          .collect(Collectors.toList()));     
    }
    
    /**
     * Read a page of video preview for a user.
     * 
     * @param userId
     *      user unique identifier
     * @param startingVideoId
     *      starting video if paging
     * @param startingAddedDate
     *      added date if paging
     * @param pagingState
     *      paging state if paging
     * @return
     *      requested video (page)
     */
    public CompletableFuture< ResultListPage <UserVideo> > getUserVideosPreview(UUID userId, 
            Optional<UUID> startingVideoId, 
            Optional<Date> startingAddedDate,
            Optional<Integer> pageSize,
            Optional<String>  pagingState) {
        
        // Create correct query
        BoundStatement bound;
        /**
         * If startingAddedDate and startingVideoId are provided,
         * we do NOT use the paging state
         */
        if (startingVideoId.isPresent() && startingAddedDate.isPresent()) {
            /**
             * The startingPointPrepared statement can be found at the top
             * of the class within PostConstruct
             */
            bound = userVideoPreview_startingPointPrepared.bind()
                    .setUUID("uid", userId)
                    .setTimestamp("ad", startingAddedDate.get())
                    .setUUID("vid", startingVideoId.get());
            LOGGER.debug("Current query is: " + bound.preparedStatement().getQueryString());
        } else {
            /**
             * The noStartingPointPrepared statement can be found at the top
             * of the class within PostConstruct
             */
            bound = userVideoPreview_noStartingPointPrepared.bind().setUUID("uid", userId);
            LOGGER.debug("Current query is: " + bound.preparedStatement().getQueryString());
        }
        pageSize.ifPresent(bound::setFetchSize);
        pagingState.ifPresent( x -> bound.setPagingState(PagingState.fromString(x)));
        
        // Execute Query
        return FutureUtils.asCompletableFuture(userVideosMapper.mapAsync(dseSession.executeAsync(bound)))
                          .< ResultListPage<UserVideo> > thenApply(ResultListPage::new);
    }
    
    /**
     * Latest video partition key is the Date. As such we need to perform a query per date. As the user
     * ask for a number of video on a given page we may have to trigger several queries, on for each day.
     * To do it we implement a couple 
     * 
     * For those of you wondering where the call to fetchMoreResults() is take a look here for an explanation.
     * https://docs.datastax.com/en/drivers/java/3.2/com/datastax/driver/core/PagingIterable.html#getAvailableWithoutFetching--
     * 
     * Quick summary, when getAvailableWithoutFetching() == 0 it automatically calls fetchMoreResults()
     * We could use it to force a fetch in a "prefetch" scenario, but that is not what we are doing here.
     * 
     * @throws ExecutionException
     *      error duing invoation 
     * @throws InterruptedException
     *      error in asynchronism 
     */
    public LatestVideosPage getLatestVideoPreviews(CustomPagingState cpState, int pageSize, Optional<Date> startDate, Optional<UUID> startVid)
    throws InterruptedException, ExecutionException {
        LatestVideosPage returnedPage = new LatestVideosPage();
        LOGGER.debug("Looking for {} latest video(s)", pageSize);
      
        // Flag to syncrhonize usage of cassandra paging state
        final AtomicBoolean isCassandraPageState = new AtomicBoolean(false);
       
        do {
          
          // (1) - Paging state (custom or cassandra)
          final Optional<String> pagingState = 
                  Optional.ofNullable(cpState.getCassandraPagingState())  // Only if present .get()
                          .filter(StringUtils::isNotBlank)                // ..and not empty
                          .filter(pg -> !isCassandraPageState.get());     // ..and cassandra paging is off
          
          // (2) - Build Query for a single bucket (=a single date)
          BoundStatement stmt = buildStatementLatestVideoPage(
                  cpState.getCurrentBucketValue(),          // Current Bucket Date yyyymmdd  
                  pagingState,                              // Custom or cassandra pageing state 
                  isCassandraPageState,                     // Flag to use and update cassandra paging 
                  startDate, startVid,                      // Optional Parameters for filtering
                  pageSize - returnedPage.getResultSize()); // Number of element to retrieve from current query
          if (LOGGER.isDebugEnabled()) {
              LOGGER.debug(" + Executing {} with :ymd='{}' fetchSize={} and pagingState={}", 
                  stmt.preparedStatement().getQueryString(), cpState.getCurrentBucketValue(), 
                  stmt.getFetchSize(), pagingState.isPresent());
          }
          stmt.setConsistencyLevel(ConsistencyLevel.ONE);
          
          // (3) - Execute Query Asynchronously
          CompletableFuture< LatestVideosPage > cfv = new CompletableFuture<>();
          Futures.addCallback(latestVideosMapper.mapAsync(dseSession.executeAsync(stmt)), new FutureCallback<Result<LatestVideo>>() {
              
              /* Mapping is performed in 'mapLatestVideosResultAsPage' through iterate on results to convert
               * Result<Bean> as 'LatestVideosPage'.
               */
              public void onSuccess(Result<LatestVideo> rs) { cfv.complete(mapLatestVideosResultAsPage(rs)); }
              
              // Propagation exception to handle it in the EXPOSITION LAYER. 
              public void onFailure(Throwable ex) { cfv.completeExceptionally(ex); }
          });
          
          // (4) - Wait for result before triggering auery for page N+1
          LatestVideosPage currentPage = cfv.get();
          returnedPage.getListOfPreview().addAll(currentPage.getListOfPreview());
          if (LOGGER.isDebugEnabled()) {
              LOGGER.debug(" + bucket:{}/{} with results:{}/{} and pagingState:{}",cpState.getCurrentBucket(), 
                      cpState.getListOfBucketsSize(), returnedPage.getResultSize(), pageSize, returnedPage.getCassandraPagingState());
          }
          
          // (5) Update NEXT PAGE BASE on current status
          if (returnedPage.getResultSize() == pageSize) {
              if (!StringUtils.isBlank(currentPage.getCassandraPagingState())) {
                  returnedPage.setNextPageState(createPagingState(cpState.getListOfBuckets(), 
                          cpState.getCurrentBucket(), currentPage.getCassandraPagingState()));
                  LOGGER.debug(" + Exiting because we got enought results.");
              }
          // --> Start from the beginning of the next bucket since we're out of rows in this one
          } else if (cpState.getCurrentBucket() == cpState.getListOfBucketsSize() - 1) {
              returnedPage.setNextPageState(createPagingState(cpState.getListOfBuckets(), cpState.getCurrentBucket() + 1, ""));
              LOGGER.debug(" + Exiting because we are out of Buckets even if not enough results");
          }
              
          // (6) Move to next BUCKET
          cpState.incCurrentBucketIndex();
            
        } while ( (returnedPage.getListOfPreview().size() < pageSize)               // Result has enough element to fill the page
                   && cpState.getCurrentBucket() < cpState.getListOfBucketsSize()); // No nore bucket available
        
        return returnedPage;
    }
    
    /**
     * Dynamically build statement based on arguments startingDate, videoId.
     */
    private BoundStatement buildStatementLatestVideoPage(
            String yyyymmdd, Optional<String> pagingState, AtomicBoolean cassandraPagingStateUsed, 
            Optional<Date> startingAddedDate, Optional<UUID> startingVideoId, int recordNeeded) {
        BoundStatement bound;
        if (startingAddedDate.isPresent() && startingVideoId.isPresent()) {
            bound = latestVideoPreview_startingPointPrepared.bind()
                    .setString("ymd", yyyymmdd)
                    .setTimestamp("ad", startingAddedDate.get())
                    .setUUID("vid", startingVideoId.get());
        } else {
             bound = latestVideoPreview_noStartingPointPrepared.bind().setString("ymd", yyyymmdd);
        }
        bound.setFetchSize(recordNeeded);
        // Use custom paging state if provided and no cassandra triggered
        pagingState.ifPresent(x -> {
            bound.setPagingState(PagingState.fromString(x));
            cassandraPagingStateUsed.compareAndSet(false, true);
        });
        return bound;
    }
    
    /**
     * Create statment to populate 3 tables in the same time.
     *
     * @param v
     *      current video to create
     * @param location
     *      location 0=YouTube
     * @return
     *      statement
     */
    private BatchStatement createStatementInsertVideo(Video v) {
        final Date   now      = new Date();
        final String yyyyMMdd = SDF.format(now);
        final BoundStatement insertVideo = submitYouTubeVideo_insertVideo.bind()
                .setUUID("videoid", v.getVideoid())
                .setUUID("userid",  v.getUserid())
                .setString("name",  v.getName())
                .setString("description", v.getDescription())
                .setString("location", v.getLocation())
                .setInt("location_type", v.getLocationType())
                .setString("preview_image_location", v.getPreviewImageLocation())
                .setSet("tags", v.getTags())
                .setTimestamp("added_date", now);
        final BoundStatement insertUserVideo = submitYouTubeVideo_insertUserVideo.bind()
                .setUUID("userid", v.getUserid())
                .setUUID("videoid", v.getVideoid())
                .setString("name", v.getName())
                .setString("preview_image_location",  v.getPreviewImageLocation())
                .setTimestamp("added_date", now);
        final BoundStatement insertLatestVideo = submitYouTubeVideo_insertLatestVideo.bind()
                .setString("yyyymmdd", yyyyMMdd)
                .setUUID("userid", v.getUserid())
                .setUUID("videoid", v.getVideoid())
                .setString("name", v.getName())
                .setString("preview_image_location", v.getPreviewImageLocation())
                .setTimestamp("added_date", now);
        /** Logged batch insert for automatic retry. */
        final BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.LOGGED);
        batchStatement.add(insertVideo);
        batchStatement.add(insertUserVideo);
        batchStatement.add(insertLatestVideo);
        batchStatement.setDefaultTimestamp(now.getTime());
        return batchStatement;
    }
    
    /**
     * Build statements.
     */
    private void prepareStatementsUserVideo() {
        
        userVideoPreview_startingPointPrepared = dseSession.prepare(
                    "SELECT * " +
                    "FROM " + userVideoKeyspace + "." + userVideoTableName + " " +
                    "WHERE userid = :uid " +
                    "AND (added_date, videoid) <= (:ad, :vid)"
        ).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        
        userVideoPreview_noStartingPointPrepared = dseSession.prepare(
                        "SELECT * " +
                        "FROM " + userVideoKeyspace + "." + userVideoTableName + " " +
                        "WHERE userid = :uid "
        ).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    }
    
    /**
     * Create prepare statements
     */
    private void prepareStatementsInsertVideo() {
        submitYouTubeVideo_insertVideo = dseSession.prepare(
                QueryBuilder.insertInto(videoKeyspace, videoTableName)
                        .value("videoId", QueryBuilder.bindMarker())
                        .value("userId", QueryBuilder.bindMarker())
                        .value("name", QueryBuilder.bindMarker())
                        .value("description", QueryBuilder.bindMarker())
                        .value("location", QueryBuilder.bindMarker())
                        .value("location_type", QueryBuilder.bindMarker())
                        .value("preview_image_location", QueryBuilder.bindMarker())
                        .value("tags", QueryBuilder.bindMarker())
                        .value("added_date", QueryBuilder.bindMarker())
        ).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        submitYouTubeVideo_insertUserVideo = dseSession.prepare(
                QueryBuilder.insertInto(userVideoKeyspace, userVideoTableName)
                        .value("userid", QueryBuilder.bindMarker())
                        .value("videoid", QueryBuilder.bindMarker())
                        .value("name", QueryBuilder.bindMarker())
                        .value("preview_image_location", QueryBuilder.bindMarker())
                        .value("added_date", QueryBuilder.bindMarker())
        ).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        submitYouTubeVideo_insertLatestVideo = dseSession.prepare(
                QueryBuilder.insertInto(latestVideoKeyspace, latestVideoTableName)
                        .value("yyyymmdd", QueryBuilder.bindMarker())
                        .value("userId", QueryBuilder.bindMarker())
                        .value("videoid", QueryBuilder.bindMarker())
                        .value("name", QueryBuilder.bindMarker())
                        .value("preview_image_location", QueryBuilder.bindMarker())
                        .value("added_date", QueryBuilder.bindMarker())
                        .using(QueryBuilder.ttl(LATEST_VIDEOS_TTL_SECONDS))
        ).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    }
    
    /**
     * Create a paging state string from the passed in parameters
     * @param buckets
     * @param bucketIndex
     * @param rowsPagingState
     * @return String
     */
    private String createPagingState(List<String> buckets, int bucketIndex, String rowsPagingState) {
        StringJoiner joiner = new StringJoiner("_");
        buckets.forEach(joiner::add);
        return joiner.toString() + "," + bucketIndex + "," + rowsPagingState;
    }
    
    /**
     * Mapping for Cassandra Result to expected bean.
     *
     * @param rs
     *      current result set
     * @return
     *      expected bean
     */
    private LatestVideosPage mapLatestVideosResultAsPage(Result<LatestVideo> rs) {
        LatestVideosPage resultPage = new LatestVideosPage();
        final PagingState state = rs.getAllExecutionInfo().get(0).getPagingState();
        if (null != state) {
            resultPage.setCassandraPagingState(state.toString());
        }
        int remaining = rs.getAvailableWithoutFetching();
        for (LatestVideo latestVideo : rs) {
            LOGGER.debug("Processing video: " + latestVideo.getVideoid());
            // Add each row to results
            resultPage.addLatestVideos(latestVideo);
            if (--remaining == 0) {
                break;
            }
        }
        return resultPage;
    }
    
}
