package com.killrvideo.dse.dao;

import static com.killrvideo.core.utils.FutureUtils.asCompletableFuture;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.graph.GraphNode;
import com.datastax.driver.dse.graph.GraphResultSet;
import com.datastax.driver.dse.graph.GraphStatement;
import com.datastax.driver.dse.graph.Vertex;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.Result;
import com.datastax.dse.graph.api.DseGraph;
import com.google.common.collect.Sets;
import com.killrvideo.dse.dao.dto.ResultListPage;
import com.killrvideo.dse.graph.KillrVideoTraversal;
import com.killrvideo.dse.graph.KillrVideoTraversalConstants;
import com.killrvideo.dse.graph.KillrVideoTraversalSource;
import com.killrvideo.dse.graph.__;
import com.killrvideo.dse.model.SchemaConstants;
import com.killrvideo.dse.model.User;
import com.killrvideo.dse.model.Video;
import com.killrvideo.dse.model.VideoRatingByUser;
import com.killrvideo.dse.utils.DseUtils;

/**
 * Implementations of operation for Videos.
 *
 * @author DataStax evangelist team.
 */
@Repository
public class SuggestedVideosDseDao extends AbstractDseDao implements KillrVideoTraversalConstants {

    /** Logger for DAO. */
    private static final Logger LOGGER = LoggerFactory.getLogger(SuggestedVideosDseDao.class);

    /** Mapper to ease queries. */
    protected Mapper< Video > mapperVideo;
    
    /** Precompile statements to speed up queries. */
    private PreparedStatement findRelatedVideos;
    
    @Autowired
    private KillrVideoTraversalSource traversalSource;
    
    /**
     * Default constructor.
     */
    public SuggestedVideosDseDao() {
        super();
    }
    
    /**
     * Allow explicit intialization for test purpose.
     */
    public SuggestedVideosDseDao(DseSession dseSession) {
        super(dseSession);
    }

    /** {@inheritDoc} */
    @Override
    protected void initialize() {
        mapperVideo = mappingManager.mapper(Video.class);
        String keyspaceVideo   = mapperVideo.getTableMetadata().getKeyspace().getName();
        String tableNameVideo  = mapperVideo.getTableMetadata().getName();
        RegularStatement queryFindRelatedVideos = QueryBuilder
                .select().all()
                .from(keyspaceVideo, tableNameVideo)
                .where(QueryBuilder.eq(SOLR_QUERY, QueryBuilder.bindMarker()));
        findRelatedVideos = dseSession.prepare(queryFindRelatedVideos);
    }
    
    /**
     * Get Pageable result for related video.
     **/
    public CompletableFuture< ResultListPage<Video> > getRelatedVideos(UUID videoId, int fetchSize, Optional<String> pagingState) {
        CompletableFuture<Result<Video>> relatedVideosFuture = findVideoById(videoId).thenCompose(video -> {
            BoundStatement stmt = createStatementToSearchVideos(videoId, fetchSize, pagingState);
            return asCompletableFuture(mapperVideo.mapAsync(dseSession.executeAsync(stmt)));
        });
        // so far I got a Result<Video> async, need to fetch only expected page and save paging state
        return relatedVideosFuture.< ResultListPage<Video> > thenApply(ResultListPage::new);
    }
    
    /**
     * Search for videos.
     *
     * @param userid
     *      current userid,
     * @return
     *         Async Page
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public CompletableFuture< List<Video> > getSuggestedVideosForUser(UUID userid) {
        
        // Parameters validation
        Assert.notNull(userid, "videoid is required to update statistics");
        
        // Build statement
        KillrVideoTraversal graphTraversal = traversalSource
                .users(userid.toString())
                .recommendByUserRating(100, 4, 250, 10);
        GraphStatement graphStatement = DseGraph.statementFromTraversal(graphTraversal);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Recommend TRAVERSAL is {} ",  DseUtils.displayGraphTranserval(graphTraversal));
        }
        
        // Execute Sync
        CompletableFuture<GraphResultSet> futureRs = 
                asCompletableFuture(dseSession.executeGraphAsync(graphStatement));
        
       // Mapping to expected List
       return futureRs.thenApply(
               rs -> rs.all().stream().map(this::mapGraphNode2Video).collect(Collectors.toList()));   
    }
    
    /**
     * Subscription is done in dedicated service 
     * {@link EventConsumerService}. (killrvideo-messaging)
     * 
     * Below we are using our KillrVideoTraversal DSL (Domain Specific Language)
     * to create our video vertex, then within add() we connect up the user responsible
     * for uploading the video with the "uploaded" edge, and then follow up with
     * any and all tags using the "taggedWith" edge.  Since we may have multiple
     * tags make sure to loop through and get them all in there.
     *
     * Also note the use of add().  Take a look at Stephen's blog here
     * -> https://www.datastax.com/dev/blog/gremlin-dsls-in-java-with-dse-graph for more information.
     * This essentially allows us to chain multiple commands (uploaded and (n * taggedWith) in this case)
     * while "preserving" our initial video traversal position. Since the video vertex passes
     * through each step we do not need to worry about traversing back to video for each step
     * in the chain.
     */
    @SuppressWarnings({"rawtypes","unchecked"})
    public void updateGraphNewVideo(Video video) {
        
        final KillrVideoTraversal traversal = traversalSource.video(
                video.getVideoid(), video.getName(), 
                video.getAddedDate(), video.getDescription(), 
                video.getPreviewImageLocation())
        .add(__.uploaded(video.getUserid()));

        Sets.newHashSet(video.getTags()).forEach(tag -> {
            traversal.add(__.taggedWith(tag,  new Date()));
        });

        /**
         * Now that our video is successfully applied lets
         * insert that video into our graph for the recommendation engine
         */
        GraphStatement gStatement = DseGraph.statementFromTraversal(traversal);
        asCompletableFuture(dseSession.executeGraphAsync(gStatement)).whenComplete((graphResultSet, ex) -> {
            if (graphResultSet != null) {
                LOGGER.debug("Added video vertex, uploaded, and taggedWith edges: " + graphResultSet.all());
            }  else {
                //TODO: Potentially add some robustness code here
                LOGGER.warn("Error handling YouTubeVideoAdded for graph: " + ex);
            }
        });
    }
    
    /**
     * Subscription is done in dedicated service 
     * {@link EventConsumerService}. (killrvideo-messaging)
     * 
     * This will create a user vertex in our graph if it does not already exist.
     * 
     * @param user
     *      current user
     */
    @SuppressWarnings({"rawtypes","unchecked"})
    public void updateGraphNewUser(User user) {
        final KillrVideoTraversal traversal = traversalSource.user(user.getUserid(), user.getEmail(), user.getCreatedAt());
        GraphStatement gStatement = DseGraph.statementFromTraversal(traversal);
        asCompletableFuture(dseSession.executeGraphAsync(gStatement)).whenComplete((graphResultSet, ex) -> {
            if (graphResultSet != null) {
                LOGGER.debug("Added user vertex: " + graphResultSet.one());
            } else {
                //TODO: Potentially add some robustness code here
                LOGGER.warn("Error creating user vertex: " + ex);
            }
        });
    }
    
    /**
     * Subscription is done in dedicated service 
     * {@link EventConsumerService}. (killrvideo-messaging)
     * 
     * Note that if either the user or video does not exist in the graph
     * the rating will not be applied nor will the user or video be
     * automatically created in this case.  This assumes both the user and video
     * already exist.
     */
    @SuppressWarnings({"rawtypes","unchecked"})
    public void updateGraphNewUserRating(VideoRatingByUser vrbu) {
        final KillrVideoTraversal traversal = traversalSource.videos(vrbu.getVideoid().toString())
                                                             .add(__.rated(vrbu.getUserid(), vrbu.getRating()));
        GraphStatement gStatement = DseGraph.statementFromTraversal(traversal);
        asCompletableFuture(dseSession.executeGraphAsync(gStatement)).whenComplete((graphResultSet, ex) -> {
            if (graphResultSet != null) {
                LOGGER.debug("Added rating between user and video: " + graphResultSet.one());
            } else {
                //TODO: Potentially add some robustness code here
                LOGGER.warn("Error Adding rating between user and video: " + ex);
            }
        });
    }
    
    private Video mapGraphNode2Video(GraphNode node) {
        Vertex v = node.get(VERTEX_VIDEO).asVertex();
        Vertex u = node.get(VERTEX_USER).asVertex();
        Video video = new Video();
        video.setAddedDate(Date.from(v.getProperty("added_date").getValue().as(Instant.class)));
        video.setName(v.getProperty("name").getValue().asString());
        video.setPreviewImageLocation(v.getProperty("preview_image_location").getValue().asString());
        video.setVideoid(v.getId().get("videoId").as(UUID.class));
        video.setUserid(u.getId().get("userId").as(UUID.class));
        return video;
    }
    
    private CompletableFuture<Video> findVideoById(UUID videoId) {
       Assert.notNull(videoId, "videoid is required to update statistics");
       return asCompletableFuture(mapperVideo.getAsync(videoId));
    }
    
    /**
     * Use a Lucene based MoreLikeThis search with DSE Search
     * !mlt = perform MoreLikeThis
     * qf = More like this fields to consider
     * mindf = MLT Minimum Document Frequency - the frequency at which words will be ignored which do not occur in at least this many docs
     * mintf = MLT Minimum Term Frequency - the frequency below which terms will be ignored in the source doc
     * 
     * TODO Figure out what is going on with paging:driver returning strange results
     **/
    private BoundStatement createStatementToSearchVideos(UUID videoId, int fetchSize, Optional<String> pagingState) {
        final StringBuilder solrQuery = new StringBuilder();
        solrQuery.append("{\"q\":\"{!mlt qf=\\\"name tags description\\\" mindf=2 mintf=2}");
        solrQuery.append(videoId);
        solrQuery.append("\", \"paging\":\"off\"}");
        BoundStatement statement = findRelatedVideos.bind().setString(SchemaConstants.SOLR_QUERY, solrQuery.toString());
        pagingState.ifPresent( x -> statement.setPagingState(PagingState.fromString(x)));
        statement.setFetchSize(fetchSize);
        return statement;
    }
  
}
