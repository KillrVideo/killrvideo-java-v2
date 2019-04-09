package com.killrvideo.service.sugestedvideo.dao;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
import com.killrvideo.dse.dao.DseDaoSupport;
import com.killrvideo.dse.dto.ResultListPage;
import com.killrvideo.dse.dto.Video;
import com.killrvideo.dse.graph.KillrVideoTraversal;
import com.killrvideo.dse.graph.KillrVideoTraversalConstants;
import com.killrvideo.dse.graph.KillrVideoTraversalSource;
import com.killrvideo.dse.graph.__;
import com.killrvideo.dse.utils.DseUtils;
import com.killrvideo.utils.FutureUtils;

/**
 * Implementations of operation for Videos.
 *
 * @author DataStax Developer Advocates team.
 */
@Repository
public class SuggestedVideosDseDao extends DseDaoSupport implements KillrVideoTraversalConstants {
    
    /** Logger for DAO. */
    private static final Logger LOGGER = LoggerFactory.getLogger(SuggestedVideosDseDao.class);

    /** Mapper to ease queries. */
    protected Mapper< Video > mapperVideo;
    
    /** Precompile statements to speed up queries. */
    private PreparedStatement findRelatedVideos;
    
    @Autowired
    private KillrVideoTraversalSource traversalSource;
    
    /**
     * Wrap search queries with "paging":"driver" to dynamically enable
     * paging to ensure we pull back all available results in the application.
     * https://docs.datastax.com/en/dse/6.0/cql/cql/cql_using/search_index/cursorsDeepPaging.html#cursorsDeepPaging__using-paging-with-cql-solr-queries-solrquery-Rim2GsbY
     */
    private String pagingDriverStart = "{\"q\":\"";
    private String pagingDriverEnd = "\", \"paging\":\"driver\"}";
    
    /**
     * Create a set of sentence conjunctions and other "undesirable"
     * words we will use later to exclude from search results.
     * Had to use .split() below because of the following conversation:
     * https://github.com/spring-projects/spring-boot/issues/501
     */
    @Value("#{'${killrvideo.search.ignoredWords}'.split(',')}")
    private Set<String> ignoredWords = new HashSet<>();
    
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
            BoundStatement stmt = createStatementToSearchVideos(video, fetchSize, pagingState);
            return FutureUtils.asCompletableFuture(mapperVideo.mapAsync(dseSession.executeAsync(stmt)));
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
        KillrVideoTraversal graphTraversal = traversalSource.users(userid.toString()).recommendByUserRating(5, 4, 1000, 5);
        GraphStatement graphStatement = DseGraph.statementFromTraversal(graphTraversal);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Recommend TRAVERSAL is {} ",  DseUtils.displayGraphTranserval(graphTraversal));
        }
        
        // Execute Sync
        CompletableFuture<GraphResultSet> futureRs = 
                FutureUtils.asCompletableFuture(dseSession.executeGraphAsync(graphStatement));
        
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
     * 
     * May be relevant to have a full sample traversal:
     * g.V().has("video","videoId", 6741b34e-03c7-4d83-bf55-deed496d6e03)
     *  .fold()
     *  .coalesce(__.unfold(),
     *   __.addV("video")
     *     .property("videoId",6741b34e-03c7-4d83-bf55-deed496d6e03))
     *     .property("added_date",Thu Aug 09 11:00:44 CEST 2018)
     *     .property("name","Paris JHipster Meetup #9")
     *     .property("description","xxxxxx")
     *     .property("preview_image_location","//img.youtube.com/vi/hOTjLOPXg48/hqdefault.jpg")
     *     // Add Edge
     *     .sideEffect(
     *        __.as("^video").coalesce(
     *           __.in("uploaded")
     *             .hasLabel("user")
     *             .has("userId",8a70e329-59f8-4e2e-aae8-1788c94e8410),
     *           __.V()
     *             .has("user","userId",8a70e329-59f8-4e2e-aae8-1788c94e8410)
     *             .addE("uploaded")
     *             .to("^video").inV())
     *      )
     *      // Tag with X (multiple times)
     *      .sideEffect(
     *        __.as("^video").coalesce(
     *          __.out("taggedWith")
     *            .hasLabel("tag")
     *            .has("name","X"),
     *          __.coalesce(
     *            __.V().has("tag","name","X"),
     *            __.addV("tag")
     *              .property("name","X")
     *               .property("tagged_date",Thu Aug 09 11:00:44 CEST 2018)
     *              ).addE("taggedWith").from("^video").inV())
     *       )
     *       // Tag with FF4j
     *       .sideEffect(
     *         __.as("^video").coalesce(
     *           __.out("taggedWith")
     *             .hasLabel("tag")
     *             .has("name","ff4j"),
     *           __.coalesce(
     *            __.V().has("tag","name","ff4j"),
     *            __.addV("tag")
     *              .property("name","ff4j")
     *              .property("tagged_date",Thu Aug 09 11:00:44 CEST 2018))
     *              .addE("taggedWith").from("^video").inV()))
     */
    @SuppressWarnings({"rawtypes","unchecked"})
    public void updateGraphNewVideo(Video video) {
        final KillrVideoTraversal traversal =
          // Add video Node
          traversalSource.video(video.getVideoid(), video.getName(), new Date(), video.getDescription(), video.getPreviewImageLocation())
          // Add Uploaded Edge
          .add(__.uploaded(video.getUserid()));
          // Add Tags Nodes and edges
          Sets.newHashSet(video.getTags()).forEach(tag -> {
            traversal.add(__.taggedWith(tag,  new Date()));
          });

        /**
         * Now that our video is successfully applied lets
         * insert that video into our graph for the recommendation engine
         */
        GraphStatement gStatement = DseGraph.statementFromTraversal(traversal);
        LOGGER.info("Traversal for 'updateGraphNewVideo' : {}", DseUtils.displayGraphTranserval(traversal));
        FutureUtils.asCompletableFuture(dseSession.executeGraphAsync(gStatement)).whenComplete((graphResultSet, ex) -> {
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
    public void updateGraphNewUser(UUID userId, String email, Date userCreation) {
        final KillrVideoTraversal traversal = traversalSource.user(userId, email, userCreation);
        GraphStatement gStatement = DseGraph.statementFromTraversal(traversal);
        LOGGER.info("Executed transversal for 'updateGraphNewUser' : {}", DseUtils.displayGraphTranserval(traversal));
        FutureUtils.asCompletableFuture(dseSession.executeGraphAsync(gStatement)).whenComplete((graphResultSet, ex) -> {
            if (graphResultSet != null) {
                LOGGER.debug("Added user vertex: " + graphResultSet.one());
            } else {
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
    public void updateGraphNewUserRating(String videoId, UUID userId, int rate) {
        final KillrVideoTraversal traversal = traversalSource.videos(videoId).add(__.rated(userId, rate));
        GraphStatement gStatement = DseGraph.statementFromTraversal(traversal);
        LOGGER.info("Executed transversal for 'updateGraphNewUserRating' : {}", DseUtils.displayGraphTranserval(traversal));
        FutureUtils.asCompletableFuture(dseSession.executeGraphAsync(gStatement)).whenComplete((graphResultSet, ex) -> {
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
       return FutureUtils.asCompletableFuture(mapperVideo.getAsync(videoId));
    }
    
    /**
     * Perform a query using DSE Search to find other videos that are similar
     * to the "request" video using terms parsed from the name, tags,
     * and description columns of the "request" video.
     *
     * The regex below will help us parse out individual words that we add to our
     * set. The set will automatically handle any duplicates that we parse out.
     * We can then use the end result termSet to query across the name, tags, and
     * description columns to find similar videos.
     */
    private BoundStatement createStatementToSearchVideos(Video video, int fetchSize, Optional<String> pagingState) {
        final String space = " ";
        final String eachWordRegEx = "[^\\w]";
        final String eachWordPattern = Pattern.compile(eachWordRegEx).pattern();

        final HashSet<String> termSet = new HashSet<>(50);
        Collections.addAll(termSet, video.getName().toLowerCase().split(eachWordPattern));
        Collections.addAll(video.getTags()); // getTags already returns a set
        Collections.addAll(termSet, video.getDescription().toLowerCase().split(eachWordPattern));
        termSet.removeAll(ignoredWords);
        termSet.removeIf(String::isEmpty);

        final String delimitedTermList = termSet.stream().map(Object::toString).collect(Collectors.joining(","));
        LOGGER.debug("delimitedTermList is : " + delimitedTermList);
       
        final StringBuilder solrQuery = new StringBuilder();
        solrQuery.append(pagingDriverStart);
        solrQuery.append("name:(").append(delimitedTermList).append(")^2").append(space);
        solrQuery.append("tags:(").append(delimitedTermList).append(")^4").append(space);
        solrQuery.append("description:").append(delimitedTermList);
        solrQuery.append(pagingDriverEnd);
        
        BoundStatement statement = findRelatedVideos.bind().setString(SOLR_QUERY, solrQuery.toString());
        pagingState.ifPresent( x -> statement.setPagingState(PagingState.fromString(x)));
        statement.setFetchSize(fetchSize);
        return statement;
    }
  
}
