package com.killrvideo.dse.dao;

import static com.killrvideo.core.utils.FutureUtils.asCompletableFuture;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.mapping.Mapper;
import com.google.common.reflect.TypeToken;
import com.killrvideo.dse.dao.dto.ResultListPage;
import com.killrvideo.dse.model.Video;

/**
 * Implementations of operation for Videos.
 *
 * @author DataStax evangelist team.
 */
@Repository
public class SearchDseDao extends AbstractDseDao {

	/** Logger for that class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SearchDseDao.class);
    
    /** Mapper to ease queries. */
    protected Mapper < Video >  mapperVideo;

    /** Precompile statements to speed up queries. */
    private PreparedStatement findSuggestedTags;
    private PreparedStatement findVideosByTags;
    private Set<String> excludeConjunctions = new HashSet<String>();
    
    /**
     * Default constructor.
     */
    public SearchDseDao() {
        super();
    }
    
    /**
     * Allow explicit intialization for test purpose.
     */
    public SearchDseDao(DseSession dseSession) {
        super(dseSession);
    }
    
    /** {@inheritDoc} */
    @PostConstruct
    protected void initialize () {
    	mapperVideo = mappingManager.mapper(Video.class);
    	
    	// Using Mapper and annotated bean to get constants value
        String keyspaceVideo   = mapperVideo.getTableMetadata().getKeyspace().getName();
        String tableNameVideo  = mapperVideo.getTableMetadata().getName();
        
        // Statement for tags
    	findSuggestedTags = dseSession.prepare(QueryBuilder
                         .select("name", "tags").from(keyspaceVideo, tableNameVideo)
                         .where(QueryBuilder.eq("solr_query", QueryBuilder.bindMarker())));
        findSuggestedTags.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
        
        // Statement for videos
        findVideosByTags = dseSession.prepare(QueryBuilder
                 		 .select().all().from(keyspaceVideo, tableNameVideo)
                 		 .where(QueryBuilder.eq("solr_query", QueryBuilder.bindMarker())));
        findVideosByTags.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
        // List of excluded
        excludeConjunctions = new HashSet<String>();;
        excludeConjunctions.addAll(Arrays.asList("and","or","but","nor",
        	"so","for","yet","after","as","till","to","the","at","in","not","of","this"));
    }
    
    /**
     * Do a Solr query against DSE search to find videos using Solr's ExtendedDisMax query parser. Query the
     * name, tags, and description fields in the videos table giving a boost to matches in the name and tags
     * fields as opposed to the description field
     * More info on ExtendedDisMax: http://wiki.apache.org/solr/ExtendedDisMax
     *
     * Notice the "paging":"driver" parameter.  This is to ensure we dynamically
     * enable pagination regardless of our nodes dse.yaml setting.
     * https://docs.datastax.com/en/dse/5.1/dse-dev/datastax_enterprise/search/cursorsDeepPaging.html#cursorsDeepPaging__srchCursorCQL
     */
    public CompletableFuture < ResultListPage<Video> > searchVideosAsync(String query, int fetchSize, Optional<String> pagingState) {
    	return asCompletableFuture(dseSession.executeAsync(
    	        createStatementToSearchVideos(query, fetchSize, pagingState)))
    	        .thenApply(rs -> new ResultListPage<Video>(rs, mapperVideo));
    }
    
    /**
     * Search for video in synchronous manner.
     *
     * @param query
     * 		current query
     * @param fetchSize
     * 		fetch size
     * @param pagingState
     * 		optional paging state
     * @return
     * 		result
     */
    public ResultListPage<Video> searchVideos(String query, int fetchSize, Optional<String> pagingState) {
    	BoundStatement stmt = createStatementToSearchVideos(query, fetchSize, pagingState);
    	return new ResultListPage<Video>(dseSession.execute(stmt), mapperVideo);
    }
    
    /**
     * Do a query against DSE search to find query suggestions using a simple search.
     * The search_suggestions "column" references a field we created in our search index
     * to store name and tag data.
     *
     * Notice the "paging":"driver" parameter.  This is to ensure we dynamically
     * enable pagination regardless of our nodes dse.yaml setting.
     * https://docs.datastax.com/en/dse/5.1/dse-dev/datastax_enterprise/search/cursorsDeepPaging.html#cursorsDeepPaging__srchCursorCQL
     */
    private BoundStatement createStatementToQuerySuggestions(String query, int fetchSize) {
        final StringBuilder solrQuery = new StringBuilder();
        solrQuery.append("{\"q\":\"search_suggestions:");
        solrQuery.append(query);
        solrQuery.append("*\", \"paging\":\"driver\"}");

        BoundStatement stmt = findSuggestedTags.bind().setString("solr_query", solrQuery.toString());
        stmt.setFetchSize(fetchSize);
        LOGGER.debug("getQuerySuggestions: {} with solr_query: {}", stmt.preparedStatement().getQueryString(), solrQuery);
        return stmt;
    }

    /**
     * Search for tags starting with provided query string.
     *
     * @param query
     * 		pattern
     * @param fetchSize
     * 		numbner of results to retrieve
     * @return
     */
    public TreeSet< String > getQuerySuggestions(String query, int fetchSize) {
    	BoundStatement stmt = createStatementToQuerySuggestions(query, fetchSize);
    	return mapTagSet(dseSession.execute(stmt), query);
    }
    
    /**
     * Search for tags starting with provided query string (ASYNC).
     *
     * @param query
     * 		pattern
     * @param fetchSize
     * 		numbner of results to retrieve
     * @return
     */
    public CompletableFuture < TreeSet< String > > getQuerySuggestionsAsync(String query, int fetchSize) {
    	BoundStatement stmt = createStatementToQuerySuggestions(query, fetchSize);
        ResultSetFuture resultSetFuture = dseSession.executeAsync(stmt);
        return asCompletableFuture(resultSetFuture).thenApplyAsync(rs -> mapTagSet(rs, query));
    }
     
    /**
     * Here, we are inserting the request from the search bar, maybe something
     * like "c", "ca", or "cas" as someone starts to type the word "cassandra".
     *
     * For each of these cases we are looking for any words in the search data that
     * start with the values above.
     *
     * @param rs
     * 		current resultset
     * @param requestQuery
     * 		query
     * @return
     * 		set of tags
     */
    @SuppressWarnings("serial")
	private TreeSet < String > mapTagSet(ResultSet rs, String requestQuery) {
        final Pattern checkRegex = Pattern.compile("(?i)\\b" + requestQuery + "[a-z]*\\b");
        TreeSet< String > suggestionSet = new TreeSet<>();
    	for (Row row : rs) {
    		/**
             * Since I simply want matches from both the name and tags fields
             * concatenate them together, apply regex, and add any results into
             * our suggestionSet TreeSet.  The TreeSet will handle any duplicates.
             */
            String name = row.getString(Video.COLUMN_NAME);
            Set<String> tags = row.getSet(Video.COLUMN_TAGS, new TypeToken<String>() {});
            Matcher regexMatcher = checkRegex.matcher(name.concat(tags.toString()));
            while (regexMatcher.find()) {
                suggestionSet.add(regexMatcher.group().toLowerCase());
            }
            suggestionSet.removeAll(excludeConjunctions);
    	}
    	 LOGGER.debug("TagSet resturned are {}", suggestionSet); 
    	return suggestionSet;
    }

    /**
     * In this case we are using DSE Search to query across the name, tags, and
     * description columns with a boost on name and tags.  Note that tags is a
     * collection of tags per each row with no extra steps to include all data
     * in the collection.
     * 
     * This is a more comprehensive search as
     * we are not just looking at values within the tags column, but also looking
     * across the other fields for similar occurrences.  This is especially helpful
     * if there are no tags for a given video as it is more likely to give us results.
     */
    private BoundStatement createStatementToSearchVideos(String query, int fetchSize, Optional<String> pagingState) {
    	
    	// Escaping special characters for query
    	final String replaceFind = "\"";
        final String replaceWith = "\\\"";
        String requestQuery = query.replaceAll(replaceFind, Matcher.quoteReplacement(replaceWith));
        
        // Build SolQuery
        final StringBuilder solrQuery = new StringBuilder();
        solrQuery.append("{\"q\":\"{!edismax qf=\\\"name^2 tags^1 description\\\"}");
        solrQuery.append(requestQuery);
        solrQuery.append("\", \"paging\":\"driver\"}");
        
        // Binding and extra parameters
        BoundStatement stmt = findVideosByTags.bind().setString("solr_query", solrQuery.toString());
        pagingState.ifPresent( x -> stmt.setPagingState(PagingState.fromString(x)));
        stmt.setFetchSize(fetchSize);
        LOGGER.debug("Executed query is {} with solr_query: {}", stmt.preparedStatement().getQueryString(),solrQuery);
        return stmt;
    }
  
}
