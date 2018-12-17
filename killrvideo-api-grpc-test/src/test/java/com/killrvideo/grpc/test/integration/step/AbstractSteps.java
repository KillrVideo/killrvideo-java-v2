package com.killrvideo.grpc.test.integration.step;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.dse.DseSession;
import com.killrvideo.core.dao.EtcdDao;
import com.killrvideo.dse.conf.DseConfiguration;
import com.killrvideo.dse.model.SchemaConstants;
import com.killrvideo.dse.utils.DseUtils;
import com.killrvideo.grpc.client.KillrVideoGrpcClient;
import com.killrvideo.grpc.test.integration.dto.VideoNameById;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:killrvideo-test.properties")
@ContextConfiguration(classes= { EtcdDao.class, DseConfiguration.class})
public abstract class AbstractSteps {
    
    /** Service Name in ETCD. */
    public static final String USER_SERVICE_NAME                = "UserManagementService";
    public static final String VIDEO_CATALOG_SERVICE_NAME       = "VideoCatalogService";
    public static final String COMMENTS_SERVICE_NAME            = "CommentsService";
    public static final String RATINGS_SERVICE_NAME             = "RatingsService";
    public static final String STATISTICS_SERVICE_NAME          = "StatisticsService";
    public static final String SEARCH_SERVICE_NAME              = "SearchService";
    public static final String SUGGESTED_VIDEOS_SERVICE_NAME    = "SuggestedVideoService";
    
    /** Logger for Test.*/
    private static Logger LOGGER = LoggerFactory.getLogger(AbstractSteps.class);
    
    /** Holds static data for tests. */
    protected Map<String, VideoNameById> testDatasetVideos      = new HashMap<>();
    protected Map<UUID, String>          testDatasetVideosById  = new HashMap<>();
    protected Map<String, UUID>          testDatasetUsers       = new HashMap<>(); 
    
    @Autowired
    protected DseSession dseSession;
     
    @Value("${killrvideo.api-grpc.port: 8899}")
    protected int grpcPort;
    
    @Value("${killrvideo.api-grpc.host: localhost}")
    protected String grpcHostName;
    
    /** GrpcClient to access services. */
    protected KillrVideoGrpcClient grpcClient;
    
    /** PrepareStement used in tests. */
    protected PreparedStatement findUserByEmailPs;
    protected PreparedStatement findVideoByIdPs;
    
    @PostConstruct
    protected void initGrpcClient() {

        // Wrapper for GRPC Killrvideo Stubs 
        grpcClient = new KillrVideoGrpcClient(grpcHostName, grpcPort);
        
        // Generate Sample Data for Users
        testDatasetUsers.clear();
        testDatasetUsers.put("user1", UUID.randomUUID());
        testDatasetUsers.put("user2", UUID.randomUUID());
        testDatasetUsers.put("user3", UUID.randomUUID());
        testDatasetUsers.put("user4", UUID.randomUUID());
        
        // Generate Sample Data for videos
        testDatasetVideos.clear();
        testDatasetVideos.put("video1", new VideoNameById(UUID.randomUUID(), "b-wing-ucs.mp4"));
        testDatasetVideos.put("video2", new VideoNameById(UUID.randomUUID(), "y-wing-ucs.mp4"));
        testDatasetVideos.put("video3", new VideoNameById(UUID.randomUUID(), "x-wing-ucs.mp4"));
        testDatasetVideos.put("video4", new VideoNameById(UUID.randomUUID(), "tie-fighter-ucs.mp4"));
        testDatasetVideos.put("video5", new VideoNameById(UUID.randomUUID(), "mil-falcon-ucs.mp4"));
        testDatasetVideosById.clear();
        testDatasetVideosById = testDatasetVideos
                .entrySet().stream()
                .collect(Collectors.toMap(x -> x.getValue().id, Map.Entry::getKey));
        
        // Initializing statements
        this.findUserByEmailPs = dseSession.prepare("SELECT * FROM killrvideo.user_credentials WHERE email = ?");
        this.findVideoByIdPs   = dseSession.prepare("SELECT added_date FROM killrvideo.videos WHERE videoid = ?");
    }
   
    /**
     * Clear tables used by killrvideo application in DSE.
     * - video_by_tag                     is not used anymore (=> Search)
     * - tags_by_letter                   is not used anymore (=> Search)
     * - video_recommendations            is not used anymore (=> Graph )
     * - video_recommendations_by_video   is not used anymore (=> Graph )
     * - video_ratings                    is not used anymore (=> Graph )
     * - video_ratings_by_user            is not used anymore (=> Graph )
     */
    protected void truncateAllTablesFromKillrVideoKeyspace() {
        LOGGER.info("Truncating All tables..");
        DseUtils.truncate(dseSession, SchemaConstants.TABLENAME_USERS);
        DseUtils.truncate(dseSession, SchemaConstants.TABLENAME_USER_VIDEOS);
        DseUtils.truncate(dseSession, SchemaConstants.TABLENAME_USER_CREDENTIALS);
        DseUtils.truncate(dseSession, SchemaConstants.TABLENAME_VIDEOS);
        DseUtils.truncate(dseSession, SchemaConstants.TABLENAME_PLAYBACK_STATS);
        DseUtils.truncate(dseSession, SchemaConstants.TABLENAME_LATEST_VIDEOS);
        DseUtils.truncate(dseSession, SchemaConstants.TABLENAME_COMMENTS_BY_USER);
        DseUtils.truncate(dseSession, SchemaConstants.TABLENAME_COMMENTS_BY_VIDEO);
        LOGGER.info("[OK]");
    }   
}
