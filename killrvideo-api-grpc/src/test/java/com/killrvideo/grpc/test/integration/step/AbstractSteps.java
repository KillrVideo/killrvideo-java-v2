package com.killrvideo.grpc.test.integration.step;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.dse.DseSession;
import com.killrvideo.core.dao.EtcdDao;
import com.killrvideo.dse.conf.DseConfiguration;
import com.killrvideo.dse.utils.DseUtils;
import com.killrvideo.grpc.client.KillrVideoGrpcClient;
import com.killrvideo.grpc.test.integration.dto.VideoNameById;
import com.killrvideo.messaging.conf.MessagingConfiguration;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:killrvideo.properties")
@ContextConfiguration(classes= { EtcdDao.class, DseConfiguration.class, MessagingConfiguration.class})
public class AbstractSteps {
    
    // Service Name in ETCD
    public static final String USER_SERVICE_NAME                = "UserManagementService";
    public static final String VIDEO_CATALOG_SERVICE_NAME       = "VideoCatalogService";
    public static final String COMMENTS_SERVICE_NAME            = "CommentsService";
    public static final String RATINGS_SERVICE_NAME             = "RatingsService";
    public static final String STATISTICS_SERVICE_NAME          = "StatisticsService";
    public static final String SEARCH_SERVICE_NAME              = "SearchService";
    public static final String SUGGESTED_VIDEOS_SERVICE_NAME    = "SuggestedVideoService";
    
    @Autowired
    protected EtcdDao etcdDao;
    
    @Autowired
    protected DseSession dseSession;
    
    @Autowired
    protected ExecutorService threadPool;
    
    @Value("${killrvideo.application.name}")
    private String applicationName;
    
    @Value("${grpc.port}")
    protected int grpcPort;
    
    protected KillrVideoGrpcClient grpcClient;
    
    public PreparedStatement findUserByEmailPs;
    
    public PreparedStatement findVideoByIdPs;
    
    @PostConstruct
    protected void initGrpcClient() {
        System.out.println(grpcPort);
        grpcClient = new KillrVideoGrpcClient("localhost", grpcPort);
        this.findUserByEmailPs = dseSession.prepare("SELECT * FROM killrvideo.user_credentials WHERE email = ?");
        this.findVideoByIdPs   = dseSession.prepare("SELECT added_date FROM killrvideo.videos WHERE videoid = ?");
    }
    
    protected void cleanUpUserAndVideoTables() {
        DseUtils.truncate(dseSession, "user_credentials");
        DseUtils.truncate(dseSession, "users");
        DseUtils.truncate(dseSession, "videos");
        DseUtils.truncate(dseSession, "user_videos");
        DseUtils.truncate(dseSession, "latest_videos");
        DseUtils.truncate(dseSession, "videos_by_tag");
        DseUtils.truncate(dseSession, "tags_by_letter");
        
    }
    
    @SuppressWarnings("serial")
    public final Map<String, VideoNameById> VIDEOS = new HashMap<String, VideoNameById>() {
        {
            put("video1", new VideoNameById(UUID.randomUUID(), "b-wing-ucs.mp4"));
            put("video2", new VideoNameById(UUID.randomUUID(), "y-wing-ucs.mp4"));
            put("video3", new VideoNameById(UUID.randomUUID(), "x-wing-ucs.mp4"));
            put("video4", new VideoNameById(UUID.randomUUID(), "tie-fighter-ucs.mp4"));
            put("video5", new VideoNameById(UUID.randomUUID(), "mil-falcon-ucs.mp4"));
        }
    };
    
    @SuppressWarnings("serial")
    public static Map<String, UUID> USERS = new HashMap<String, UUID>() {
        {
            put("user1", UUID.randomUUID());
            put("user2", UUID.randomUUID());
            put("user3", UUID.randomUUID());
            put("user4", UUID.randomUUID());
        }
    };

    public final Map<UUID, String> VIDEOS_BY_ID = VIDEOS.entrySet().stream()
            .collect(Collectors.toMap(x -> x.getValue().id, Map.Entry::getKey));
    
    public Row getOne(BoundStatement bs) {
        return dseSession.execute(bs).one();
    }
        
}
