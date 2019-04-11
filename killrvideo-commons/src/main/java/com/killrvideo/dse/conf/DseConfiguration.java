package com.killrvideo.dse.conf;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.TrustManagerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.RemoteEndpointAwareNettySSLOptions;
import com.datastax.driver.core.policies.AddressTranslator;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.dse.DseCluster.Builder;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.auth.DsePlainTextAuthProvider;
import com.datastax.driver.dse.graph.GraphOptions;
import com.datastax.driver.dse.graph.GraphProtocol;
import com.datastax.driver.mapping.DefaultPropertyMapper;
import com.datastax.driver.mapping.MappingConfiguration;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.PropertyMapper;
import com.datastax.driver.mapping.PropertyTransienceStrategy;
import com.datastax.dse.graph.api.DseGraph;
import com.evanlennick.retry4j.CallExecutor;
import com.evanlennick.retry4j.config.RetryConfig;
import com.evanlennick.retry4j.config.RetryConfigBuilder;
import com.killrvideo.discovery.ServiceDiscoveryDao;
import com.killrvideo.dse.graph.KillrVideoTraversalSource;
import com.killrvideo.dse.utils.BlobToStringCodec;
import com.killrvideo.model.CommonConstants;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

/**
 * Connectivity to DSE (cassandra, graph, search, analytics).
 *
 * @author DataStax Developer Advocates team.
 */
@Configuration
public class DseConfiguration {

	/** Internal logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(DseConfiguration.class);
     
    @Value("${killrvideo.discovery.service.cassandra: cassandra}")
    private String cassandraServiceName;
    
    @Value("${killrvideo.cassandra.clustername: 'killrvideo'}")
    public String dseClustOerName;
    
    @Value("${killrvideo.graph.timeout: 30000}")
    public Integer graphTimeout;
    
    @Value("${killrvideo.graph.recommendation.name: 'killrvideo_video_recommendations'}")
    public String graphRecommendationName;
    
    @Value("#{environment.KILLRVIDEO_DSE_USERNAME}")
    public Optional < String > dseUsername;
   
    @Value("#{environment.KILLRVIDEO_DSE_PASSWORD}")
    public Optional < String > dsePassword;
    
    @Value("${killrvideo.cassandra.maxNumberOfTries: 10}")
    private int maxNumberOfTries  = 10;
    
    @Value("${killrvideo.cassandra.delayBetweenTries: 2}")
    private int delayBetweenTries = 2;
    
    @Value("${killrvideo.ssl.CACertFileLocation: cassandra.cert}")
    private String sslCACertFileLocation;
    
    @Value("${killrvideo.ssl.enable: false}")
    public boolean dseEnableSSL = false;
    
    @Value("${killrvideo.etcd.enabled : true}")
    private boolean etcdLookup = false;
    
    @Autowired
    private ServiceDiscoveryDao discoveryDao;
    
    @Bean
    public DseSession initializeDSE() {
         long top = System.currentTimeMillis();
         LOGGER.info("Initializing connection to DSE Cluster...");
         Builder clusterConfig = new Builder();
         populateContactPoints(clusterConfig);
         populateAuthentication(clusterConfig);
         populateGraphOptions(clusterConfig);
         populateSSL(clusterConfig);
         
         // More options available with flags through QueryOptions 
         QueryOptions options = new QueryOptions();
         options.setConsistencyLevel(ConsistencyLevel.QUORUM);
         options.setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);
         clusterConfig.withQueryOptions(options);
         clusterConfig.withReconnectionPolicy(new ConstantReconnectionPolicy(2000));
         
         // Required
         clusterConfig.withoutJMXReporting();
         clusterConfig.withoutMetrics();
         clusterConfig.getConfiguration().getSocketOptions().setReadTimeoutMillis(1000);
         clusterConfig.getConfiguration().getCodecRegistry().register(new BlobToStringCodec());
         
         final AtomicInteger atomicCount = new AtomicInteger(1);
         Callable<DseSession> connectionToDse = () -> {
             return clusterConfig.build().connect(CommonConstants.KILLRVIDEO_KEYSPACE);
         };
         
         // Connecting to DSE with a retry mechanism : 
         /* In docker deployments we may have to wait until all components are up and running. */
         RetryConfig config = new RetryConfigBuilder()
                 .retryOnAnyException()
                 .withMaxNumberOfTries(maxNumberOfTries)
                 .withDelayBetweenTries(delayBetweenTries, ChronoUnit.SECONDS)
                 .withFixedBackoff()
                 .build();
         
         return new CallExecutor<DseSession>(config)
                 .afterFailedTry(s -> { 
                     LOGGER.info("Attempt #{}/{} failed.. trying in {} seconds, waiting Dse to Start", atomicCount.getAndIncrement(),
                             maxNumberOfTries,  delayBetweenTries); })
                 .onFailure(s -> {
                     LOGGER.error("Cannot connection to DSE after {} attempts, exiting", maxNumberOfTries);
                     System.err.println("Can not conenction to DSE after " + maxNumberOfTries + " attempts, exiting");
                     System.exit(500);
                  })
                 .onSuccess(s -> {   
                     long timeElapsed = System.currentTimeMillis() - top;
                     LOGGER.info("[OK] Connection etablished to DSE Cluster in {} millis.", timeElapsed);})
                 .execute(connectionToDse).getResult();
    }
    
    /**
     * Use to create mapper and perform ORM on top of Cassandra tables.
     * 
     * @param session
     *      current dse session.
     * @return
     *      mapper
     */
    @Bean
    public MappingManager initializeMappingManager(DseSession session) {
        // Do not map all fields, only the annotated ones with @Column or @Fields
        PropertyMapper propertyMapper = new DefaultPropertyMapper()
                .setPropertyTransienceStrategy(PropertyTransienceStrategy.OPT_IN);
        // Build configuration from mapping
        MappingConfiguration configuration = MappingConfiguration.builder()
                .withPropertyMapper(propertyMapper)
                .build();
        // Sample Manager with advance configuration
        return new MappingManager(session, configuration);
    }
    
    /**
     * Graph Traversal for suggested videos.
     *
     * @param session
     *      current dse session.
     * @return
     *      traversal
     */
    @Bean
    public KillrVideoTraversalSource initializeGraphTraversalSource(DseSession session) {
        return DseGraph.traversal(session, KillrVideoTraversalSource.class);
    }
    
    /**
     * Retrieve cluster nodes adresses (eg:node1:9042) from ETCD and initialize the `contact points`,
     * endpoints of Cassandra cluster nodes.
     * 
     * @note 
     * [Initializing Contact Points with Java Driver]
     * 
     * (1) The default port is 9042. If you keep using the default port you
     * do not need to use or `withPort()` or `addContactPointsWithPorts()`, only `addContactPoint()`.
     * 
     * (2) Best practice is to use the SAME PORT for each node and to  setup the port through `withPort()`.
     * 
     * (3) Never, ever use `addContactPointsWithPorts` in clusters : it will ony set port FOR THE FIRST NODE. 
     * DON'T USE, EVEN IF ALL NODE USE SAME PORT. It purpose is only for tests and standalone servers.
     * 
     * (4) What if I have a cluster and nodes do not use the same port (eg: node1:9043, node2:9044, node3:9045) ?
     * You need to use {@link AddressTranslator} as defined below and reference with `withAddressTranslator(translator);`
     * 
     * <code>
     * public class MyClusterAddressTranslator implements AddressTranslator {
     *  @Override
     *  public void init(Cluster cluster) {}
     *  @Override
     *  public void close() {}
     *  
     *  @Override
     *  public InetSocketAddress translate(InetSocketAddress incoming) {
     *     // Given the configuration
     *     Map<String, Integer> clusterNodes = new HashMap<String, Integer>() { {
     *       put("node1", 9043);put("node2", 9044);put("node3", 9045);
     *      }};
     *      String targetHostName = incoming.getHostName();
     *      if (clusterNodes.containsKey(targetHostName)) {
     *          return new InetSocketAddress(targetHostName, clusterNodes.get(targetHostName));
     *      }
     *      throw new IllegalArgumentException("Cannot translate URL " + incoming + " hostName not found");
     *   }
     * }
     * </code>
     * 
     * @param clusterConfig
     *      current configuration
     */
     private void populateContactPoints(Builder clusterConfig)  {
        discoveryDao.lookup(cassandraServiceName).stream()
                    .map(this::asSocketInetAdress)
                    .filter(node -> node.isPresent())
                    .map(node -> node.get())
                    .map(adress -> adress.getHostName())
                    .forEach(clusterConfig::addContactPoint);
    }
    
    /**
     * Check to see if we have username and password from the environment
     * This is here because we have a dual use scenario.  One for developers and others
     * who download KillrVideo and run within a local Docker container and the other
     * who might need (like us for example) to connect KillrVideo up to an external
     * cluster that requires authentication.
     */
    private void populateAuthentication(Builder clusterConfig) {
        if (dseUsername.isPresent() && dsePassword.isPresent() 
                                    && dseUsername.get().length() > 0) {
            AuthProvider cassandraAuthProvider = new DsePlainTextAuthProvider(dseUsername.get(), dsePassword.get());
            clusterConfig.withAuthProvider(cassandraAuthProvider);
            String obfuscatedPassword = new String(new char[dsePassword.get().length()]).replace("\0", "*");
            LOGGER.info(" + Using supplied DSE username: '{}' and password: '{}' from environment variables", 
                        dseUsername.get(), obfuscatedPassword);
        } else {
            LOGGER.info(" + Connection is not authenticated (no username/password)");
        }
    }
    
    private void populateGraphOptions(Builder clusterConfig) {
        GraphOptions go = new GraphOptions();
        go.setGraphName(graphRecommendationName);
        go.setReadTimeoutMillis(graphTimeout);
        go.setGraphSubProtocol(GraphProtocol.GRAPHSON_2_0);
        clusterConfig.withGraphOptions(go);
    }
    
    /**
     * Convert information in ETCD as real adress {@link InetSocketAddress} if possible.
     *
     * @param contactPoint
     *      network node adress information like hostname:port
     * @return
     *      java formatted inet adress
     */
    private Optional<InetSocketAddress> asSocketInetAdress(String contactPoint) {
        Optional<InetSocketAddress> target = Optional.empty();
        try {
            if (contactPoint != null && contactPoint.length() > 0) {
                String[] chunks = contactPoint.split(":");
                if (chunks.length == 2) {
                    LOGGER.info(" + Adding node '{}' to the Cassandra cluster definition", contactPoint);
                    return Optional.of(new InetSocketAddress(InetAddress.getByName(chunks[0]), Integer.parseInt(chunks[1])));
                }
            }
        } catch (NumberFormatException e) {
            LOGGER.warn(" + Cannot read contactPoint - "
                    + "Invalid Port Numer, entry '" + contactPoint + "' will be ignored", e);
        } catch (UnknownHostException e) {
            LOGGER.warn(" + Cannot read contactPoint - "
                    + "Invalid Hostname, entry '" + contactPoint + "' will be ignored", e);
        }
        return target;
    }
    
    /**
     * If SSL is enabled use the supplied CA cert file to create
     * an SSL context and use to configure our cluster.
     *
     * @param clusterConfig
     *      current configuration
     */
    private void populateSSL(Builder clusterConfig) {
        if (dseEnableSSL) {
            LOGGER.info(" + SSL is enabled, using supplied SSL certificate: '{}'", sslCACertFileLocation);
             try {
                 // X509 Certificate
                FileInputStream fis = new FileInputStream(sslCACertFileLocation);
                X509Certificate caCert = (X509Certificate) CertificateFactory.getInstance("X.509")
                        .generateCertificate(new BufferedInputStream(fis));
                
                // KeyStore
                KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
                ks.load(null, null);
                ks.setCertificateEntry(Integer.toString(1), caCert);

                // TrustStore
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(ks);
                SslContext sslContext = SslContextBuilder.forClient().trustManager(tmf).build();
                clusterConfig.withSSL(new RemoteEndpointAwareNettySSLOptions(sslContext));
                
             } catch (FileNotFoundException fne) {
                 String errMsg = "SSL cert file not found. You must provide a valid certification file when using SSL encryption option.";
                 LOGGER.error(errMsg, fne);
                 throw new IllegalArgumentException(errMsg, fne);
                 
             } catch (CertificateException ce) {
                String errCert = "Your CA certificate looks invalid. You must provide a valid certification file when using SSL encryption option.";
                LOGGER.error(errCert, ce);
                
                throw new IllegalArgumentException(errCert, ce);
             } catch (Exception e) {
                String errSsl = "Exception in SSL configuration: ";
                LOGGER.error(errSsl, e);
                throw new IllegalArgumentException(errSsl, e);
            }
         } else {
            LOGGER.info(" + SSL encryption is not enabled)");
        }
     }
   
}
