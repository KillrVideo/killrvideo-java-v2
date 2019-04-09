package com.killrvideo.service.user.dao;

import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.Result;
import com.killrvideo.dse.dao.DseDaoSupport;
import com.killrvideo.service.user.dto.User;
import com.killrvideo.service.user.dto.UserCredentials;
import com.killrvideo.utils.FutureUtils;

/**
 * Handling user.
 *
 * @author DataStax Developer Advocates Team
 */
@Repository
public class UserDseDao extends DseDaoSupport {

    /** Logger for DAO. */
    private static final Logger LOGGER = LoggerFactory.getLogger(UserDseDao.class);
    
    /** Data model constants. */
    public static final String TABLENAME_USERS                         = "users";
    public static final String TABLENAME_USER_CREDENTIALS              = "user_credentials";
    
    /** Mapper to ease queries. */
    protected Mapper < User >             mapperUsers;
    protected Mapper < UserCredentials >  mapperUserCredentials;
    
    /** Precompile statements to speed up queries. */
    private PreparedStatement insertCredentialsStatement;
    private PreparedStatement insertUserStatement;
    private PreparedStatement findUsersByIdsStatement;
   
    /**
     * Default constructor.
     */
    public UserDseDao() {
        super();
    }
    
    /**
     * Allow explicit intialization for test purpose.
     */
    public UserDseDao(DseSession dseSession) {
        super(dseSession);
    }
    
    /** {@inheritDoc} */
    @PostConstruct
    protected void initialize () {
        
        // Mapping Bean to tables
        mapperUsers            = mappingManager.mapper(User.class);
        mapperUserCredentials  = mappingManager.mapper(UserCredentials.class);
        
        // Create User Credentials
        RegularStatement stmt = QueryBuilder.insertInto(
                mapperUserCredentials.getTableMetadata().getKeyspace().getName(), 
                mapperUserCredentials.getTableMetadata().getName())
            .value(UserCredentials.COLUMN_EMAIL, QueryBuilder.bindMarker())
            .value(UserCredentials.COLUMN_PASSWORD, QueryBuilder.bindMarker())
            .value(UserCredentials.COLUMN_USERID, QueryBuilder.bindMarker())
            .ifNotExists();
        insertCredentialsStatement = dseSession.prepare(stmt);
        insertCredentialsStatement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        
        // Create User
        RegularStatement stmt2 =  QueryBuilder.insertInto(
                mapperUsers.getTableMetadata().getKeyspace().getName(), 
                mapperUsers.getTableMetadata().getName())
                .value(User.COLUMN_USERID,    QueryBuilder.bindMarker())
                .value(User.COLUMN_FIRSTNAME, QueryBuilder.bindMarker())
                .value(User.COLUMN_LASTNAME,  QueryBuilder.bindMarker())
                .value(User.COLUMN_EMAIL, QueryBuilder.bindMarker())
                .value(User.COLUMN_CREATE, QueryBuilder.bindMarker())
                .ifNotExists(); // use lightweight transaction
        insertUserStatement = dseSession.prepare(stmt2);
        insertUserStatement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        
        // Find User profiles
        RegularStatement stmt3 =  QueryBuilder.select().all()
                .from(mapperUsers.getTableMetadata().getKeyspace().getName(), 
                      mapperUsers.getTableMetadata().getName())
                .where(QueryBuilder.in(User.COLUMN_USERID, QueryBuilder.bindMarker()));
        findUsersByIdsStatement = dseSession.prepare(stmt3);
        findUsersByIdsStatement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    }
    
    /**
     * Create user Asynchronously composing things. (with Mappers)
     * 
     * @param user
     *      user Management
     * @param hashedPassword
     *      hashed Password
     * @return
     */
    public CompletableFuture<Void> createUserAsync(User user, String hashedPassword) {
        
        String errMsg = String.format("Exception creating user because it already exists with email %s", user.getEmail());
        
        final BoundStatement insertCredentialsQuery = insertCredentialsStatement.bind()
                .setString(UserCredentials.COLUMN_EMAIL, user.getEmail())
                .setString(UserCredentials.COLUMN_PASSWORD, hashedPassword)
                .setUUID(UserCredentials.COLUMN_USERID, user.getUserid());
        
        // Create Record in user_Credentials if not already exist
        CompletableFuture<ResultSet> future1 = FutureUtils.asCompletableFuture(dseSession.executeAsync(insertCredentialsQuery)); 
        
        // Execute user creation only if credentials did no exist
        CompletableFuture<ResultSet> future2 = future1.thenCompose(rs -> {
            if (rs != null && rs.wasApplied()) {
                final BoundStatement insertUserQuery = insertUserStatement.bind()
                                .setUUID(User.COLUMN_USERID, user.getUserid())
                                .setString(User.COLUMN_FIRSTNAME, user.getFirstname()).setString(User.COLUMN_LASTNAME, user.getLastname())
                                .setString(User.COLUMN_EMAIL, user.getEmail()).setTimestamp(User.COLUMN_CREATE, new Date());
                    return FutureUtils.asCompletableFuture(dseSession.executeAsync(insertUserQuery));
            }
            return future1;
        });

        /**
         * ThenAccept in the same thread pool (not using thenAcceptAsync())
         */
        return future2.thenAccept(rs -> {
            if (rs != null && !rs.wasApplied()) {
                LOGGER.error(errMsg);
                throw new CompletionException(errMsg, new IllegalArgumentException(errMsg));
            }
        });
    }

    /**
     * Get user Credentials 
     * @param email
     * @return
     */
    public CompletableFuture< UserCredentials > getUserCredentialAsync(String email) {
        return FutureUtils.asCompletableFuture(mapperUserCredentials.getAsync(email));
    }
    
    /**
     * Retrieve user profiles.
     *
     * @param userids
     * @return
     */
    public CompletableFuture < List < User > > getUserProfilesAsync(List < UUID > userids) {
        Statement stmt = findUsersByIdsStatement.bind().setList(0, userids, UUID.class);
        return FutureUtils.asCompletableFuture(mapperUsers.mapAsync(dseSession.executeAsync(stmt))).thenApply(Result::all);
    }
       
}
