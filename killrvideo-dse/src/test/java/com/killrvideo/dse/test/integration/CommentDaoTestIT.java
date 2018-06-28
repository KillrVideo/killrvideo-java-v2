package com.killrvideo.dse.test.integration;

import java.util.UUID;

import org.junit.Ignore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.killrvideo.dse.dao.CommentDseDao;
import com.killrvideo.dse.dao.dto.QueryCommentByUser;
import com.killrvideo.dse.dao.dto.QueryCommentByVideo;
import com.killrvideo.dse.dao.dto.ResultListPage;
import com.killrvideo.dse.model.Comment;
import com.killrvideo.dse.test.AbstractTest;

/** 
 * Utility to test DAO on remote SERVER (just provide the URL).
 */
@Ignore
public class CommentDaoTestIT extends AbstractTest {

	// Where to look : ¯\_(ツ)_/¯
	protected String getContactPointAdress()         { return "localhost"; }
    protected int    getContactPointPort()           { return 9042;        }
    protected ConsistencyLevel getConsistencyLevel() { return ConsistencyLevel.QUORUM; }
   
    /** Target DAO     ¯\_(ツ)_/¯      */
    private CommentDseDao commentDao;
    
    @BeforeEach
    public void initDAO() {
        if (commentDao == null) {
            connectKeyspace(KILLRVIDEO_KEYSPACE);
            commentDao = new CommentDseDao(dseSession);
        }
    }
    
    @Test
    public void getCommentsByUser() throws Exception {
    	QueryCommentByUser query = new QueryCommentByUser(UUID.fromString("805d83ad-2fbc-43e9-a61b-9ebb3dae0950"));
    	ResultListPage<Comment> myComments = commentDao.findCommentsByUserId(query);
    	System.out.println(myComments.toString());
    }
  
    @Test
    public void getCommentsByVideo() throws Exception {
    	QueryCommentByVideo query = new QueryCommentByVideo(UUID.fromString("3ffd0bbb-dd80-4816-af39-cd6e6c2e7507"));
    	ResultListPage<Comment> myComments = commentDao.findCommentsByVideoId(query);
    	System.out.println(myComments.toString());
    }
    
    @Test
    public void updateComment() throws Exception {
    	Comment target =  new Comment();
    	target.setComment("COMMENT FROM TEST");
    	target.setCommentid(UUID.fromString("f4e65c40-3be4-11e8-8ded-3195fe4e490f"));
    	target.setVideoid(UUID.fromString("3ffd0bbb-dd80-4816-af39-cd6e6c2e7507"));
    	target.setUserid(UUID.fromString("805d83ad-2fbc-43e9-a61b-9ebb3dae0950"));
    	commentDao.updateComment(target);
    }
    
    @Test
    public void deleteComment() throws Exception {
    	Comment target =  new Comment();
    	target.setCommentid(UUID.fromString("52f095b0-3bdd-11e8-8ded-3195fe4e490f"));
    	target.setVideoid(UUID.fromString("fd7f1690-2a48-4751-9afb-9073af0c5c30"));
    	target.setUserid(UUID.fromString("805d83ad-2fbc-43e9-a61b-9ebb3dae0950"));
    	commentDao.deleteComment(target);
    }
    
}
