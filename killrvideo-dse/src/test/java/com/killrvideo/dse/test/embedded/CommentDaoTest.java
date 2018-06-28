package com.killrvideo.dse.test.embedded;

import java.util.UUID;

import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import com.killrvideo.dse.dao.CommentDseDao;
import com.killrvideo.dse.dao.dto.QueryCommentByUser;
import com.killrvideo.dse.dao.dto.QueryCommentByVideo;
import com.killrvideo.dse.dao.dto.ResultListPage;
import com.killrvideo.dse.model.Comment;
import com.killrvideo.dse.test.AbstractTestEmbedded;

@DisplayName("DAO Comments Unit Tests")
public class CommentDaoTest extends AbstractTestEmbedded {

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
    @DisplayName("When inserting Comment into empty tables you got 1 record")
    public void testInsertComment() {
        // Given
        assertTableExist(KILLRVIDEO_KEYSPACE, TABLENAME_COMMENTS_BY_USER);
        assertTableExist(KILLRVIDEO_KEYSPACE, TABLENAME_COMMENTS_BY_VIDEO);
        assertTableIsEmpty(KILLRVIDEO_KEYSPACE, TABLENAME_COMMENTS_BY_USER);
        assertTableIsEmpty(KILLRVIDEO_KEYSPACE, TABLENAME_COMMENTS_BY_VIDEO);
        // When
        Comment comment = new Comment("Hello World !");
        comment.setCommentid(UUIDs.timeBased());
        comment.setUserid(UUID.randomUUID());
        comment.setVideoid(UUID.randomUUID());
        commentDao.insertComment(comment);
        // Then
        assertCountItemInTable(1, KILLRVIDEO_KEYSPACE, TABLENAME_COMMENTS_BY_USER);
        assertCountItemInTable(1, KILLRVIDEO_KEYSPACE, TABLENAME_COMMENTS_BY_VIDEO);
    }
    
    @Test
    @DisplayName("Insert X random comments in empty table (same user) : expect X result for comment_per_user")
    public void testGetUserComments() {
        // Given
        UUID randomUserId = UUID.randomUUID();
        QueryCommentByUser query = new QueryCommentByUser(randomUserId);
        Assert.assertEquals(0, commentDao.findCommentsByUserId(query).getResults().size());
        // When
        int  testData = new Double(10 * Math.random()).intValue();
        for(int idx =0;idx<testData;idx++) {
            Comment comment = new Comment("Foo bar.." + idx);
            comment.setCommentid(UUIDs.timeBased());
            comment.setUserid(randomUserId);
            comment.setVideoid(UUID.randomUUID());
            commentDao.insertComment(comment);
        }
        // Then
        Assert.assertEquals(testData, commentDao.findCommentsByUserId(query).getResults().size());
    }
    
    @Test
    @DisplayName("Insert X random comments in empty table (same video) : expect X result for comment_per_video")
    public void testGetVideoComments() {
        // Given
        UUID randomVideoId = UUID.randomUUID();
        QueryCommentByVideo query = new QueryCommentByVideo(randomVideoId);
        Assert.assertEquals(0, commentDao.findCommentsByVideoId(query).getResults().size());
        // When
        int  testData = new Double(10 * Math.random()).intValue();
        for(int idx =0;idx<testData;idx++) {
            Comment comment = new Comment("Foo bar.." + idx);
            comment.setCommentid(UUIDs.timeBased());
            comment.setUserid(UUID.randomUUID());
            comment.setVideoid(randomVideoId);
            commentDao.insertComment(comment);
        }
        // Then
        Assert.assertEquals(testData, commentDao.findCommentsByVideoId(query).getResults().size());
    }
    
    @Test
    @DisplayName("Update existing comment (new text) : expect both tables to be updated")
    public void testUpdateComment() {
        // Given
        assertTableExist(KILLRVIDEO_KEYSPACE, TABLENAME_COMMENTS_BY_USER);
        assertTableExist(KILLRVIDEO_KEYSPACE, TABLENAME_COMMENTS_BY_VIDEO);
        assertTableIsEmpty(KILLRVIDEO_KEYSPACE, TABLENAME_COMMENTS_BY_USER);
        assertTableIsEmpty(KILLRVIDEO_KEYSPACE, TABLENAME_COMMENTS_BY_VIDEO);
        Comment comment = new Comment("Hello World !");
        comment.setCommentid(UUIDs.timeBased());
        comment.setUserid(UUID.randomUUID());
        comment.setVideoid(UUID.randomUUID());
        commentDao.insertComment(comment);
        // When Updating comment
        String newComment = "NEW " + System.currentTimeMillis();
        comment.setComment(newComment);
        commentDao.updateComment(comment);
        // Then Text is updated in both tables
        ResultListPage<Comment > res1 = commentDao.findCommentsByUserId(new QueryCommentByUser(comment.getUserid()));
        ResultListPage<Comment > res2 = commentDao.findCommentsByVideoId(new QueryCommentByVideo(comment.getVideoid()));
        Assert.assertNotNull(res1);
        Assert.assertNotNull(res1.getResults());
        Assert.assertEquals(newComment, res1.getResults().get(0).getComment());
        Assert.assertNotNull(res2);
        Assert.assertNotNull(res2.getResults());
        Assert.assertEquals(newComment, res2.getResults().get(0).getComment());
    }
    
    @Test
    @DisplayName("Delete a comment in 2 tables")
    public void testdeleteComment() {
        // Given
        Comment comment = new Comment("Hello World !");
        comment.setCommentid(UUIDs.timeBased());
        comment.setUserid(UUID.randomUUID());
        comment.setVideoid(UUID.randomUUID());
        commentDao.insertComment(comment);
        assertCountItemInTable(1, KILLRVIDEO_KEYSPACE, TABLENAME_COMMENTS_BY_USER);
        assertCountItemInTable(1, KILLRVIDEO_KEYSPACE, TABLENAME_COMMENTS_BY_VIDEO);
        // When Deleting
        commentDao.deleteComment(comment);
        // Then table are empty again
        assertCountItemInTable(0, KILLRVIDEO_KEYSPACE, TABLENAME_COMMENTS_BY_USER);
        assertCountItemInTable(0, KILLRVIDEO_KEYSPACE, TABLENAME_COMMENTS_BY_VIDEO);
    }  
    
}

