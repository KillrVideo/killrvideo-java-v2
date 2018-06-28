package com.killrvideo.graphql.fetch;

import org.springframework.beans.factory.annotation.Autowired;

import com.killrvideo.dse.dao.CommentDseDao;
import com.killrvideo.dse.dao.dto.ResultListPage;
import com.killrvideo.dse.model.Comment;

import graphql.schema.DataFetcher;

/**
 * Code mutualization for fetchers in comments.
 *
 * @author DataStax evangelist team.
 */
public abstract class AbstractCommentFetcher implements DataFetcher<ResultListPage<Comment>> {

    /** Parameters provided in Schema. */
    protected static final String PARAMQL_QUERY      = "query";
    protected static final String PARAMQL_USERID     = "userid";
    protected static final String PARAMQL_VIDEOID    = "videoid";
    protected static final String PARAMQL_COMMENTID  = "commentid";
    protected static final String PARAMQL_PAGESIZE   = "pageSize";
    protected static final String PARAMQL_PAGESTATE  = "pageState";
    
    @Autowired
    private CommentDseDao commentDseDao;

    /**
     * Getter for attribute 'commentDseDao'.
     *
     * @return
     *       current value of 'commentDseDao'
     */
    public CommentDseDao getCommentDseDao() {
        return commentDseDao;
    }

    /**
     * Setter for attribute 'commentDseDao'.
     * @param commentDseDao
     * 		new value for 'commentDseDao '
     */
    public void setCommentDseDao(CommentDseDao commentDseDao) {
        this.commentDseDao = commentDseDao;
    }

}
