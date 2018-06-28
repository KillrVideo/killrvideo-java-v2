package com.killrvideo.graphql.fetch;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.killrvideo.dse.dao.CommentDseDao;
import com.killrvideo.dse.dao.dto.QueryCommentByUser;
import com.killrvideo.dse.dao.dto.ResultListPage;
import com.killrvideo.dse.model.Comment;

import graphql.schema.DataFetchingEnvironment;

/**
 * A data fetcher is responsible for returning a data value back for a given graphql field. 
 *
 * @author DataStax evangelist team.
 */
@Component
public class CommentsByUserFetch extends AbstractCommentFetcher {

    /** Loger for that class. */
    private static Logger LOGGER = LoggerFactory.getLogger(CommentsByUserFetch.class);
    
    @Autowired
    private static CommentDseDao commentDseDao;
    
    /** {@inheritDoc} */
    public ResultListPage<Comment> get(DataFetchingEnvironment environment) {
        LOGGER.debug("Entering Fetcher for Comments");
        Map<String, Object> mapInputs = environment.getArgument(PARAMQL_QUERY);
        UUID userId = UUID.fromString((String) mapInputs.get(PARAMQL_USERID));
        QueryCommentByUser query = new QueryCommentByUser(userId);
        // Comment Id, only if provided in the request
        String commentId = (String) mapInputs.get(PARAMQL_COMMENTID);
        if (!StringUtils.isBlank(commentId)) {
            query.setCommentId(Optional.of(UUID.fromString(commentId)));
        }
        // Pagination if relevant
        String pageSize  = (String) mapInputs.get(PARAMQL_PAGESIZE);
        String pageState = (String) mapInputs.get(PARAMQL_PAGESTATE);
        if (!StringUtils.isBlank(pageState) && !StringUtils.isBlank(pageSize)) {
            query.setPageSize(Integer.parseInt(pageSize));
            query.setPageState(Optional.of(pageState));
        }
        return commentDseDao.findCommentsByUserId(query);
    }
}