package com.killrvideo.graphql.dao;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.killrvideo.dse.dao.dto.QueryCommentByVideo;
import com.killrvideo.dse.dao.dto.ResultListPage;
import com.killrvideo.graphql.domain.CommentGQL;

@Component
public class CommentRepositoryMock {

  private Map<String, CommentGQL> comments;

  public CommentRepositoryMock() {
      comments = new HashMap<>();
  }

  public CommentGQL save(CommentGQL newComment) {
    String id = UUID.randomUUID().toString();
    newComment.setCommentid(id);
    newComment.setDateOfComment(new Date());
    comments.put(id, newComment);
    return newComment;
  }

  public ResultListPage< CommentGQL > getVideoComment(QueryCommentByVideo qcbv) {
      ResultListPage< CommentGQL > rp = new ResultListPage<>();
      rp.getResults().addAll(comments.values());
      return rp;
  }
  
  public CommentGQL find(String id) {
    return comments.get(id);
  }
  
  
  
}
