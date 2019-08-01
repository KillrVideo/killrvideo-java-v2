package com.killrvideo.service.video.dto;

import java.util.ArrayList;
import java.util.List;

/**
 * Latest page.
 *
 * @author DataStax Developer Advocates team.
 */
public class LatestVideosPage {
    
    /** List of Previews. */
    private List< LatestVideo > listOfPreview = new ArrayList<>();
    
    /** Flag if paging state. */
    private String cassandraPagingState = "";
    
    /** Use to return for query. */
    private String nextPageState = "";

    /**
     * Access latest videos.
     *
     * @param video
     *      adding a video
     */
    public void addLatestVideos(LatestVideo video) {
        listOfPreview.add(video);
    }
    
    /**
     * Getter for attribute 'listOfPreview'.
     *
     * @return
     *       current value of 'listOfPreview'
     */
    public List<LatestVideo> getListOfPreview() {
        return listOfPreview;
    }
    
    public int getResultSize() {
        return getListOfPreview().size();
    }

    /**
     * Setter for attribute 'listOfPreview'.
     * @param listOfPreview
     * 		new value for 'listOfPreview '
     */
    public void setListOfPreview(List<LatestVideo> listOfPreview) {
        this.listOfPreview = listOfPreview;
    }

    /**
     * Getter for attribute 'cassandraPagingState'.
     *
     * @return
     *       current value of 'cassandraPagingState'
     */
    public String getCassandraPagingState() {
        return cassandraPagingState;
    }

    /**
     * Setter for attribute 'cassandraPagingState'.
     * @param cassandraPagingState
     * 		new value for 'cassandraPagingState '
     */
    public void setCassandraPagingState(String cassandraPagingState) {
        this.cassandraPagingState = cassandraPagingState;
    }

    /**
     * Getter for attribute 'nextPageState'.
     *
     * @return
     *       current value of 'nextPageState'
     */
    public String getNextPageState() {
        return nextPageState;
    }

    /**
     * Setter for attribute 'nextPageState'.
     * @param nextPageState
     * 		new value for 'nextPageState '
     */
    public void setNextPageState(String nextPageState) {
        this.nextPageState = nextPageState;
    }

}
