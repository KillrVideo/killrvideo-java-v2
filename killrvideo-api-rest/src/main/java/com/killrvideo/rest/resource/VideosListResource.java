package com.killrvideo.rest.resource;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.web.bind.annotation.RequestMethod.GET;

import java.util.Optional;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.killrvideo.dse.dao.dto.LatestVideosPage;
import com.killrvideo.dse.model.Video;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
@RequestMapping("/api/v1/videos")
public class VideosListResource {
    
    //private static final int DEFAULT_PAGE_SIZE = 10;
    
    @RequestMapping(value = "/latest", method = GET, produces = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "Retrieve latest videos (home page)", response = Video.class)
    @ApiResponses(@ApiResponse(code = 200, message = "Retrieve comments for a dedicated video"))
    public LatestVideosPage getLatestVideo(
            @ApiParam(name="pageSize", value="Requested page size, default is 10", required=false ) 
            @RequestParam("pageSize") Optional<Integer> ppageSize,
            @ApiParam(name="pageState", value="Use to retrieve next pages", required=false ) 
            @RequestParam("pageState") Optional<String> ppageState,
            @ApiParam(name="startDate", value="Use to retrieve next pages", required=false ) 
            @RequestParam("startDate") Optional<String> pstartDate,
            @ApiParam(name="startVideo", value="Use to retrieve next pages", required=false ) 
            @RequestParam("startVideo") Optional<String> pstartVideo) {
       
       /*CustomPagingState pagingState = 
               CustomPagingState.parse(pageState).orElse(videoCatalogDao.buildFirstCustomPagingState());
       int pageSize =  ppageSize.isPresent() ? ppageSize.get() : DEFAULT_PAGE_SIZE;
       
       
       VideoCatalogDao.SDF.parse(pstartDate);

       return  videoCatalogDao.getLatestVideoPreviews(pagingState, pageSize, startDate, startVid);*/
        return null;
    }

}
