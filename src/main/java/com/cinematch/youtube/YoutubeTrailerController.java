package com.cinematch.youtube;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class YoutubeTrailerController {

    private final YoutubeTrailerService youtubeTrailerService;
    private final JdbcTemplate jdbcTemplate;

    public YoutubeTrailerController(YoutubeTrailerService youtubeTrailerService, JdbcTemplate jdbcTemplate) {
        this.youtubeTrailerService = youtubeTrailerService;
        this.jdbcTemplate = jdbcTemplate;
    }

    /** 백그라운드에서 예고편 채우기 시작. 즉시 응답 반환. */
    @PostMapping("/youtube/fill-trailers")
    public YoutubeTrailerService.JobStatus fillTrailers(
            @RequestParam(defaultValue = "300") int limit
    ) {
        return youtubeTrailerService.startFillAsync(limit);
    }

    /** 진행 상황 확인 */
    @GetMapping("/youtube/fill-trailers/status")
    public YoutubeTrailerService.JobStatus status() {
        return youtubeTrailerService.getStatus();
    }

    /** 특정 영화의 YOUTUBE 소스 예고편 삭제 후 재검색 */
    @DeleteMapping("/youtube/reset-trailer")
    public String resetTrailer(@RequestParam long movieId) {
        jdbcTemplate.update("DELETE FROM movie_video WHERE movie_id = ? AND source = 'YOUTUBE'", movieId);
        String videoKey = youtubeTrailerService.searchForMovie(movieId);
        if (videoKey != null) return "OK: " + videoKey;
        return "NOT_FOUND";
    }
}
