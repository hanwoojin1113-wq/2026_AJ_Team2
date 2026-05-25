package com.cinematch.youtube;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class YoutubeTrailerController {

    private final YoutubeTrailerService youtubeTrailerService;

    public YoutubeTrailerController(YoutubeTrailerService youtubeTrailerService) {
        this.youtubeTrailerService = youtubeTrailerService;
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
}
