package com.cinematch.admin;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/admin/seed")
@RequiredArgsConstructor
public class CommunitySeedController {

    private final CommunitySeedService communitySeedService;

    @PostMapping("/community")
    public Map<String, Object> seedCommunity() {
        String log = communitySeedService.seed();
        return Map.of("status", "ok", "log", log);
    }

    @PostMapping("/phase1")
    public Map<String, Object> seedPhase1() {
        String log = communitySeedService.seedPhase1();
        return Map.of("status", "ok", "log", log);
    }

    @PostMapping("/phase2")
    public Map<String, Object> seedPhase2() {
        String log = communitySeedService.seedPhase2();
        return Map.of("status", "ok", "log", log);
    }
}
