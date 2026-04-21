package com.cinematch.admin;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/admin/pipeline")
public class AdminBatchController {

    private final MovieDataBatchService movieDataBatchService;

    public AdminBatchController(MovieDataBatchService movieDataBatchService) {
        this.movieDataBatchService = movieDataBatchService;
    }

    @GetMapping("/runs")
    public java.util.List<MovieDataBatchService.JobRunHistory> runs(
            @RequestParam(defaultValue = "20") int limit
    ) {
        return movieDataBatchService.listRuns(limit);
    }

    @PostMapping("/kobis/import-yearly")
    public MovieDataBatchService.JobRunResponse importKobisYearly(
            @RequestParam(required = false) Integer startYear,
            @RequestParam(required = false) Integer endYear,
            @RequestParam(defaultValue = "20") int targetPerYear
    ) {
        return movieDataBatchService.runKobisYearlyImport(startYear, endYear, targetPerYear);
    }

    @PostMapping("/kobis/normalize")
    public MovieDataBatchService.JobRunResponse normalizeKobis() {
        return movieDataBatchService.runKobisNormalize();
    }

    @PostMapping("/tmdb/import-popular")
    public MovieDataBatchService.JobRunResponse importTmdbPopular() {
        return movieDataBatchService.runTmdbPopularImport();
    }

    @PostMapping("/tmdb/import-existing")
    public MovieDataBatchService.JobRunResponse importTmdbExisting(
            @RequestParam(defaultValue = "200") int limit
    ) {
        return movieDataBatchService.runTmdbImportExisting(limit);
    }

    @PostMapping("/tmdb/normalize")
    public MovieDataBatchService.JobRunResponse normalizeTmdb() {
        return movieDataBatchService.runTmdbNormalize();
    }

    @PostMapping("/tags/rebuild")
    public MovieDataBatchService.JobRunResponse rebuildTags() {
        return movieDataBatchService.runTagRebuild();
    }

    @PostMapping("/recommendation/profile")
    public MovieDataBatchService.JobRunResponse rebuildPreferenceProfile(
            @RequestParam Long userId
    ) {
        return movieDataBatchService.runPreferenceProfileRebuild(userId);
    }

    @PostMapping("/recommendation/ranking")
    public MovieDataBatchService.JobRunResponse rebuildRecommendationRanking(
            @RequestParam Long userId,
            @RequestParam(required = false) Integer limit
    ) {
        return movieDataBatchService.runRecommendationRankingRebuild(userId, limit);
    }

    @PostMapping("/test-users/seed")
    public MovieDataBatchService.JobRunResponse seedTestUsers(
            @RequestParam(defaultValue = "false") boolean reset
    ) {
        return movieDataBatchService.runTestUserSeed(reset);
    }

    @PostMapping("/test-users/activities")
    public MovieDataBatchService.JobRunResponse seedTestUserActivities(
            @RequestParam(defaultValue = "false") boolean reset,
            @RequestParam(defaultValue = "false") boolean refreshRecommendations
    ) {
        return movieDataBatchService.runTestUserActivitySeed(reset, refreshRecommendations);
    }

    @PostMapping("/recommendation/refresh-dirty")
    public MovieDataBatchService.JobRunResponse refreshDirtyRecommendations(
            @RequestParam(defaultValue = "20") int batchSize,
            @RequestParam(required = false) Integer limit
    ) {
        return movieDataBatchService.runRefreshDirtyRecommendations(batchSize, limit);
    }

    @GetMapping("/recommendation/blocks")
    public com.cinematch.recommendation.RecommendationBlockService.RecommendationBlockResponse recommendationBlocks(
            @RequestParam Long userId,
            @RequestParam(defaultValue = "true") boolean refreshIfDirty,
            @RequestParam(required = false) Integer sliceLimit,
            @RequestParam(required = false) Integer itemLimit
    ) {
        return movieDataBatchService.previewRecommendationBlocks(userId, refreshIfDirty, sliceLimit, itemLimit);
    }

    @GetMapping("/recommendation/validate")
    public com.cinematch.recommendation.RecommendationValidationService.ValidationResponse validateRecommendations(
            @RequestParam(required = false) String loginId,
            @RequestParam(defaultValue = "true") boolean refreshIfDirty,
            @RequestParam(required = false) Integer topRecommendationLimit
    ) {
        return movieDataBatchService.validateRecommendations(loginId, refreshIfDirty, topRecommendationLimit);
    }

    @PostMapping("/run")
    public MovieDataBatchService.PipelineRunResponse runPipeline(
            @RequestParam(required = false) Integer startYear,
            @RequestParam(required = false) Integer endYear,
            @RequestParam(defaultValue = "20") int targetPerYear,
            @RequestParam(defaultValue = "200") int tmdbLimit
    ) {
        return movieDataBatchService.runPipeline(startYear, endYear, targetPerYear, tmdbLimit);
    }
}
