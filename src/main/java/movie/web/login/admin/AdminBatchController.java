package movie.web.login.admin;

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
