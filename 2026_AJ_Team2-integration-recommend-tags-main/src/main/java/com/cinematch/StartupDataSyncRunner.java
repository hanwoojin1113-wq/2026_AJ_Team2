package com.cinematch;

import com.cinematch.kobis.KobisMovieImportService;
import com.cinematch.kobis.KobisMovieNormalizeService;
import com.cinematch.tag.MovieTagService;
import com.cinematch.tmdb.TmdbMovieImportService;
import com.cinematch.tmdb.TmdbMovieNormalizeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Component
@Order(3)
@ConditionalOnProperty(value = "app.startup-sync.enabled", havingValue = "true")
public class StartupDataSyncRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(StartupDataSyncRunner.class);

    private final KobisMovieImportService kobisMovieImportService;
    private final KobisMovieNormalizeService kobisMovieNormalizeService;
    private final TmdbMovieImportService tmdbMovieImportService;
    private final TmdbMovieNormalizeService tmdbMovieNormalizeService;
    private final MovieTagService movieTagService;
    private final int importExistingLimit;
    private final Integer kobisStartYear;
    private final Integer kobisEndYear;
    private final int kobisTargetPerYear;
    private final String kobisApiKey;
    private final String tmdbToken;

    public StartupDataSyncRunner(
            KobisMovieImportService kobisMovieImportService,
            KobisMovieNormalizeService kobisMovieNormalizeService,
            TmdbMovieImportService tmdbMovieImportService,
            TmdbMovieNormalizeService tmdbMovieNormalizeService,
            MovieTagService movieTagService,
            @Value("${app.startup-sync.import-existing-limit:200}") int importExistingLimit,
            @Value("${app.startup-sync.kobis-start-year:2016}") Integer kobisStartYear,
            @Value("${app.startup-sync.kobis-end-year:2025}") Integer kobisEndYear,
            @Value("${app.startup-sync.kobis-target-per-year:20}") int kobisTargetPerYear,
            @Value("${kobis.api-key:}") String kobisApiKey,
            @Value("${tmdb.token:}") String tmdbToken
    ) {
        this.kobisMovieImportService = kobisMovieImportService;
        this.kobisMovieNormalizeService = kobisMovieNormalizeService;
        this.tmdbMovieImportService = tmdbMovieImportService;
        this.tmdbMovieNormalizeService = tmdbMovieNormalizeService;
        this.movieTagService = movieTagService;
        this.importExistingLimit = importExistingLimit;
        this.kobisStartYear = kobisStartYear;
        this.kobisEndYear = kobisEndYear;
        this.kobisTargetPerYear = kobisTargetPerYear;
        this.kobisApiKey = kobisApiKey;
        this.tmdbToken = tmdbToken;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (kobisApiKey != null && !kobisApiKey.isBlank()) {
            log.info("Startup sync started: KOBIS yearly import startYear={}, endYear={}, targetPerYear={}",
                    kobisStartYear, kobisEndYear, kobisTargetPerYear);
            KobisMovieImportService.YearlyImportResult kobisImportResult =
                    kobisMovieImportService.importYearlyRepresentativeMovies(kobisStartYear, kobisEndYear, kobisTargetPerYear);
            log.info("KOBIS yearly import completed: collected={}, savedRaw={}, updatedRaw={}",
                    kobisImportResult.collectedMovieCount(),
                    kobisImportResult.savedRawCount(),
                    kobisImportResult.updatedRawCount());

            KobisMovieNormalizeService.NormalizeResult kobisNormalizeResult = kobisMovieNormalizeService.normalizeRawMovies();
            log.info("KOBIS normalize completed: processed={}, insertedMovie={}, updatedMovie={}, matchedExisting={}, insertedSource={}",
                    kobisNormalizeResult.processedCount(),
                    kobisNormalizeResult.insertedMovieCount(),
                    kobisNormalizeResult.updatedMovieCount(),
                    kobisNormalizeResult.matchedExistingMovieCount(),
                    kobisNormalizeResult.insertedSourceCount());
        } else {
            log.warn("KOBIS startup sync skipped: KOBIS API key is empty");
        }

        if (tmdbToken == null || tmdbToken.isBlank()) {
            log.warn("TMDB startup sync skipped: TMDB token is empty");
        } else {
            log.info("Startup sync started: TMDB import-existing limit={}", importExistingLimit);
            TmdbMovieImportService.ExistingImportResult importResult =
                    tmdbMovieImportService.importExistingMovies(importExistingLimit);
            log.info("TMDB import-existing completed: processed={}, matched={}, noMatch={}, savedRaw={}, updatedRaw={}",
                    importResult.processedCount(),
                    importResult.matchedCount(),
                    importResult.noMatchCount(),
                    importResult.savedRawCount(),
                    importResult.updatedRawCount());

            TmdbMovieNormalizeService.NormalizeResult normalizeResult = tmdbMovieNormalizeService.normalizeRawMovies();
            log.info("TMDB normalize completed: processed={}, insertedMovie={}, updatedMovie={}, matchedExisting={}, insertedSource={}",
                    normalizeResult.processedCount(),
                    normalizeResult.insertedMovieCount(),
                    normalizeResult.updatedMovieCount(),
                    normalizeResult.matchedExistingMovieCount(),
                    normalizeResult.insertedSourceCount());
        }

        MovieTagService.RebuildResult rebuildResult = movieTagService.rebuildTags();
        log.info("Tag rebuild completed: candidates={}, taggedRows={}",
                rebuildResult.candidateCount(),
                rebuildResult.totalTaggedRows());
    }
}
