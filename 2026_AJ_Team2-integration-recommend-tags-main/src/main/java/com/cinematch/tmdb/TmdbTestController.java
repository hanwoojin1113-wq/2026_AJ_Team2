package com.cinematch.tmdb;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TmdbTestController {

    private final TmdbMovieImportService tmdbMovieImportService;
    private final TmdbMovieNormalizeService tmdbMovieNormalizeService;

    public TmdbTestController(
            TmdbMovieImportService tmdbMovieImportService,
            TmdbMovieNormalizeService tmdbMovieNormalizeService
    ) {
        this.tmdbMovieImportService = tmdbMovieImportService;
        this.tmdbMovieNormalizeService = tmdbMovieNormalizeService;
    }

    @GetMapping("/tmdb/test")
    public String testTmdb() {
        return "TMDB import endpoint is ready. Send POST /tmdb/import, /tmdb/import-existing, /tmdb/normalize";
    }

    @PostMapping("/tmdb/import")
    public TmdbMovieImportService.ImportResult importMovies() {
        return tmdbMovieImportService.importPopularMovies();
    }

    @PostMapping("/tmdb/import-existing")
    public TmdbMovieImportService.ExistingImportResult importExistingMovies(
            @RequestParam(defaultValue = "100") int limit
    ) {
        return tmdbMovieImportService.importExistingMovies(limit);
    }

    @PostMapping("/tmdb/normalize")
    public TmdbMovieNormalizeService.NormalizeResult normalizeMovies() {
        return tmdbMovieNormalizeService.normalizeRawMovies();
    }
}
