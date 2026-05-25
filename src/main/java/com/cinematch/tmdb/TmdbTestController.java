package com.cinematch.tmdb;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class TmdbTestController {

    private final TmdbMovieImportService tmdbMovieImportService;
    private final TmdbMovieNormalizeService tmdbMovieNormalizeService;
    private final JdbcTemplate jdbcTemplate;

    public TmdbTestController(
            TmdbMovieImportService tmdbMovieImportService,
            TmdbMovieNormalizeService tmdbMovieNormalizeService,
            JdbcTemplate jdbcTemplate
    ) {
        this.tmdbMovieImportService = tmdbMovieImportService;
        this.tmdbMovieNormalizeService = tmdbMovieNormalizeService;
        this.jdbcTemplate = jdbcTemplate;
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

    @GetMapping("/tmdb/video-stats")
    public Map<String, Object> videoStats() {
        long totalMovies = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM movie", Long.class);
        long moviesWithVideo = jdbcTemplate.queryForObject(
                "SELECT COUNT(DISTINCT movie_id) FROM movie_video", Long.class);
        var byType = jdbcTemplate.queryForList(
                "SELECT video_type, COUNT(*) AS cnt FROM movie_video GROUP BY video_type ORDER BY cnt DESC");
        var noVideoSample = jdbcTemplate.queryForList("""
                SELECT COALESCE(m.title, m.movie_name) AS title
                FROM movie m
                WHERE NOT EXISTS (SELECT 1 FROM movie_video v WHERE v.movie_id = m.id)
                  AND EXISTS (SELECT 1 FROM movie_source ms WHERE ms.movie_id = m.id AND ms.source_type = 'TMDB')
                ORDER BY m.popularity DESC NULLS LAST
                LIMIT 10
                """);
        return Map.of(
                "totalMovies", totalMovies,
                "moviesWithVideo", moviesWithVideo,
                "moviesWithoutVideo", totalMovies - moviesWithVideo,
                "byType", byType,
                "noVideoSample_top10popular", noVideoSample
        );
    }
}
