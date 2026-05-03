package com.cinematch.tmdb;

import com.cinematch.chart.ChartMovieRow;
import com.cinematch.tag.MovieTagService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@Service
public class TmdbTrendingService {

    private static final Logger log = LoggerFactory.getLogger(TmdbTrendingService.class);

    private static final String TMDB_IMAGE_BASE_URL = "https://image.tmdb.org/t/p/w500";
    private static final int MAX_TRENDING_MOVIES = 10;
    private static final long REFRESH_STALE_HOURS = 24L;

    private final RestTemplate tmdbRestTemplate;
    private final ObjectMapper objectMapper;
    private final JdbcTemplate jdbcTemplate;
    private final TmdbMovieImportService tmdbMovieImportService;
    private final TmdbMovieNormalizeService tmdbMovieNormalizeService;
    private final MovieTagService movieTagService;
    private final String baseUrl;
    private final String token;

    public TmdbTrendingService(
            RestTemplate tmdbRestTemplate,
            ObjectMapper objectMapper,
            JdbcTemplate jdbcTemplate,
            TmdbMovieImportService tmdbMovieImportService,
            TmdbMovieNormalizeService tmdbMovieNormalizeService,
            MovieTagService movieTagService,
            @Value("${tmdb.base-url}") String baseUrl,
            @Value("${tmdb.token:}") String token
    ) {
        this.tmdbRestTemplate = tmdbRestTemplate;
        this.objectMapper = objectMapper;
        this.jdbcTemplate = jdbcTemplate;
        this.tmdbMovieImportService = tmdbMovieImportService;
        this.tmdbMovieNormalizeService = tmdbMovieNormalizeService;
        this.movieTagService = movieTagService;
        this.baseUrl = baseUrl;
        this.token = token;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void refreshTrendingCatalogOnStartup() {
        try {
            TrendingRefreshResult result = refreshTrendingCatalogIfStale();
            if (result.skipped()) {
                log.info("TMDB trending startup refresh skipped: {}", result.skippedReason());
                return;
            }
            log.info(
                    "TMDB trending startup refresh completed: fetched={}, inserted={}, reused={}, taggedRows={}, categoryUpdated={}",
                    result.fetchedCount(),
                    result.insertedCount(),
                    result.reusedCount(),
                    result.taggedRowCount(),
                    result.categoryUpdated()
            );
        } catch (Exception e) {
            log.warn("TMDB trending startup refresh failed. Existing snapshot will be kept.", e);
        }
    }

    public List<TrendingMovieView> fetchTrendingMovies(int limit) {
        initializeTrendingTable();
        return loadStoredTrendingViews(normalizeLimit(limit));
    }

    public List<ChartMovieRow> fetchTrendingChartRows(int limit) {
        int normalizedLimit = normalizeLimit(limit);
        initializeTrendingTable();

        return jdbcTemplate.query("""
                SELECT
                    tc.rank_no,
                    m.movie_cd,
                    COALESCE(m.title, m.movie_name) AS movie_name,
                    COALESCE(m.movie_name_en, m.original_title, m.movie_name_original) AS movie_name_en,
                    m.poster_image_url,
                    COALESCE(m.release_date, m.movie_info_open_date, m.box_office_open_date) AS open_date,
                    m.production_year
                FROM tmdb_trending_chart tc
                JOIN movie m ON m.id = tc.movie_id
                WHERE m.poster_image_url IS NOT NULL
                  AND m.poster_image_url <> ''
                ORDER BY tc.rank_no
                LIMIT ?
                """, (rs, rowNum) -> new ChartMovieRow(
                rs.getInt("rank_no"),
                rs.getString("movie_cd"),
                rs.getString("movie_name"),
                rs.getString("movie_name_en"),
                rs.getString("poster_image_url"),
                rs.getObject("open_date", LocalDate.class),
                rs.getObject("production_year", Integer.class),
                "실시간 순위",
                "#" + rs.getInt("rank_no"),
                "TRENDING"
        ), normalizedLimit);
    }

    public TrendingRefreshResult refreshTrendingCatalogIfStale() {
        return refreshTrendingCatalog(false, MAX_TRENDING_MOVIES);
    }

    public TrendingRefreshResult refreshTrendingCatalog() {
        return refreshTrendingCatalog(true, MAX_TRENDING_MOVIES);
    }

    public TrendingRefreshResult refreshTrendingCatalog(boolean force, int limit) {
        int normalizedLimit = normalizeLimit(limit);
        initializeTrendingTable();

        if (token == null || token.isBlank()) {
            return TrendingRefreshResult.skipped("TMDB token is empty.");
        }

        LocalDateTime lastRefreshedAt = loadLastRefreshedAt();
        if (!force && !isRefreshRequired(lastRefreshedAt)) {
            return TrendingRefreshResult.skipped("TMDB trending snapshot is still fresh.");
        }

        List<TrendingSourceMovie> sourceMovies;
        try {
            sourceMovies = fetchTrendingSourceMovies(normalizedLimit);
        } catch (Exception e) {
            log.warn("TMDB trending fetch failed. Existing snapshot will be kept.", e);
            return TrendingRefreshResult.skipped("TMDB API fetch failed.");
        }

        if (sourceMovies.isEmpty()) {
            return TrendingRefreshResult.skipped("TMDB trending response was empty.");
        }

        int fetchedCount = sourceMovies.size();
        int insertedCount = 0;
        int reusedCount = 0;
        int taggedRowCount = 0;

        Set<Long> movieIdsToTag = new LinkedHashSet<>();
        List<TrendingSnapshotRow> snapshotRows = new ArrayList<>();

        for (TrendingSourceMovie sourceMovie : sourceMovies) {
            Long movieId = findLocalMovieId(sourceMovie.tmdbMovieId());
            if (movieId != null) {
                reusedCount++;
                if (!hasMovieTags(movieId)) {
                    movieIdsToTag.add(movieId);
                }
                snapshotRows.add(new TrendingSnapshotRow(sourceMovie.rankNo(), sourceMovie.tmdbMovieId(), movieId));
                continue;
            }

            try {
                tmdbMovieImportService.importMovieIds(List.of(sourceMovie.tmdbMovieId()));
                tmdbMovieNormalizeService.normalizeMovieIds(List.of(sourceMovie.tmdbMovieId()));

                movieId = findLocalMovieId(sourceMovie.tmdbMovieId());
                if (movieId == null) {
                    log.warn("TMDB trending movie {} could not be resolved to a local movie row after normalize.", sourceMovie.tmdbMovieId());
                    continue;
                }

                insertedCount++;
                movieIdsToTag.add(movieId);
                snapshotRows.add(new TrendingSnapshotRow(sourceMovie.rankNo(), sourceMovie.tmdbMovieId(), movieId));
            } catch (Exception e) {
                log.warn("TMDB trending movie {} import/normalize failed. Skipping this movie.", sourceMovie.tmdbMovieId(), e);
            }
        }

        if (!movieIdsToTag.isEmpty()) {
            try {
                taggedRowCount = movieTagService.tagMovies(movieIdsToTag).totalTaggedRows();
            } catch (Exception e) {
                log.warn("TMDB trending tag generation failed for movieIds={}. Continuing without failing the refresh.", movieIdsToTag, e);
            }
        }

        boolean categoryUpdated = false;
        if (!snapshotRows.isEmpty()) {
            replaceTrendingSnapshot(snapshotRows);
            categoryUpdated = true;
        }

        return new TrendingRefreshResult(
                false,
                null,
                fetchedCount,
                insertedCount,
                reusedCount,
                taggedRowCount,
                categoryUpdated,
                loadLastRefreshedAt()
        );
    }

    public Integer findTrendingRank(Long movieId) {
        if (movieId == null) {
            return null;
        }
        initializeTrendingTable();
        return jdbcTemplate.query("""
                SELECT rank_no
                FROM tmdb_trending_chart
                WHERE movie_id = ?
                """, rs -> rs.next() ? rs.getInt("rank_no") : null, movieId);
    }

    private boolean isRefreshRequired(LocalDateTime lastRefreshedAt) {
        if (lastRefreshedAt == null) {
            return true;
        }
        long elapsedHours = ChronoUnit.HOURS.between(lastRefreshedAt, LocalDateTime.now());
        return elapsedHours >= REFRESH_STALE_HOURS;
    }

    private LocalDateTime loadLastRefreshedAt() {
        initializeTrendingTable();
        Timestamp timestamp = jdbcTemplate.query("""
                SELECT MAX(fetched_at) AS fetched_at
                FROM tmdb_trending_chart
                """, rs -> rs.next() ? rs.getTimestamp("fetched_at") : null);
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }

    private List<TrendingSourceMovie> fetchTrendingSourceMovies(int limit) {
        JsonNode root = readTree(tmdbRestTemplate.getForObject(buildTrendingMovieUri(), String.class));
        if (root == null || !root.path("results").isArray()) {
            return List.of();
        }

        List<TrendingSourceMovie> movies = new ArrayList<>();
        int rankNo = 1;
        for (JsonNode movieNode : root.path("results")) {
            if (movies.size() >= limit) {
                break;
            }

            Long tmdbMovieId = longValue(movieNode, "id");
            String title = coalesce(textValue(movieNode, "title"), textValue(movieNode, "original_title"));
            if (tmdbMovieId == null || title == null) {
                continue;
            }

            movies.add(new TrendingSourceMovie(
                    rankNo++,
                    tmdbMovieId,
                    title,
                    buildSubtitle(movieNode),
                    toPosterImageUrl(textValue(movieNode, "poster_path"))
            ));
        }
        return movies;
    }

    private void replaceTrendingSnapshot(List<TrendingSnapshotRow> snapshotRows) {
        jdbcTemplate.update("DELETE FROM tmdb_trending_chart");

        Timestamp now = Timestamp.valueOf(LocalDateTime.now());
        for (TrendingSnapshotRow row : snapshotRows) {
            jdbcTemplate.update("""
                    INSERT INTO tmdb_trending_chart (
                        rank_no,
                        tmdb_movie_id,
                        movie_id,
                        fetched_at
                    ) VALUES (?, ?, ?, ?)
                    """, row.rankNo(), row.tmdbMovieId(), row.movieId(), now);
        }
    }

    private List<TrendingMovieView> loadStoredTrendingViews(int limit) {
        return jdbcTemplate.query("""
                SELECT
                    tc.rank_no,
                    m.movie_cd,
                    COALESCE(m.title, m.movie_name) AS movie_name,
                    COALESCE(m.movie_name_en, m.original_title, m.movie_name_original) AS movie_name_en,
                    m.poster_image_url,
                    COALESCE(m.release_date, m.movie_info_open_date, m.box_office_open_date) AS release_date,
                    m.production_year
                FROM tmdb_trending_chart tc
                JOIN movie m ON m.id = tc.movie_id
                WHERE m.poster_image_url IS NOT NULL
                  AND m.poster_image_url <> ''
                ORDER BY tc.rank_no
                LIMIT ?
                """, (rs, rowNum) -> new TrendingMovieView(
                "/movies/" + rs.getString("movie_cd"),
                rs.getString("movie_name"),
                buildStoredSubtitle(
                        rs.getString("movie_name_en"),
                        rs.getObject("release_date", LocalDate.class),
                        rs.getObject("production_year", Integer.class)
                ),
                rs.getString("poster_image_url")
        ), limit);
    }

    private boolean hasMovieTags(Long movieId) {
        Integer count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM movie_tag
                WHERE movie_id = ?
                """, Integer.class, movieId);
        return count != null && count > 0;
    }

    private String buildStoredSubtitle(String movieNameEn, LocalDate releaseDate, Integer productionYear) {
        Integer year = releaseDate != null ? releaseDate.getYear() : productionYear;
        if (year == null) {
            return movieNameEn;
        }
        return movieNameEn == null || movieNameEn.isBlank()
                ? String.valueOf(year)
                : movieNameEn + " · " + year;
    }

    private void initializeTrendingTable() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS tmdb_trending_chart (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    rank_no INT NOT NULL UNIQUE,
                    tmdb_movie_id BIGINT NOT NULL UNIQUE,
                    movie_id BIGINT NOT NULL UNIQUE,
                    fetched_at TIMESTAMP NOT NULL,
                    CONSTRAINT fk_tmdb_trending_chart_movie FOREIGN KEY (movie_id) REFERENCES movie(id)
                )
                """);
    }

    private int normalizeLimit(int limit) {
        return Math.max(1, Math.min(limit, MAX_TRENDING_MOVIES));
    }

    private URI buildTrendingMovieUri() {
        return UriComponentsBuilder.fromUriString(baseUrl)
                .path("/trending/movie/day")
                .queryParam("language", "ko-KR")
                .build()
                .toUri();
    }

    private JsonNode readTree(String response) {
        try {
            return response == null ? null : objectMapper.readTree(response);
        } catch (JacksonException e) {
            throw new IllegalStateException("TMDB trending JSON parsing failed.", e);
        }
    }

    private Long longValue(JsonNode node, String fieldName) {
        JsonNode value = node.get(fieldName);
        return value == null || value.isNull() ? null : value.asLong();
    }

    private String textValue(JsonNode node, String fieldName) {
        JsonNode value = node.get(fieldName);
        if (value == null || value.isNull()) {
            return null;
        }
        String text = value.asText();
        return text == null || text.isBlank() ? null : text;
    }

    private String toPosterImageUrl(String posterPath) {
        return posterPath == null ? null : TMDB_IMAGE_BASE_URL + posterPath;
    }

    private String buildSubtitle(JsonNode movieNode) {
        String titleEn = textValue(movieNode, "original_title");
        LocalDate releaseDate = localDateValue(movieNode, "release_date");
        if (releaseDate != null) {
            return titleEn == null || titleEn.isBlank()
                    ? String.valueOf(releaseDate.getYear())
                    : titleEn + " · " + releaseDate.getYear();
        }
        return titleEn;
    }

    private LocalDate localDateValue(JsonNode node, String fieldName) {
        String text = textValue(node, fieldName);
        return text == null ? null : LocalDate.parse(text);
    }

    private Long findLocalMovieId(Long tmdbMovieId) {
        String sourceKey = String.valueOf(tmdbMovieId);
        String tmdbMovieCode = "TMDB-" + tmdbMovieId;

        return jdbcTemplate.query("""
                SELECT m.id
                FROM movie m
                LEFT JOIN movie_source ms
                  ON ms.movie_id = m.id
                 AND ms.source_type = 'TMDB'
                 AND ms.source_key = ?
                WHERE ms.movie_id IS NOT NULL
                   OR m.movie_cd = ?
                ORDER BY CASE WHEN ms.movie_id IS NOT NULL THEN 0 ELSE 1 END, m.id
                LIMIT 1
                """, rs -> rs.next() ? rs.getLong("id") : null, sourceKey, tmdbMovieCode);
    }

    private String coalesce(String first, String second) {
        return first != null && !first.isBlank() ? first : second;
    }

    public record TrendingMovieView(
            String detailUrl,
            String title,
            String subtitle,
            String posterImageUrl
    ) {
    }

    public record TrendingRefreshResult(
            boolean skipped,
            String skippedReason,
            int fetchedCount,
            int insertedCount,
            int reusedCount,
            int taggedRowCount,
            boolean categoryUpdated,
            LocalDateTime refreshedAt
    ) {
        public static TrendingRefreshResult skipped(String skippedReason) {
            return new TrendingRefreshResult(true, skippedReason, 0, 0, 0, 0, false, null);
        }
    }

    private record TrendingSourceMovie(
            int rankNo,
            Long tmdbMovieId,
            String title,
            String subtitle,
            String posterImageUrl
    ) {
    }

    private record TrendingSnapshotRow(
            int rankNo,
            Long tmdbMovieId,
            Long movieId
    ) {
    }
}
