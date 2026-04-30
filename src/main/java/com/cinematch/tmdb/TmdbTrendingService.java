package com.cinematch.tmdb;

import com.cinematch.chart.ChartMovieRow;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Service
public class TmdbTrendingService {

    private static final String TMDB_IMAGE_BASE_URL = "https://image.tmdb.org/t/p/w500";
    private static final int MAX_TRENDING_MOVIES = 10;

    private final RestTemplate tmdbRestTemplate;
    private final ObjectMapper objectMapper;
    private final JdbcTemplate jdbcTemplate;
    private final TmdbMovieImportService tmdbMovieImportService;
    private final TmdbMovieNormalizeService tmdbMovieNormalizeService;
    private final String baseUrl;
    private final String token;

    public TmdbTrendingService(
            RestTemplate tmdbRestTemplate,
            ObjectMapper objectMapper,
            JdbcTemplate jdbcTemplate,
            TmdbMovieImportService tmdbMovieImportService,
            TmdbMovieNormalizeService tmdbMovieNormalizeService,
            @Value("${tmdb.base-url}") String baseUrl,
            @Value("${tmdb.token:}") String token
    ) {
        this.tmdbRestTemplate = tmdbRestTemplate;
        this.objectMapper = objectMapper;
        this.jdbcTemplate = jdbcTemplate;
        this.tmdbMovieImportService = tmdbMovieImportService;
        this.tmdbMovieNormalizeService = tmdbMovieNormalizeService;
        this.baseUrl = baseUrl;
        this.token = token;
    }

    public List<TrendingMovieView> fetchTrendingMovies(int limit) {
        int normalizedLimit = normalizeLimit(limit);
        initializeTrendingTable();

        try {
            syncTrendingCatalog(normalizedLimit);
        } catch (Exception ignored) {
            // TMDB 실패가 홈 전체를 깨지 않게 하고, 기존 저장 스냅샷이 있으면 그걸 재사용한다.
        }

        List<TrendingMovieView> storedViews = loadStoredTrendingViews(normalizedLimit);
        if (!storedViews.isEmpty() || token == null || token.isBlank()) {
            return storedViews;
        }

        List<TrendingSourceMovie> sourceMovies = fetchTrendingSourceMovies(normalizedLimit);
        List<TrendingMovieView> fallbackViews = new ArrayList<>();
        for (TrendingSourceMovie movie : sourceMovies) {
            String movieCode = findLocalMovieCode(movie.tmdbMovieId());
            fallbackViews.add(new TrendingMovieView(
                    movieCode == null ? null : "/movies/" + movieCode,
                    movie.title(),
                    movie.subtitle(),
                    movie.posterImageUrl()
            ));
        }
        return fallbackViews;
    }

    public List<ChartMovieRow> fetchTrendingChartRows(int limit) {
        int normalizedLimit = normalizeLimit(limit);
        initializeTrendingTable();

        try {
            syncTrendingCatalog(normalizedLimit);
        } catch (Exception ignored) {
            // chart는 기존 스냅샷만으로도 열려야 하므로 예외를 삼키고 저장본을 그대로 사용한다.
        }

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

    @Transactional
    public int syncTrendingCatalog(int limit) {
        int normalizedLimit = normalizeLimit(limit);
        if (token == null || token.isBlank()) {
            return 0;
        }

        initializeTrendingTable();
        List<TrendingSourceMovie> sourceMovies = fetchTrendingSourceMovies(normalizedLimit);
        if (sourceMovies.isEmpty()) {
            return 0;
        }

        List<Long> tmdbMovieIds = sourceMovies.stream()
                .map(TrendingSourceMovie::tmdbMovieId)
                .toList();

        tmdbMovieImportService.importMovieIds(tmdbMovieIds);
        tmdbMovieNormalizeService.normalizeMovieIds(tmdbMovieIds);

        List<TrendingSnapshotRow> snapshotRows = resolveSnapshotRows(sourceMovies);
        if (snapshotRows.isEmpty()) {
            return 0;
        }

        replaceTrendingSnapshot(snapshotRows);
        return snapshotRows.size();
    }

    private List<TrendingSourceMovie> fetchTrendingSourceMovies(int limit) {
        if (token == null || token.isBlank() || limit <= 0) {
            return List.of();
        }

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

    private List<TrendingSnapshotRow> resolveSnapshotRows(List<TrendingSourceMovie> sourceMovies) {
        List<TrendingSnapshotRow> rows = new ArrayList<>();
        for (TrendingSourceMovie movie : sourceMovies) {
            Long movieId = findLocalMovieId(movie.tmdbMovieId());
            if (movieId == null) {
                continue;
            }
            rows.add(new TrendingSnapshotRow(movie.rankNo(), movie.tmdbMovieId(), movieId));
        }
        return rows;
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
                .path("/trending/movie/week")
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

    private String findLocalMovieCode(Long tmdbMovieId) {
        String sourceKey = String.valueOf(tmdbMovieId);
        String tmdbMovieCode = "TMDB-" + tmdbMovieId;

        return jdbcTemplate.query("""
                SELECT m.movie_cd
                FROM movie m
                LEFT JOIN movie_source ms
                  ON ms.movie_id = m.id
                 AND ms.source_type = 'TMDB'
                 AND ms.source_key = ?
                WHERE ms.movie_id IS NOT NULL
                   OR m.movie_cd = ?
                ORDER BY CASE WHEN ms.movie_id IS NOT NULL THEN 0 ELSE 1 END, m.id
                LIMIT 1
                """, rs -> rs.next() ? rs.getString("movie_cd") : null, sourceKey, tmdbMovieCode);
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
