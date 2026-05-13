package com.cinematch.tmdb;

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
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
public class TmdbMovieImportService {

    private static final int TARGET_MOVIE_COUNT = 300;
    private static final int MOVIES_PER_PAGE = 20;
    private static final int MAX_EXISTING_IMPORT_LIMIT = 1000;
    private static final int MIN_MATCH_SCORE = 80;
    private static final int NEAR_MISS_MIN_SCORE = 60;
    private static final int MAX_SHORTLIST_SIZE = 3;

    private final RestTemplate tmdbRestTemplate;
    private final ObjectMapper objectMapper;
    private final JdbcTemplate jdbcTemplate;
    private final String baseUrl;

    public TmdbMovieImportService(
            RestTemplate tmdbRestTemplate,
            ObjectMapper objectMapper,
            JdbcTemplate jdbcTemplate,
            @Value("${tmdb.base-url}") String baseUrl
    ) {
        this.tmdbRestTemplate = tmdbRestTemplate;
        this.objectMapper = objectMapper;
        this.jdbcTemplate = jdbcTemplate;
        this.baseUrl = baseUrl;
    }

    @Transactional
    public ImportResult importPopularMovies() {
        initializeRawTable();
        Set<Long> linked = loadLinkedTmdbIds();

        int requestedCount = 0;
        int savedRawCount = 0;
        int updatedRawCount = 0;

        int lastPage = TARGET_MOVIE_COUNT / MOVIES_PER_PAGE;
        for (int page = 1; page <= lastPage; page++) {
            JsonNode root = readTree(tmdbRestTemplate.getForObject(buildPopularMovieUri(page), String.class));
            if (root == null || !root.path("results").isArray()) {
                continue;
            }

            for (JsonNode movieNode : root.path("results")) {
                Long tmdbMovieId = longValue(movieNode, "id");
                if (tmdbMovieId == null) {
                    continue;
                }

                requestedCount++;
                if (linked.contains(tmdbMovieId)) {
                    continue;
                }

                JsonNode detailNode = readTree(tmdbRestTemplate.getForObject(buildMovieDetailUri(tmdbMovieId), String.class));
                if (detailNode == null) {
                    continue;
                }

                if (upsertRawMovie(tmdbMovieId, toJson(detailNode))) {
                    savedRawCount++;
                } else {
                    updatedRawCount++;
                }
            }
        }

        return new ImportResult(requestedCount, savedRawCount, updatedRawCount);
    }

    @Transactional
    public ImportResult importTopRatedMovies(int maxPages) {
        initializeRawTable();
        Set<Long> linked = loadLinkedTmdbIds();

        int requestedCount = 0;
        int savedRawCount = 0;
        int updatedRawCount = 0;

        for (int page = 1; page <= maxPages; page++) {
            JsonNode root = readTree(tmdbRestTemplate.getForObject(buildTopRatedUri(page), String.class));
            if (root == null || !root.path("results").isArray()) {
                break;
            }

            for (JsonNode movieNode : root.path("results")) {
                Long tmdbMovieId = longValue(movieNode, "id");
                if (tmdbMovieId == null) {
                    continue;
                }

                requestedCount++;
                if (linked.contains(tmdbMovieId)) {
                    continue;
                }

                JsonNode detailNode = readTree(tmdbRestTemplate.getForObject(buildMovieDetailUri(tmdbMovieId), String.class));
                if (detailNode == null) {
                    continue;
                }

                if (upsertRawMovie(tmdbMovieId, toJson(detailNode))) {
                    savedRawCount++;
                } else {
                    updatedRawCount++;
                }
            }
        }

        return new ImportResult(requestedCount, savedRawCount, updatedRawCount);
    }

    /**
     * TMDB Discover API로 영화 수집.
     * source: "korean-ott" | "korean-movies" | "high-rated"
     */
    @Transactional
    public ImportResult importDiscoverMovies(String source, int maxPages) {
        initializeRawTable();
        Set<Long> linked = loadLinkedTmdbIds();

        int requestedCount = 0;
        int savedRawCount = 0;
        int updatedRawCount = 0;

        for (int page = 1; page <= maxPages; page++) {
            JsonNode root = readTree(tmdbRestTemplate.getForObject(buildDiscoverUri(source, page), String.class));
            if (root == null || !root.path("results").isArray()) {
                break;
            }

            for (JsonNode movieNode : root.path("results")) {
                Long tmdbMovieId = longValue(movieNode, "id");
                if (tmdbMovieId == null) {
                    continue;
                }

                requestedCount++;
                if (linked.contains(tmdbMovieId)) {
                    continue;
                }

                JsonNode detailNode = readTree(tmdbRestTemplate.getForObject(buildMovieDetailUri(tmdbMovieId), String.class));
                if (detailNode == null) {
                    continue;
                }

                if (upsertRawMovie(tmdbMovieId, toJson(detailNode))) {
                    savedRawCount++;
                } else {
                    updatedRawCount++;
                }
            }
        }

        return new ImportResult(requestedCount, savedRawCount, updatedRawCount);
    }

    @Transactional
    public ExistingImportResult importExistingMovies(int limit) {
        initializeRawTable();

        int normalizedLimit = Math.min(Math.max(limit, 1), MAX_EXISTING_IMPORT_LIMIT);
        List<ExistingMovieCandidate> candidates = loadExistingMovieCandidates(normalizedLimit);

        int processedCount = 0;
        int matchedCount = 0;
        int noMatchCount = 0;
        int savedRawCount = 0;
        int updatedRawCount = 0;

        for (ExistingMovieCandidate candidate : candidates) {
            processedCount++;

            JsonNode matchedMovie = findBestSearchMatch(candidate);
            if (matchedMovie == null) {
                noMatchCount++;
                continue;
            }

            Long tmdbMovieId = longValue(matchedMovie, "id");
            if (tmdbMovieId == null) {
                noMatchCount++;
                continue;
            }

            JsonNode detailNode = readTree(tmdbRestTemplate.getForObject(buildMovieDetailUri(tmdbMovieId), String.class));
            if (detailNode == null) {
                noMatchCount++;
                continue;
            }

            if (upsertRawMovie(tmdbMovieId, toJson(detailNode))) {
                savedRawCount++;
            } else {
                updatedRawCount++;
            }
            matchedCount++;
        }

        return new ExistingImportResult(processedCount, matchedCount, noMatchCount, savedRawCount, updatedRawCount);
    }

    @Transactional
    public ImportResult importMovieIds(List<Long> tmdbMovieIds) {
        initializeRawTable();

        if (tmdbMovieIds == null || tmdbMovieIds.isEmpty()) {
            return new ImportResult(0, 0, 0);
        }

        int requestedCount = 0;
        int savedRawCount = 0;
        int updatedRawCount = 0;

        for (Long tmdbMovieId : tmdbMovieIds.stream().filter(id -> id != null && id > 0).distinct().toList()) {
            requestedCount++;

            JsonNode detailNode = readTree(tmdbRestTemplate.getForObject(buildMovieDetailUri(tmdbMovieId), String.class));
            if (detailNode == null) {
                continue;
            }

            if (upsertRawMovie(tmdbMovieId, toJson(detailNode))) {
                savedRawCount++;
            } else {
                updatedRawCount++;
            }
        }

        return new ImportResult(requestedCount, savedRawCount, updatedRawCount);
    }

    private void initializeRawTable() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS tmdb_movie_raw (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    tmdb_movie_id BIGINT NOT NULL UNIQUE,
                    payload CLOB NOT NULL,
                    imported_at TIMESTAMP NOT NULL
                )
                """);
    }

    private boolean upsertRawMovie(Long tmdbMovieId, String payload) {
        Integer existingCount = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM tmdb_movie_raw
                WHERE tmdb_movie_id = ?
                """, Integer.class, tmdbMovieId);

        Timestamp now = Timestamp.valueOf(LocalDateTime.now());
        if (existingCount != null && existingCount > 0) {
            jdbcTemplate.update("""
                    UPDATE tmdb_movie_raw
                    SET payload = ?, imported_at = ?
                    WHERE tmdb_movie_id = ?
                    """, payload, now, tmdbMovieId);
            return false;
        }

        jdbcTemplate.update("""
                INSERT INTO tmdb_movie_raw (tmdb_movie_id, payload, imported_at)
                VALUES (?, ?, ?)
                """, tmdbMovieId, payload, now);
        return true;
    }

    public JsonNode findBestTmdbMatch(String title, LocalDate releaseDate) {
        return findBestSearchMatch(new ExistingMovieCandidate(null, title, null, releaseDate));
    }

    private List<ExistingMovieCandidate> loadExistingMovieCandidates(int limit) {
        return jdbcTemplate.query("""
                SELECT
                    m.id,
                    COALESCE(m.title, m.movie_name) AS display_title,
                    m.movie_name_original,
                    COALESCE(m.release_date, m.movie_info_open_date) AS release_date
                FROM movie m
                WHERE COALESCE(m.title, m.movie_name) IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1
                    FROM movie_source ms
                    WHERE ms.movie_id = m.id
                      AND ms.source_type = 'TMDB'
                )
                ORDER BY m.id
                LIMIT ?
                """, (rs, rowNum) -> new ExistingMovieCandidate(
                rs.getLong("id"),
                rs.getString("display_title"),
                rs.getString("movie_name_original"),
                rs.getObject("release_date", LocalDate.class)
        ), limit);
    }

    private JsonNode findBestSearchMatch(ExistingMovieCandidate candidate) {
        List<JsonNode> displayResults = searchMovieResults(candidate.displayTitle());
        JsonNode fastMatch = chooseBestMatch(candidate, displayResults);
        if (fastMatch != null) {
            return fastMatch;
        }

        List<JsonNode> originalResults = List.of();
        if (candidate.originalTitle() != null && !candidate.originalTitle().isBlank()
                && !candidate.originalTitle().equals(candidate.displayTitle())) {
            originalResults = searchMovieResults(candidate.originalTitle());
            fastMatch = chooseBestMatch(candidate, originalResults);
            if (fastMatch != null) {
                return fastMatch;
            }
        }

        // Near-miss path: search 결과의 제목이 어긋나는 경우(한글↔영문 차이 등)를
        // TMDB detail의 alternative_titles(KR) / translations(ko)로 재검증한다.
        List<JsonNode> allResults = new ArrayList<>(displayResults);
        allResults.addAll(originalResults);
        return findBestMatchWithAlternativeTitles(candidate, allResults);
    }

    private JsonNode findBestMatchWithAlternativeTitles(ExistingMovieCandidate candidate, List<JsonNode> results) {
        List<ScoredResult> shortlist = new ArrayList<>();
        for (JsonNode result : results) {
            int baseScore = scoreSearchResult(candidate, result);
            if (baseScore >= NEAR_MISS_MIN_SCORE) {
                shortlist.add(new ScoredResult(result, baseScore));
            }
        }
        shortlist.sort((a, b) -> b.score() - a.score());
        if (shortlist.size() > MAX_SHORTLIST_SIZE) {
            shortlist = shortlist.subList(0, MAX_SHORTLIST_SIZE);
        }
        if (shortlist.isEmpty()) {
            return null;
        }

        JsonNode bestMatch = null;
        int bestScore = MIN_MATCH_SCORE - 1;
        Set<Long> checkedIds = new HashSet<>();

        for (ScoredResult sr : shortlist) {
            Long tmdbId = longValue(sr.node(), "id");
            if (tmdbId == null || !checkedIds.add(tmdbId)) {
                continue;
            }
            JsonNode detail = readTree(tmdbRestTemplate.getForObject(buildMovieDetailUri(tmdbId), String.class));
            if (detail == null) {
                continue;
            }
            int totalScore = sr.score() + scoreAlternativeTitles(candidate, detail);
            if (totalScore > bestScore) {
                bestScore = totalScore;
                bestMatch = sr.node();
            }
        }

        return bestScore >= MIN_MATCH_SCORE ? bestMatch : null;
    }

    private int scoreAlternativeTitles(ExistingMovieCandidate candidate, JsonNode detail) {
        String displayTitle = normalizeTitle(candidate.displayTitle());
        if (displayTitle == null) {
            return 0;
        }

        JsonNode altTitles = detail.path("alternative_titles").path("titles");
        if (altTitles.isArray()) {
            for (JsonNode entry : altTitles) {
                if (!"KR".equals(textValue(entry, "iso_3166_1"))) {
                    continue;
                }
                if (displayTitle.equals(normalizeTitle(textValue(entry, "title")))) {
                    return 45;
                }
            }
        }

        JsonNode translations = detail.path("translations").path("translations");
        if (translations.isArray()) {
            for (JsonNode entry : translations) {
                if (!"ko".equals(textValue(entry, "iso_639_1"))) {
                    continue;
                }
                JsonNode dataNode = entry.get("data");
                if (dataNode == null || dataNode.isNull()) {
                    continue;
                }
                if (displayTitle.equals(normalizeTitle(textValue(dataNode, "title")))) {
                    return 40;
                }
            }
        }

        return 0;
    }

    private List<JsonNode> searchMovieResults(String query) {
        if (query == null || query.isBlank()) {
            return List.of();
        }

        JsonNode root = readTree(tmdbRestTemplate.getForObject(buildSearchMovieUri(query), String.class));
        if (root == null || !root.path("results").isArray()) {
            return List.of();
        }

        List<JsonNode> results = new ArrayList<>();
        for (JsonNode result : root.path("results")) {
            results.add(result);
        }
        return results;
    }

    private JsonNode chooseBestMatch(ExistingMovieCandidate candidate, List<JsonNode> results) {
        JsonNode bestMatch = null;
        int bestScore = Integer.MIN_VALUE;

        for (JsonNode result : results) {
            int score = scoreSearchResult(candidate, result);
            if (score > bestScore) {
                bestScore = score;
                bestMatch = result;
            }
        }

        return bestScore >= MIN_MATCH_SCORE ? bestMatch : null;
    }

    private int scoreSearchResult(ExistingMovieCandidate candidate, JsonNode result) {
        int score = 0;
        String displayTitle = normalizeTitle(candidate.displayTitle());
        String originalTitle = normalizeTitle(candidate.originalTitle());
        String resultTitle = normalizeTitle(textValue(result, "title"));
        String resultOriginalTitle = normalizeTitle(textValue(result, "original_title"));
        LocalDate resultReleaseDate = localDateValue(result, "release_date");

        if (displayTitle != null && (displayTitle.equals(resultTitle) || displayTitle.equals(resultOriginalTitle))) {
            score += 100;
        }
        if (originalTitle != null && (originalTitle.equals(resultTitle) || originalTitle.equals(resultOriginalTitle))) {
            score += 90;
        }
        if (displayTitle != null && resultTitle != null && resultTitle.contains(displayTitle)) {
            score += 30;
        }
        if (originalTitle != null && resultOriginalTitle != null && resultOriginalTitle.contains(originalTitle)) {
            score += 20;
        }
        if (candidate.releaseDate() != null && resultReleaseDate != null) {
            if (candidate.releaseDate().equals(resultReleaseDate)) {
                score += 40;
            } else if (candidate.releaseDate().getYear() == resultReleaseDate.getYear()) {
                score += 20;
            }
        }

        return score;
    }

    private Set<Long> loadLinkedTmdbIds() {
        List<String> keys = jdbcTemplate.queryForList(
                "SELECT source_key FROM movie_source WHERE source_type = 'TMDB'",
                String.class);
        Set<Long> ids = new HashSet<>();
        for (String key : keys) {
            try { ids.add(Long.parseLong(key)); }
            catch (NumberFormatException ignored) {}
        }
        return ids;
    }

    private URI buildPopularMovieUri(int page) {
        return UriComponentsBuilder.fromUriString(baseUrl)
                .path("/movie/popular")
                .queryParam("language", "ko-KR")
                .queryParam("region", "KR")
                .queryParam("page", page)
                .build()
                .toUri();
    }

    private URI buildTopRatedUri(int page) {
        return UriComponentsBuilder.fromUriString(baseUrl)
                .path("/movie/top_rated")
                .queryParam("language", "ko-KR")
                .queryParam("region", "KR")
                .queryParam("page", page)
                .build()
                .toUri();
    }

    /**
     * source:
     *   korean-ott    — 넷플릭스·왓챠·웨이브·티빙 한국 제공 영화
     *   korean-movies — 한국어(ko) 원작 영화
     *   high-rated    — vote_average ≥ 7.5 & vote_count ≥ 500 (다큐·TV영화 제외)
     */
    private URI buildDiscoverUri(String source, int page) {
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(baseUrl)
                .path("/discover/movie")
                .queryParam("language", "ko-KR")
                .queryParam("page", page)
                .queryParam("include_adult", "false");
        switch (source) {
            case "korean-ott" -> builder
                    .queryParam("watch_region", "KR")
                    .queryParam("with_watch_providers", "8,97,356,229")  // Netflix,Watcha,Wavve,Tving
                    .queryParam("sort_by", "popularity.desc");
            case "korean-movies" -> builder
                    .queryParam("with_original_language", "ko")
                    .queryParam("sort_by", "popularity.desc");
            case "high-rated" -> builder
                    .queryParam("vote_average.gte", "7.5")
                    .queryParam("vote_count.gte", "500")
                    .queryParam("sort_by", "vote_count.desc")
                    .queryParam("without_genres", "99,10770");  // Documentary, TV Movie
            default -> throw new IllegalArgumentException("Unknown discover source: " + source);
        }
        return builder.build().toUri();
    }

    private URI buildSearchMovieUri(String query) {
        return UriComponentsBuilder.fromUriString(baseUrl)
                .path("/search/movie")
                .queryParam("language", "ko-KR")
                .queryParam("region", "KR")
                .queryParam("include_adult", "false")
                .queryParam("query", query)
                .build()
                .toUri();
    }

    private URI buildMovieDetailUri(Long tmdbMovieId) {
        return UriComponentsBuilder.fromUriString(baseUrl)
                .path("/movie/{tmdbMovieId}")
                .queryParam("language", "ko-KR")
                .queryParam("append_to_response", "credits,keywords,videos,release_dates,watch/providers,external_ids,images,translations,alternative_titles")
                .build(tmdbMovieId);
    }

    private String normalizeTitle(String title) {
        if (title == null || title.isBlank()) {
            return null;
        }

        return title.toLowerCase()
                .replace(" ", "")
                .replace(":", "")
                .replace("-", "")
                .replace("'", "")
                .replace("’", "");
    }

    private JsonNode readTree(String response) {
        try {
            return response == null ? null : objectMapper.readTree(response);
        } catch (JacksonException e) {
            throw new IllegalStateException("TMDB response JSON parsing failed.", e);
        }
    }

    private String toJson(JsonNode node) {
        try {
            return objectMapper.writeValueAsString(node);
        } catch (JacksonException e) {
            throw new IllegalStateException("TMDB JSON serialization failed.", e);
        }
    }

    private String textValue(JsonNode node, String fieldName) {
        JsonNode value = node.get(fieldName);
        if (value == null || value.isNull()) {
            return null;
        }
        String text = value.asText();
        return text.isBlank() ? null : text;
    }

    private Long longValue(JsonNode node, String fieldName) {
        JsonNode value = node.get(fieldName);
        return value == null || value.isNull() ? null : value.asLong();
    }

    private LocalDate localDateValue(JsonNode node, String fieldName) {
        String text = textValue(node, fieldName);
        return text == null ? null : LocalDate.parse(text);
    }

    public record ImportResult(
            int requestedCount,
            int savedRawCount,
            int updatedRawCount
    ) {
    }

    public record ExistingImportResult(
            int processedCount,
            int matchedCount,
            int noMatchCount,
            int savedRawCount,
            int updatedRawCount
    ) {
    }

    private record ExistingMovieCandidate(
            Long movieId,
            String displayTitle,
            String originalTitle,
            LocalDate releaseDate
    ) {
    }

    private record ScoredResult(JsonNode node, int score) {
    }
}
