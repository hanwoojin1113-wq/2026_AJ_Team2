package com.cinematch.tmdb;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@Service
public class TmdbMovieNormalizeService {
    /*
     * TMDB 정규화는 KOBIS가 만든 영화 골격에 "추천용 상세 메타데이터"를 보강하는 단계다.
     * 포스터, 줄거리, 평점, popularity, 키워드, OTT 제공처 같은 정보는
     * 추천 품질과 UI 완성도에 직접 영향을 주기 때문에 TMDB를 통해 덧입힌다.
     */

    private static final String SOURCE_TYPE_TMDB = "TMDB";
    private static final String TMDB_CODE_PREFIX = "TMDB-";
    private static final String TMDB_IMAGE_BASE_URL = "https://image.tmdb.org/t/p/w500";

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public TmdbMovieNormalizeService(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public NormalizeResult normalizeRawMovies() {
        // TMDB는 provider/keyword를 함께 가져오므로 관련 테이블을 먼저 보장한다.
        initializeProviderTables();
        initializeKeywordTables();

        List<RawMovieRow> rawMovies = loadRawMovies("""
                SELECT tmdb_movie_id, payload
                FROM tmdb_movie_raw
                ORDER BY tmdb_movie_id
                """);

        return normalizeRawMovieRows(rawMovies);
    }

    @Transactional
    public NormalizeResult normalizeMovieIds(List<Long> tmdbMovieIds) {
        initializeProviderTables();
        initializeKeywordTables();

        if (tmdbMovieIds == null || tmdbMovieIds.isEmpty()) {
            return new NormalizeResult(0, 0, 0, 0, 0);
        }

        List<Long> normalizedIds = tmdbMovieIds.stream()
                .filter(id -> id != null && id > 0)
                .distinct()
                .toList();
        if (normalizedIds.isEmpty()) {
            return new NormalizeResult(0, 0, 0, 0, 0);
        }

        String placeholders = String.join(", ", normalizedIds.stream().map(id -> "?").toList());
        List<RawMovieRow> rawMovies = loadRawMovies("""
                SELECT tmdb_movie_id, payload
                FROM tmdb_movie_raw
                WHERE tmdb_movie_id IN (%s)
                ORDER BY tmdb_movie_id
                """.formatted(placeholders), normalizedIds.toArray());

        return normalizeRawMovieRows(rawMovies);
    }

    private List<RawMovieRow> loadRawMovies(String sql, Object... params) {
        return jdbcTemplate.query(sql, (rs, rowNum) -> new RawMovieRow(
                rs.getLong("tmdb_movie_id"),
                rs.getString("payload")
        ), params);
    }

    private NormalizeResult normalizeRawMovieRows(List<RawMovieRow> rawMovies) {

        int processedCount = 0;
        int insertedMovieCount = 0;
        int updatedMovieCount = 0;
        int matchedExistingMovieCount = 0;
        int insertedSourceCount = 0;

        for (RawMovieRow rawMovie : rawMovies) {
            JsonNode root = readTree(rawMovie.payload());
            if (root == null) {
                continue;
            }

            processedCount++;
            Long movieId = findMovieIdBySource(rawMovie.tmdbMovieId());
            boolean matchedExistingMovie = false;

            // 1차: 이미 TMDB source가 연결된 영화 재사용
            if (movieId == null) {
                movieId = findMovieIdByTitle(
                        textValue(root, "title"),
                        textValue(root, "original_title"),
                        "KOBIS",
                        SOURCE_TYPE_TMDB
                );
                matchedExistingMovie = movieId != null;
            }

            // 2차: 제목만으로 모호하면 개봉일을 같이 써서 기존 KOBIS 영화와 매칭
            if (movieId == null) {
                movieId = findMovieIdByTitleAndReleaseDate(
                        textValue(root, "title"),
                        textValue(root, "original_title"),
                        localDateValue(root, "release_date")
                );
                matchedExistingMovie = movieId != null;
            }

            if (movieId == null) {
                movieId = insertMovie(rawMovie.tmdbMovieId(), root);
                insertedMovieCount++;
            } else {
                updateMovie(movieId, root);
                updatedMovieCount++;
                if (matchedExistingMovie) {
                matchedExistingMovieCount++;
                }
            }

            // 출처 연결을 남겨두면 이후 재수집 때 같은 TMDB 영화를 안정적으로 추적할 수 있다.
            if (upsertMovieSource(movieId, rawMovie.tmdbMovieId())) {
                insertedSourceCount++;
            }

            // TMDB는 추천과 UI에 필요한 상세 feature를 정규화하는 역할이 크다.
            normalizeGenres(movieId, root.path("genres"));
            normalizeActors(movieId, root.path("credits").path("cast"));
            normalizeDirectors(movieId, root.path("credits").path("crew"));
            normalizeCompanies(movieId, root.path("production_companies"));
            normalizeProviders(movieId, root.path("watch/providers").path("results").path("KR"));
            normalizeKeywords(movieId, root.path("keywords"));
        }

        return new NormalizeResult(
                processedCount,
                insertedMovieCount,
                updatedMovieCount,
                matchedExistingMovieCount,
                insertedSourceCount
        );
    }

    private Long findMovieIdBySource(Long tmdbMovieId) {
        return jdbcTemplate.query("""
                SELECT movie_id
                FROM movie_source
                WHERE source_type = ? AND source_key = ?
                """, rs -> rs.next() ? rs.getLong("movie_id") : null, SOURCE_TYPE_TMDB, String.valueOf(tmdbMovieId));
    }

    private Long findMovieIdByTitleAndReleaseDate(String title, String originalTitle, LocalDate releaseDate) {
        if (title == null || releaseDate == null) {
            return null;
        }

        List<Object> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder("""
                SELECT id
                FROM movie
                WHERE movie_info_open_date = ?
                  AND id NOT IN (
                        SELECT movie_id
                        FROM movie_source
                        WHERE source_type = ?
                  )
                  AND (
                        movie_name = ?
                """);
        params.add(Date.valueOf(releaseDate));
        params.add(SOURCE_TYPE_TMDB);
        params.add(title);

        if (originalTitle != null) {
            sql.append(" OR movie_name_original = ?");
            params.add(originalTitle);
        }

        sql.append("""
                  )
                ORDER BY id
                LIMIT 1
                """);

        return jdbcTemplate.query(sql.toString(), rs -> rs.next() ? rs.getLong("id") : null, params.toArray());
    }

    private Long findMovieIdByTitle(String title, String originalTitle, String preferredSourceType, String currentSourceType) {
        if ((title == null || title.isBlank()) && (originalTitle == null || originalTitle.isBlank())) {
            return null;
        }

        return jdbcTemplate.query("""
                SELECT m.id
                FROM movie m
                LEFT JOIN movie_source preferred_ms
                    ON preferred_ms.movie_id = m.id
                   AND preferred_ms.source_type = ?
                LEFT JOIN movie_source current_ms
                    ON current_ms.movie_id = m.id
                   AND current_ms.source_type = ?
                WHERE current_ms.id IS NULL
                  AND (
                        (? IS NOT NULL AND (m.movie_name = ? OR m.title = ?))
                     OR (? IS NOT NULL AND (m.movie_name_original = ? OR m.original_title = ?))
                  )
                ORDER BY
                    CASE WHEN preferred_ms.id IS NOT NULL THEN 0 ELSE 1 END,
                    CASE
                        WHEN ? IS NOT NULL AND (m.movie_name = ? OR m.title = ?) THEN 0
                        WHEN ? IS NOT NULL AND (m.movie_name_original = ? OR m.original_title = ?) THEN 1
                        ELSE 2
                    END,
                    m.id
                LIMIT 1
                """, rs -> rs.next() ? rs.getLong("id") : null,
                preferredSourceType,
                currentSourceType,
                title, title, title,
                originalTitle, originalTitle, originalTitle,
                title, title, title,
                originalTitle, originalTitle, originalTitle
        );
    }

    private Long insertMovie(Long tmdbMovieId, JsonNode root) {
        // KOBIS 매칭에 실패한 TMDB 영화도 독립 영화로 저장할 수 있게 기본 movie row를 생성한다.
        String title = coalesce(textValue(root, "title"), textValue(root, "original_title"), TMDB_CODE_PREFIX + tmdbMovieId);
        String originalTitle = textValue(root, "original_title");
        LocalDate releaseDate = localDateValue(root, "release_date");
        Integer runtime = intValue(root, "runtime");
        String posterImageUrl = buildImageUrl(textValue(root, "poster_path"));
        LocalDateTime now = LocalDateTime.now();

        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(connection -> {
            PreparedStatement statement = connection.prepareStatement("""
                    INSERT INTO movie (
                        movie_cd,
                        movie_name,
                        movie_name_en,
                        movie_name_original,
                        movie_info_open_date,
                        production_year,
                        show_time,
                        poster_image_url,
                        title,
                        original_title,
                        overview,
                        release_date,
                        runtime,
                        budget,
                        revenue,
                        tmdb_status,
                        tagline,
                        poster_path,
                        backdrop_path,
                        vote_average,
                        vote_count,
                        popularity,
                        adult,
                        original_language,
                        homepage,
                        imdb_id,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, Statement.RETURN_GENERATED_KEYS);
            statement.setString(1, TMDB_CODE_PREFIX + tmdbMovieId);
            statement.setString(2, title);
            statement.setString(3, originalTitle);
            statement.setString(4, originalTitle);
            statement.setDate(5, toSqlDate(releaseDate));
            statement.setObject(6, releaseDate != null ? releaseDate.getYear() : null);
            statement.setObject(7, runtime);
            statement.setString(8, posterImageUrl);
            statement.setString(9, title);
            statement.setString(10, originalTitle);
            statement.setString(11, textValue(root, "overview"));
            statement.setDate(12, toSqlDate(releaseDate));
            statement.setObject(13, runtime);
            statement.setObject(14, longValue(root, "budget"));
            statement.setObject(15, longValue(root, "revenue"));
            statement.setString(16, textValue(root, "status"));
            statement.setString(17, textValue(root, "tagline"));
            statement.setString(18, textValue(root, "poster_path"));
            statement.setString(19, textValue(root, "backdrop_path"));
            statement.setObject(20, doubleValue(root, "vote_average"));
            statement.setObject(21, intValue(root, "vote_count"));
            statement.setObject(22, doubleValue(root, "popularity"));
            statement.setObject(23, booleanValue(root, "adult"));
            statement.setString(24, textValue(root, "original_language"));
            statement.setString(25, textValue(root, "homepage"));
            statement.setString(26, textValue(root, "imdb_id"));
            statement.setTimestamp(27, Timestamp.valueOf(now));
            statement.setTimestamp(28, Timestamp.valueOf(now));
            return statement;
        }, keyHolder);

        return keyHolder.getKeyAs(Long.class);
    }

    private void updateMovie(Long movieId, JsonNode root) {
        // TMDB가 가진 포스터/줄거리/인기도/언어/홈페이지 등 "보강 정보"를 movie 본체에 채운다.
        String title = textValue(root, "title");
        String originalTitle = textValue(root, "original_title");
        String overview = textValue(root, "overview");
        LocalDate releaseDate = localDateValue(root, "release_date");
        Integer runtime = intValue(root, "runtime");
        Integer productionYear = releaseDate != null ? releaseDate.getYear() : null;
        String posterImageUrl = buildImageUrl(textValue(root, "poster_path"));
        Long budget = longValue(root, "budget");
        Long revenue = longValue(root, "revenue");
        String status = textValue(root, "status");
        String tagline = textValue(root, "tagline");
        String posterPath = textValue(root, "poster_path");
        String backdropPath = textValue(root, "backdrop_path");
        Double voteAverage = doubleValue(root, "vote_average");
        Integer voteCount = intValue(root, "vote_count");
        Double popularity = doubleValue(root, "popularity");
        Boolean adult = booleanValue(root, "adult");
        String originalLanguage = textValue(root, "original_language");
        String homepage = textValue(root, "homepage");
        String imdbId = textValue(root, "imdb_id");

        jdbcTemplate.update("""
                UPDATE movie
                SET movie_name = COALESCE(?, movie_name),
                    movie_name_en = COALESCE(?, movie_name_en),
                    movie_name_original = COALESCE(?, movie_name_original),
                    movie_info_open_date = COALESCE(?, movie_info_open_date),
                    production_year = COALESCE(?, production_year),
                    show_time = COALESCE(?, show_time),
                    poster_image_url = COALESCE(?, poster_image_url),
                    title = COALESCE(?, title),
                    original_title = COALESCE(?, original_title),
                    overview = COALESCE(?, overview),
                    release_date = COALESCE(?, release_date),
                    runtime = COALESCE(?, runtime),
                    budget = COALESCE(?, budget),
                    revenue = COALESCE(?, revenue),
                    tmdb_status = COALESCE(?, tmdb_status),
                    tagline = COALESCE(?, tagline),
                    poster_path = COALESCE(?, poster_path),
                    backdrop_path = COALESCE(?, backdrop_path),
                    vote_average = COALESCE(?, vote_average),
                    vote_count = COALESCE(?, vote_count),
                    popularity = COALESCE(?, popularity),
                    adult = COALESCE(?, adult),
                    original_language = COALESCE(?, original_language),
                    homepage = COALESCE(?, homepage),
                    imdb_id = COALESCE(?, imdb_id),
                    updated_at = ?
                WHERE id = ?
                """,
                title,
                originalTitle,
                originalTitle,
                toSqlDate(releaseDate),
                productionYear,
                runtime,
                posterImageUrl,
                title,
                originalTitle,
                overview,
                toSqlDate(releaseDate),
                runtime,
                budget,
                revenue,
                status,
                tagline,
                posterPath,
                backdropPath,
                voteAverage,
                voteCount,
                popularity,
                adult,
                originalLanguage,
                homepage,
                imdbId,
                Timestamp.valueOf(LocalDateTime.now()),
                movieId
        );
    }

    private boolean upsertMovieSource(Long movieId, Long tmdbMovieId) {
        Integer existingCount = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM movie_source
                WHERE source_type = ? AND source_key = ?
                """, Integer.class, SOURCE_TYPE_TMDB, String.valueOf(tmdbMovieId));

        if (existingCount != null && existingCount > 0) {
            jdbcTemplate.update("""
                    UPDATE movie_source
                    SET movie_id = ?, updated_at = ?
                    WHERE source_type = ? AND source_key = ?
                    """, movieId, Timestamp.valueOf(LocalDateTime.now()), SOURCE_TYPE_TMDB, String.valueOf(tmdbMovieId));
            return false;
        }

        LocalDateTime now = LocalDateTime.now();
        jdbcTemplate.update("""
                INSERT INTO movie_source (movie_id, source_type, source_key, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """, movieId, SOURCE_TYPE_TMDB, String.valueOf(tmdbMovieId), Timestamp.valueOf(now), Timestamp.valueOf(now));
        return true;
    }

    private void normalizeGenres(Long movieId, JsonNode genresNode) {
        if (!genresNode.isArray()) {
            return;
        }

        for (String genreName : collectDistinctNames(genresNode, "name")) {
            Long genreId = resolveLookupId("genre", canonicalGenreName(genreName));
            if (genreId != null && !existsLink("movie_genre", "movie_id", movieId, "genre_id", genreId)) {
                jdbcTemplate.update("""
                        INSERT INTO movie_genre (movie_id, genre_id, display_order)
                        VALUES (?, ?, ?)
                        """, movieId, genreId, nextDisplayOrder("movie_genre", movieId));
            }
        }
    }

    private void normalizeActors(Long movieId, JsonNode castNode) {
        if (!castNode.isArray()) {
            return;
        }

        for (String actorName : collectDistinctNames(castNode, "name")) {
            Long personId = resolveLookupId("person", actorName);
            if (personId != null && !existsLink("movie_actor", "movie_id", movieId, "person_id", personId)) {
                jdbcTemplate.update("""
                        INSERT INTO movie_actor (movie_id, person_id, display_order)
                        VALUES (?, ?, ?)
                        """, movieId, personId, nextDisplayOrder("movie_actor", movieId));
            }
        }
    }

    private void normalizeDirectors(Long movieId, JsonNode crewNode) {
        if (!crewNode.isArray()) {
            return;
        }

        Set<String> directors = new LinkedHashSet<>();
        for (JsonNode crew : crewNode) {
            if (!"Director".equals(textValue(crew, "job"))) {
                continue;
            }
            String name = textValue(crew, "name");
            if (name != null) {
                directors.add(name);
            }
        }

        for (String directorName : directors) {
            Long personId = resolveLookupId("person", directorName);
            if (personId != null && !existsLink("movie_director", "movie_id", movieId, "person_id", personId)) {
                jdbcTemplate.update("""
                        INSERT INTO movie_director (movie_id, person_id, display_order)
                        VALUES (?, ?, ?)
                        """, movieId, personId, nextDisplayOrder("movie_director", movieId));
            }
        }
    }

    private void normalizeCompanies(Long movieId, JsonNode companiesNode) {
        if (!companiesNode.isArray()) {
            return;
        }

        for (String companyName : collectDistinctNames(companiesNode, "name")) {
            Long companyId = resolveLookupId("company", companyName);
            if (companyId != null && !existsLink("movie_company", "movie_id", movieId, "company_id", companyId)) {
                jdbcTemplate.update("""
                        INSERT INTO movie_company (movie_id, company_id, company_role, display_order)
                        VALUES (?, ?, ?, ?)
                        """, movieId, companyId, "production", nextDisplayOrder("movie_company", movieId));
            }
        }
    }

    private void normalizeProviders(Long movieId, JsonNode krProvidersNode) {
        // 제공처는 국가/유형(정액제/대여/구매)을 함께 저장해 추천과 UI에서 선택적으로 쓸 수 있게 한다.
        if (krProvidersNode == null || krProvidersNode.isMissingNode() || krProvidersNode.isNull()) {
            return;
        }

        normalizeProviderType(movieId, krProvidersNode.path("flatrate"), "FLATRATE", "KR");
        normalizeProviderType(movieId, krProvidersNode.path("rent"), "RENT", "KR");
        normalizeProviderType(movieId, krProvidersNode.path("buy"), "BUY", "KR");
    }

    private void normalizeProviderType(Long movieId, JsonNode providersNode, String providerType, String regionCode) {
        if (!providersNode.isArray()) {
            return;
        }

        int displayOrder = 1;
        for (JsonNode providerNode : providersNode) {
            Long providerId = resolveProviderId(providerNode);
            if (providerId == null) {
                continue;
            }

            Integer existingCount = jdbcTemplate.queryForObject("""
                    SELECT COUNT(*)
                    FROM movie_provider
                    WHERE movie_id = ? AND provider_id = ? AND provider_type = ? AND region_code = ?
                    """, Integer.class, movieId, providerId, providerType, regionCode);
            if (existingCount != null && existingCount > 0) {
                displayOrder++;
                continue;
            }

            jdbcTemplate.update("""
                    INSERT INTO movie_provider (movie_id, provider_id, provider_type, region_code, display_order)
                    VALUES (?, ?, ?, ?, ?)
                    """, movieId, providerId, providerType, regionCode, displayOrder);
            displayOrder++;
        }
    }

    private void normalizeKeywords(Long movieId, JsonNode keywordsNode) {
        // 키워드는 태그 생성과 추천 품질 튜닝에 직접 쓰이므로 최신 상태로 재구성한다.
        JsonNode keywordArray = extractKeywordArray(keywordsNode);
        jdbcTemplate.update("""
                DELETE FROM movie_keyword
                WHERE movie_id = ?
                """, movieId);

        if (!keywordArray.isArray()) {
            return;
        }

        int displayOrder = 1;
        Set<Long> insertedKeywordIds = new LinkedHashSet<>();
        for (JsonNode keywordNode : keywordArray) {
            Long keywordId = resolveKeywordId(keywordNode);
            if (keywordId == null || !insertedKeywordIds.add(keywordId)) {
                continue;
            }

            jdbcTemplate.update("""
                    INSERT INTO movie_keyword (movie_id, keyword_id, display_order)
                    VALUES (?, ?, ?)
                    """, movieId, keywordId, displayOrder);
            displayOrder++;
        }
    }

    private Long resolveLookupId(String tableName, String value) {
        if (value == null || value.isBlank()) {
            return null;
        }

        Long existingId = jdbcTemplate.query("""
                SELECT id
                FROM %s
                WHERE name = ?
                """.formatted(tableName), rs -> rs.next() ? rs.getLong("id") : null, value);
        if (existingId != null) {
            return existingId;
        }

        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(connection -> {
            PreparedStatement statement = connection.prepareStatement("""
                    INSERT INTO %s (name)
                    VALUES (?)
                    """.formatted(tableName), Statement.RETURN_GENERATED_KEYS);
            statement.setString(1, value);
            return statement;
        }, keyHolder);
        return keyHolder.getKeyAs(Long.class);
    }

    private Long resolveKeywordId(JsonNode keywordNode) {
        // TMDB keyword id가 있으면 그걸 우선 기준으로 쓰고, 없으면 name 기반으로 병합한다.
        String keywordName = textValue(keywordNode, "name");
        Long tmdbKeywordId = longValue(keywordNode, "id");
        if (keywordName == null) {
            return null;
        }

        if (tmdbKeywordId != null) {
            Long existingByTmdbId = jdbcTemplate.query("""
                    SELECT id
                    FROM keyword
                    WHERE tmdb_keyword_id = ?
                    """, rs -> rs.next() ? rs.getLong("id") : null, tmdbKeywordId);
            if (existingByTmdbId != null) {
                jdbcTemplate.update("""
                        UPDATE keyword
                        SET name = ?
                        WHERE id = ?
                        """, keywordName, existingByTmdbId);
                return existingByTmdbId;
            }
        }

        Long existingByName = jdbcTemplate.query("""
                SELECT id
                FROM keyword
                WHERE name = ?
                """, rs -> rs.next() ? rs.getLong("id") : null, keywordName);
        if (existingByName != null) {
            if (tmdbKeywordId != null) {
                jdbcTemplate.update("""
                        UPDATE keyword
                        SET tmdb_keyword_id = COALESCE(tmdb_keyword_id, ?)
                        WHERE id = ?
                        """, tmdbKeywordId, existingByName);
            }
            return existingByName;
        }

        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(connection -> {
            PreparedStatement statement = connection.prepareStatement("""
                    INSERT INTO keyword (tmdb_keyword_id, name)
                    VALUES (?, ?)
                    """, Statement.RETURN_GENERATED_KEYS);
            statement.setObject(1, tmdbKeywordId);
            statement.setString(2, keywordName);
            return statement;
        }, keyHolder);
        return keyHolder.getKeyAs(Long.class);
    }

    private JsonNode extractKeywordArray(JsonNode keywordsNode) {
        if (keywordsNode == null || keywordsNode.isMissingNode() || keywordsNode.isNull()) {
            return null;
        }
        if (keywordsNode.isArray()) {
            return keywordsNode;
        }
        JsonNode nestedKeywords = keywordsNode.path("keywords");
        if (nestedKeywords.isArray()) {
            return nestedKeywords;
        }
        JsonNode nestedResults = keywordsNode.path("results");
        return nestedResults.isArray() ? nestedResults : null;
    }

    private Long resolveProviderId(JsonNode providerNode) {
        // provider도 TMDB provider id를 기준 키로 두어 로고/우선순위를 안정적으로 유지한다.
        Long tmdbProviderId = longValue(providerNode, "provider_id");
        String providerName = textValue(providerNode, "provider_name");
        if (tmdbProviderId == null || providerName == null) {
            return null;
        }

        Long existingId = jdbcTemplate.query("""
                SELECT id
                FROM provider
                WHERE tmdb_provider_id = ?
                """, rs -> rs.next() ? rs.getLong("id") : null, tmdbProviderId);
        if (existingId != null) {
            jdbcTemplate.update("""
                    UPDATE provider
                    SET provider_name = ?, logo_path = ?, display_priority = ?
                    WHERE id = ?
                    """,
                    providerName,
                    textValue(providerNode, "logo_path"),
                    intValue(providerNode, "display_priority"),
                    existingId
            );
            return existingId;
        }

        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(connection -> {
            PreparedStatement statement = connection.prepareStatement("""
                    INSERT INTO provider (tmdb_provider_id, provider_name, logo_path, display_priority)
                    VALUES (?, ?, ?, ?)
                    """, Statement.RETURN_GENERATED_KEYS);
            statement.setObject(1, tmdbProviderId);
            statement.setString(2, providerName);
            statement.setString(3, textValue(providerNode, "logo_path"));
            statement.setObject(4, intValue(providerNode, "display_priority"));
            return statement;
        }, keyHolder);
        return keyHolder.getKeyAs(Long.class);
    }

    private String canonicalGenreName(String genreName) {
        if (genreName == null || genreName.isBlank()) {
            return genreName;
        }

        return switch (genreName.trim()) {
            case "멜로/로맨스" -> "로맨스";
            case "공포(호러)" -> "공포";
            case "어드벤처" -> "모험";
            default -> genreName.trim();
        };
    }

    private boolean existsLink(String tableName, String leftColumnName, Long leftId, String rightColumnName, Long rightId) {
        Integer count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM %s
                WHERE %s = ? AND %s = ?
                """.formatted(tableName, leftColumnName, rightColumnName), Integer.class, leftId, rightId);
        return count != null && count > 0;
    }

    private int nextDisplayOrder(String tableName, Long movieId) {
        Integer maxDisplayOrder = jdbcTemplate.queryForObject("""
                SELECT COALESCE(MAX(display_order), 0)
                FROM %s
                WHERE movie_id = ?
                """.formatted(tableName), Integer.class, movieId);
        return (maxDisplayOrder == null ? 0 : maxDisplayOrder) + 1;
    }

    private List<String> collectDistinctNames(JsonNode arrayNode, String fieldName) {
        Set<String> values = new LinkedHashSet<>();
        for (JsonNode node : arrayNode) {
            String value = textValue(node, fieldName);
            if (value != null) {
                values.add(value);
            }
        }
        return values.stream().toList();
    }

    private JsonNode readTree(String payload) {
        try {
            return payload == null ? null : objectMapper.readTree(payload);
        } catch (JacksonException e) {
            throw new IllegalStateException("TMDB raw payload JSON parsing failed.", e);
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

    private Integer intValue(JsonNode node, String fieldName) {
        JsonNode value = node.get(fieldName);
        return value == null || value.isNull() ? null : value.asInt();
    }

    private Long longValue(JsonNode node, String fieldName) {
        JsonNode value = node.get(fieldName);
        return value == null || value.isNull() ? null : value.asLong();
    }

    private Double doubleValue(JsonNode node, String fieldName) {
        JsonNode value = node.get(fieldName);
        return value == null || value.isNull() ? null : value.asDouble();
    }

    private Boolean booleanValue(JsonNode node, String fieldName) {
        JsonNode value = node.get(fieldName);
        return value == null || value.isNull() ? null : value.asBoolean();
    }

    private LocalDate localDateValue(JsonNode node, String fieldName) {
        String text = textValue(node, fieldName);
        return text == null ? null : LocalDate.parse(text);
    }

    private Date toSqlDate(LocalDate value) {
        return value == null ? null : Date.valueOf(value);
    }

    private String buildImageUrl(String posterPath) {
        return posterPath == null ? null : TMDB_IMAGE_BASE_URL + posterPath;
    }

    private String coalesce(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    private void initializeProviderTables() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS provider (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    tmdb_provider_id BIGINT NOT NULL UNIQUE,
                    provider_name VARCHAR(255) NOT NULL,
                    logo_path VARCHAR(500),
                    display_priority INT
                )
                """);

        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS movie_provider (
                    movie_id BIGINT NOT NULL,
                    provider_id BIGINT NOT NULL,
                    provider_type VARCHAR(20) NOT NULL,
                    region_code VARCHAR(10) NOT NULL,
                    display_order INT NOT NULL,
                    PRIMARY KEY (movie_id, provider_id, provider_type, region_code),
                    CONSTRAINT fk_movie_provider_movie FOREIGN KEY (movie_id) REFERENCES movie(id),
                    CONSTRAINT fk_movie_provider_provider FOREIGN KEY (provider_id) REFERENCES provider(id)
                )
                """);
    }

    private void initializeKeywordTables() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS keyword (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    tmdb_keyword_id BIGINT,
                    name VARCHAR(255) NOT NULL UNIQUE
                )
                """);

        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS movie_keyword (
                    movie_id BIGINT NOT NULL,
                    keyword_id BIGINT NOT NULL,
                    display_order INT NOT NULL,
                    PRIMARY KEY (movie_id, keyword_id),
                    CONSTRAINT fk_movie_keyword_movie FOREIGN KEY (movie_id) REFERENCES movie(id),
                    CONSTRAINT fk_movie_keyword_keyword FOREIGN KEY (keyword_id) REFERENCES keyword(id)
                )
                """);
    }

    private record RawMovieRow(Long tmdbMovieId, String payload) {
    }

    public record NormalizeResult(
            int processedCount,
            int insertedMovieCount,
            int updatedMovieCount,
            int matchedExistingMovieCount,
            int insertedSourceCount
    ) {
    }
}
