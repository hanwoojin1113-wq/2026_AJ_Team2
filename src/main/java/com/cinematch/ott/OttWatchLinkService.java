package com.cinematch.ott;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

@Service
public class OttWatchLinkService {

    private static final String COUPANG_PLAY_LOGO_URL = "/images/providers/coupang-play.png";
    private static final String LAFTEL_LOGO_URL = "/images/providers/laftel.svg";
    private static final String CGV_LOGO_URL = "/images/providers/cgv.svg";
    private static final String LOTTE_CINEMA_LOGO_URL = "/images/providers/lotte-cinema.svg";
    private static final String MEGABOX_LOGO_URL = "/images/providers/megabox.svg";
    private static final Set<String> TERMINAL_CRAWL_STATUSES = Set.of("SUCCESS", "NO_LINK", "NO_TITLE");

    private final JdbcTemplate jdbcTemplate;
    private volatile boolean tablesInitialized = false;

    public OttWatchLinkService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<WatchLinkView> fetchWatchLinks(Long movieId, Map<String, String> providerLogoUrls) {
        initializeTables();
        return jdbcTemplate.query("""
                SELECT provider_name, watch_url
                FROM movie_ott_link
                WHERE movie_id = ?
                  AND watch_url IS NOT NULL
                  AND watch_url <> ''
                ORDER BY display_order, provider_name
                """, (rs, rowNum) -> {
            String providerName = rs.getString("provider_name");
            return new WatchLinkView(
                    providerName,
                    rs.getString("watch_url"),
                    resolveProviderLogoUrl(providerName, providerLogoUrls)
            );
        }, movieId);
    }

    public boolean hasCrawlStatus(Long movieId) {
        initializeTables();
        Integer count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM movie_ott_crawl_status
                WHERE movie_id = ?
                """, Integer.class, movieId);
        return count != null && count > 0;
    }

    public List<CrawlCandidate> findCrawlCandidates(int limit) {
        initializeTables();
        int safeLimit = Math.max(1, Math.min(limit, 1000));
        return jdbcTemplate.query("""
                SELECT
                    m.id AS movie_id,
                    m.movie_cd,
                    COALESCE(NULLIF(m.title, ''), NULLIF(m.movie_name, '')) AS title,
                    COALESCE(m.release_date, m.movie_info_open_date, m.box_office_open_date) AS release_date,
                    m.poster_image_url,
                    ktm.kinolights_url,
                    CASE
                        WHEN EXISTS (SELECT 1 FROM tmdb_trending_chart tc WHERE tc.movie_id = m.id) THEN 'TMDB_TRENDING'
                        WHEN EXISTS (SELECT 1 FROM user_recommendation_result urr WHERE urr.movie_id = m.id) THEN 'RECOMMENDED'
                        WHEN EXISTS (SELECT 1 FROM social_post sp WHERE sp.movie_id = m.id AND sp.is_deleted = FALSE) THEN 'SOCIAL_POST'
                        WHEN EXISTS (SELECT 1 FROM movie_tag mt WHERE mt.movie_id = m.id) THEN 'TAGGED'
                        ELSE 'CATALOG'
                    END AS priority_reason
                FROM movie m
                LEFT JOIN kinolights_title_mapping ktm ON ktm.movie_id = m.id
                WHERE COALESCE(NULLIF(m.title, ''), NULLIF(m.movie_name, '')) IS NOT NULL
                  AND m.poster_image_url IS NOT NULL
                  AND m.poster_image_url <> ''
                  AND COALESCE(m.release_date, m.movie_info_open_date, m.box_office_open_date) IS NOT NULL
                  AND COALESCE(m.release_date, m.movie_info_open_date, m.box_office_open_date) <= CURRENT_DATE
                  AND NOT EXISTS (
                      SELECT 1
                      FROM movie_ott_crawl_status s
                      WHERE s.movie_id = m.id
                        AND s.status IN ('SUCCESS', 'NO_LINK', 'NO_TITLE')
                  )
                ORDER BY
                    CASE
                        WHEN EXISTS (SELECT 1 FROM tmdb_trending_chart tc WHERE tc.movie_id = m.id) THEN 1
                        WHEN EXISTS (SELECT 1 FROM user_recommendation_result urr WHERE urr.movie_id = m.id) THEN 2
                        WHEN EXISTS (SELECT 1 FROM social_post sp WHERE sp.movie_id = m.id AND sp.is_deleted = FALSE) THEN 3
                        WHEN EXISTS (SELECT 1 FROM movie_tag mt WHERE mt.movie_id = m.id) THEN 4
                        ELSE 5
                    END,
                    COALESCE(m.vote_count, 0) DESC,
                    COALESCE(m.box_office_audi_acc, 0) DESC,
                    COALESCE(m.popularity, 0) DESC,
                    m.id DESC
                LIMIT ?
                """, (rs, rowNum) -> new CrawlCandidate(
                rs.getLong("movie_id"),
                rs.getString("movie_cd"),
                rs.getString("title"),
                rs.getObject("release_date", LocalDate.class),
                rs.getString("poster_image_url"),
                rs.getString("kinolights_url"),
                rs.getString("priority_reason")
        ), safeLimit);
    }

    public ImportResult importCrawlerRows(List<CrawlerRow> rows) {
        initializeTables();
        Map<String, List<CrawlerRow>> grouped = new LinkedHashMap<>();
        for (CrawlerRow row : rows) {
            String key = row.movieId() != null ? "id:" + row.movieId() : "title:" + normalizeTitle(row.title());
            grouped.computeIfAbsent(key, ignored -> new ArrayList<>()).add(row);
        }

        int moviesProcessed = 0;
        int moviesImported = 0;
        int moviesNoLink = 0;
        int moviesNoTitle = 0;
        int moviesFailed = 0;
        int linksImported = 0;
        List<String> skipped = new ArrayList<>();

        for (List<CrawlerRow> group : grouped.values()) {
            CrawlerRow first = group.getFirst();
            // CSV의 movie_id는 크롤링 당시(다른 DB)의 id라 신뢰하지 않는다.
            // 두 DB에서 동일하게 유지되는 movie_cd(KOBIS 코드 또는 TMDB-{id})로 먼저 매칭하고,
            // 없으면 제목으로 현재 DB에서 다시 찾는다. (DB 교체 후 재적재 시 오적재 방지)
            Long movieId = findMovieIdByCode(first.movieCode());
            if (movieId == null) {
                movieId = findMovieIdByTitle(first.title());
            }
            if (movieId == null) {
                skipped.add(first.title());
                continue;
            }

            moviesProcessed++;
            String kinolightsUrl = firstNonBlank(group, CrawlerRow::kinolightsUrl);
            String error = firstNonBlank(group, CrawlerRow::error);
            boolean hasNoTitleStatus = group.stream().anyMatch(row -> "NO_TITLE".equalsIgnoreCase(row.status()));
            boolean hasNoLinkStatus = group.stream().anyMatch(row -> "NO_LINK".equalsIgnoreCase(row.status()));
            boolean hasFailedStatus = group.stream().anyMatch(row -> "FAILED".equalsIgnoreCase(row.status()));
            List<CrawlerRow> directRows = group.stream()
                    .filter(row -> hasText(row.provider()) && hasText(row.watchUrl()) && row.isExternalDirect())
                    .toList();

            String status;
            if (!directRows.isEmpty()) {
                status = "SUCCESS";
                Map<String, CrawlerRow> firstByProvider = firstLinkByProvider(directRows);
                jdbcTemplate.update("DELETE FROM movie_ott_link WHERE movie_id = ? AND source = 'KINOLIGHTS_CACHE'", movieId);
                int displayOrder = 0;
                for (CrawlerRow row : firstByProvider.values()) {
                    jdbcTemplate.update("""
                            MERGE INTO movie_ott_link
                                (movie_id, provider_name, watch_url, source, display_order,
                                 raw_url, raw_text, source_method, is_external_direct, crawled_at)
                            KEY (movie_id, provider_name)
                            VALUES (?, ?, ?, 'KINOLIGHTS_CACHE', ?, ?, ?, ?, ?, ?)
                            """,
                            movieId,
                            row.provider(),
                            row.watchUrl(),
                            displayOrder++,
                            row.rawUrl(),
                            row.rawText(),
                            row.sourceMethod(),
                            row.isExternalDirect(),
                            Timestamp.from(row.crawledAt() != null ? row.crawledAt() : Instant.now()));
                    linksImported++;
                }
                moviesImported++;
            } else if (hasNoTitleStatus || !hasText(kinolightsUrl)) {
                status = "NO_TITLE";
                moviesNoTitle++;
            } else if (hasNoLinkStatus) {
                status = "NO_LINK";
                moviesNoLink++;
            } else if (hasFailedStatus || hasText(error)) {
                status = "FAILED";
                moviesFailed++;
            } else {
                status = "NO_LINK";
                moviesNoLink++;
            }

            upsertMapping(movieId, kinolightsUrl, status, error);
            upsertCrawlStatus(movieId, kinolightsUrl, status, directRows, error);
        }

        return new ImportResult(
                moviesProcessed,
                moviesImported,
                moviesNoLink,
                moviesNoTitle,
                moviesFailed,
                linksImported,
                skipped
        );
    }

    public void initializeTables() {
        // DDL은 최초 1회만 실행한다. (추천 재계산 등 핫패스에서 매번 ALTER/CREATE가 돌면
        //  H2 테이블 락 경합 + 암묵적 커밋으로 QueryTimeout이 발생하기 때문)
        if (tablesInitialized) {
            return;
        }
        synchronized (this) {
            if (tablesInitialized) {
                return;
            }
            doInitializeTables();
            tablesInitialized = true;
        }
    }

    private void doInitializeTables() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS movie_ott_link (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    movie_id BIGINT NOT NULL,
                    provider_name VARCHAR(120) NOT NULL,
                    watch_url VARCHAR(1000) NOT NULL,
                    source VARCHAR(80) NOT NULL DEFAULT 'KINOLIGHTS_CACHE',
                    display_order INT NOT NULL DEFAULT 0,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT uk_movie_ott_link UNIQUE (movie_id, provider_name)
                )
                """);
        jdbcTemplate.execute("""
                CREATE INDEX IF NOT EXISTS idx_movie_ott_link_movie
                ON movie_ott_link (movie_id, display_order)
                """);
        jdbcTemplate.execute("ALTER TABLE movie_ott_link ADD COLUMN IF NOT EXISTS raw_url VARCHAR(1000)");
        jdbcTemplate.execute("ALTER TABLE movie_ott_link ADD COLUMN IF NOT EXISTS raw_text VARCHAR(1000)");
        jdbcTemplate.execute("ALTER TABLE movie_ott_link ADD COLUMN IF NOT EXISTS source_method VARCHAR(80)");
        jdbcTemplate.execute("ALTER TABLE movie_ott_link ADD COLUMN IF NOT EXISTS is_external_direct BOOLEAN NOT NULL DEFAULT TRUE");
        jdbcTemplate.execute("ALTER TABLE movie_ott_link ADD COLUMN IF NOT EXISTS crawled_at TIMESTAMP");

        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS kinolights_title_mapping (
                    movie_id BIGINT PRIMARY KEY,
                    kinolights_url VARCHAR(1000),
                    status VARCHAR(30) NOT NULL DEFAULT 'PENDING',
                    note VARCHAR(1000),
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """);

        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS movie_ott_crawl_status (
                    movie_id BIGINT PRIMARY KEY,
                    kinolights_url VARCHAR(1000),
                    status VARCHAR(30) NOT NULL,
                    link_count INT NOT NULL DEFAULT 0,
                    provider_count INT NOT NULL DEFAULT 0,
                    crawled_at TIMESTAMP,
                    error_message VARCHAR(2000),
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """);
        jdbcTemplate.execute("""
                CREATE INDEX IF NOT EXISTS idx_movie_ott_crawl_status_status
                ON movie_ott_crawl_status (status, updated_at)
                """);
    }

    private Map<String, CrawlerRow> firstLinkByProvider(List<CrawlerRow> rows) {
        Map<String, CrawlerRow> result = new LinkedHashMap<>();
        for (CrawlerRow row : rows) {
            result.putIfAbsent(normalizeProviderName(row.provider()), row);
        }
        return result;
    }

    private void upsertMapping(Long movieId, String kinolightsUrl, String status, String note) {
        jdbcTemplate.update("""
                MERGE INTO kinolights_title_mapping (movie_id, kinolights_url, status, note, updated_at)
                KEY (movie_id)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, movieId, emptyToNull(kinolightsUrl), status, emptyToNull(note));
    }

    private void upsertCrawlStatus(
            Long movieId,
            String kinolightsUrl,
            String status,
            List<CrawlerRow> directRows,
            String error
    ) {
        int providerCount = firstLinkByProvider(directRows).size();
        Instant crawledAt = directRows.stream()
                .map(CrawlerRow::crawledAt)
                .filter(value -> value != null)
                .findFirst()
                .orElse(Instant.now());
        jdbcTemplate.update("""
                MERGE INTO movie_ott_crawl_status
                    (movie_id, kinolights_url, status, link_count, provider_count, crawled_at, error_message, updated_at)
                KEY (movie_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                movieId,
                emptyToNull(kinolightsUrl),
                status,
                directRows.size(),
                providerCount,
                Timestamp.from(crawledAt),
                emptyToNull(error));
    }

    private Long findMovieIdByCode(String movieCode) {
        if (!hasText(movieCode)) {
            return null;
        }
        return jdbcTemplate.query("""
                SELECT id
                FROM movie
                WHERE movie_cd = ?
                LIMIT 1
                """, rs -> rs.next() ? rs.getLong("id") : null, movieCode.trim());
    }

    private Long findMovieIdByTitle(String title) {
        if (!hasText(title)) {
            return null;
        }
        return jdbcTemplate.query("""
                SELECT id
                FROM movie
                WHERE LOWER(COALESCE(title, '')) = LOWER(?)
                   OR LOWER(COALESCE(movie_name, '')) = LOWER(?)
                   OR LOWER(COALESCE(original_title, '')) = LOWER(?)
                   OR LOWER(COALESCE(movie_name_original, '')) = LOWER(?)
                ORDER BY
                    CASE WHEN poster_image_url IS NOT NULL AND poster_image_url <> '' THEN 0 ELSE 1 END,
                    COALESCE(popularity, 0) DESC,
                    id DESC
                LIMIT 1
                """, rs -> rs.next() ? rs.getLong("id") : null, title, title, title, title);
    }

    private String resolveProviderLogoUrl(String providerName, Map<String, String> providerLogoUrls) {
        String key = normalizeProviderName(providerName);
        if ("coupangplay".equals(key)) {
            return COUPANG_PLAY_LOGO_URL;
        }
        if ("laftel".equals(key)) {
            return LAFTEL_LOGO_URL;
        }
        if ("cgv".equals(key)) {
            return CGV_LOGO_URL;
        }
        if ("lottecinema".equals(key)) {
            return LOTTE_CINEMA_LOGO_URL;
        }
        if ("megabox".equals(key)) {
            return MEGABOX_LOGO_URL;
        }
        for (Map.Entry<String, String> entry : providerLogoUrls.entrySet()) {
            String providerKey = normalizeProviderName(entry.getKey());
            if (hasText(entry.getValue()) && (providerKey.equals(key) || providerKey.contains(key) || key.contains(providerKey))) {
                return entry.getValue();
            }
        }
        return null;
    }

    public static String normalizeProviderName(String value) {
        if (value == null) {
            return "";
        }
        return value.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9가-힣]", "");
    }

    private static String normalizeTitle(String value) {
        if (value == null) {
            return "";
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private static String emptyToNull(String value) {
        return hasText(value) ? value : null;
    }

    private static String firstNonBlank(List<CrawlerRow> rows, ValueGetter getter) {
        for (CrawlerRow row : rows) {
            String value = getter.get(row);
            if (hasText(value)) {
                return value;
            }
        }
        return "";
    }

    public static String escapeCsv(String value) {
        if (value == null) {
            return "";
        }
        boolean quote = value.contains(",") || value.contains("\"") || value.contains("\r") || value.contains("\n");
        String escaped = value.replace("\"", "\"\"");
        return quote ? "\"" + escaped + "\"" : escaped;
    }

    public static Instant parseInstant(String value) {
        if (!hasText(value)) {
            return null;
        }
        try {
            return Instant.parse(value);
        } catch (Exception ignored) {
            return null;
        }
    }

    public record WatchLinkView(String providerName, String url, String logoUrl) {
        /** 로컬 로고가 없을 때 표시할 한 글자 배지. providerName 첫 글자(대문자), 비어 있으면 "?". */
        public String fallbackLabel() {
            if (providerName == null || providerName.isBlank()) {
                return "?";
            }
            return providerName.trim().substring(0, 1).toUpperCase();
        }
    }

    public record CrawlCandidate(
            Long movieId,
            String movieCode,
            String title,
            LocalDate releaseDate,
            String posterImageUrl,
            String kinolightsUrl,
            String priorityReason
    ) {
        public List<String> toCsvRow() {
            return List.of(
                    String.valueOf(movieId),
                    movieCode != null ? movieCode : "",
                    title != null ? title : "",
                    releaseDate != null ? releaseDate.format(DateTimeFormatter.ISO_LOCAL_DATE) : "",
                    posterImageUrl != null ? posterImageUrl : "",
                    kinolightsUrl != null ? kinolightsUrl : "",
                    priorityReason != null ? priorityReason : ""
            );
        }
    }

    public record CrawlerRow(
            Long movieId,
            String movieCode,
            String title,
            String kinolightsUrl,
            String status,
            String provider,
            String watchUrl,
            String rawUrl,
            String rawText,
            String sourceMethod,
            boolean isExternalDirect,
            String error,
            Instant crawledAt
    ) {
    }

    public record ImportResult(
            int moviesProcessed,
            int moviesImported,
            int moviesNoLink,
            int moviesNoTitle,
            int moviesFailed,
            int linksImported,
            List<String> skippedTitles
    ) {
    }

    @FunctionalInterface
    private interface ValueGetter {
        String get(CrawlerRow row);
    }
}
