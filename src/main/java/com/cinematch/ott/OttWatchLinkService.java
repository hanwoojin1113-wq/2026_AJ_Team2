package com.cinematch.ott;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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

    private record SeedLink(String provider, String watchUrl) {}

    private static final Map<String, List<SeedLink>> WATCH_LINK_SEEDS = buildSeeds();

    private static Map<String, List<SeedLink>> buildSeeds() {
        Map<String, List<SeedLink>> m = new LinkedHashMap<>();
        m.put("기생충", List.of(
            new SeedLink("Netflix",      "https://www.netflix.com/kr/title/81221938"),
            new SeedLink("TVING",        "https://www.tving.com/contents/M000244233"),
            new SeedLink("Wavve",        "https://www.wavve.com/player/movie?movieid=MV_EN01_EN000000008"),
            new SeedLink("Watcha",       "https://watcha.com/contents/mdRL4eL")));
        m.put("공조2: 인터내셔날", List.of(
            new SeedLink("Netflix",      "https://www.netflix.com/kr/title/81650130"),
            new SeedLink("TVING",        "https://www.tving.com/contents/M000370011"),
            new SeedLink("Wavve",        "https://www.wavve.com/player/movie?movieid=MV_EN01_EN000001021"),
            new SeedLink("Watcha",       "https://watcha.com/contents/m5agQGD")));
        m.put("스파이더맨: 노 웨이 홈", List.of(
            new SeedLink("Wavve",        "https://www.wavve.com/player/movie?movieid=MV_CF01_SY0000011741"),
            new SeedLink("Watcha",       "https://watcha.com/contents/mOVP23Z"),
            new SeedLink("Apple TV",     "https://tv.apple.com/kr/movie/%E1%84%89%E1%85%B3%E1%84%91%E1%85%A1%E1%84%8B%E1%85%B5%E1%84%83%E1%85%A5%E1%84%86%E1%85%A2%E1%86%AB-%E1%84%82%E1%85%A9-%E1%84%8B%E1%85%B0%E1%84%8B%E1%85%B5-%E1%84%92%E1%85%A9%E1%86%B7/umc.cmc.2qf7xc5hds0m5jgx4roago580")));
        m.put("명량", List.of(
            new SeedLink("Netflix",      "https://www.netflix.com/kr/title/80011706"),
            new SeedLink("TVING",        "https://www.tving.com/contents/M000316042"),
            new SeedLink("Wavve",        "https://www.wavve.com/player/movie?movieid=MV_EN01_EN000000011"),
            new SeedLink("Watcha",       "https://watcha.com/contents/mWzM9D5")));
        m.put("탑건: 매버릭", List.of(
            new SeedLink("CGV",          "https://cgv.co.kr/cnm/cgvChart/movieChart/82120"),
            new SeedLink("Megabox",      "https://www.megabox.co.kr/movie-detail?rpstMovieNo=22018400"),
            new SeedLink("Wavve",        "https://www.wavve.com/player/movie?movieid=MV_CQ01_PT0000011377"),
            new SeedLink("Coupang Play", "https://www.coupangplay.com/titles/65dcb74f-2557-498d-b644-5f41c1b71ab4")));
        m.put("극장판 귀멸의 칼날: 무한열차편", List.of(
            new SeedLink("Netflix",      "https://www.netflix.com/kr/title/81504496"),
            new SeedLink("TVING",        "https://www.tving.com/contents/M000361348"),
            new SeedLink("Wavve",        "https://www.wavve.com/player/movie?movieid=MV_CR01_DN0000011239"),
            new SeedLink("Watcha",       "https://watcha.com/contents/mdEmAbg"),
            new SeedLink("Laftel",       "https://laftel.net/item/40847")));
        m.put("포레스트 검프", List.of(
            new SeedLink("Wavve",        "https://www.wavve.com/player/movie?movieid=MV_CQ01_PT0000011145")));
        m.put("쇼생크 탈출", List.of(
            new SeedLink("Coupang Play", "https://www.coupangplay.com/titles/6719833f-9d39-4537-a79a-997ff5b424b7")));
        m.put("엑시트", List.of(
            new SeedLink("Netflix",      "https://www.netflix.com/kr/title/81336395"),
            new SeedLink("TVING",        "https://www.tving.com/contents/M000250033"),
            new SeedLink("Wavve",        "https://www.wavve.com/player/movie?movieid=MV_EN01_EN000000018"),
            new SeedLink("Watcha",       "https://watcha.com/contents/mdErj22")));
        m.put("프로젝트 헤일메리", List.of(
            new SeedLink("CGV",          "https://cgv.co.kr/cnm/cgvChart/movieChart/30000994"),
            new SeedLink("Lotte Cinema", "https://www.lottecinema.co.kr/NLCHS/Movie/MovieDetailView?movie=24013"),
            new SeedLink("Megabox",      "https://www.megabox.co.kr/movie-detail?rpstMovieNo=26013700"),
            new SeedLink("Apple TV",     "https://tv.apple.com/kr/movie/%E1%84%91%E1%85%B3%E1%84%85%E1%85%A9%E1%84%8C%E1%85%A6%E1%86%A8%E1%84%90%E1%85%B3-%E1%84%92%E1%85%A6%E1%84%8B%E1%85%B5%E1%86%AF%E1%84%86%E1%85%A6%E1%84%85%E1%85%B5/umc.cmc.7jxdlxvz304lj3iwhtrhbe8fv")));
        return m;
    }

    private final JdbcTemplate jdbcTemplate;

    public OttWatchLinkService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<WatchLinkView> fetchWatchLinks(Long movieId, Map<String, String> providerLogoUrls) {
        initializeTables();
        seedWatchLinksIfNeeded(movieId);
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
                        WHEN EXISTS (SELECT 1 FROM social_post sp WHERE sp.movie_id = m.id AND sp.is_deleted = FALSE) THEN 'SOCIAL_POST'
                        WHEN EXISTS (SELECT 1 FROM movie_tag mt WHERE mt.movie_id = m.id) THEN 'TAGGED'
                        ELSE 'POPULARITY'
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
                        WHEN EXISTS (SELECT 1 FROM social_post sp WHERE sp.movie_id = m.id AND sp.is_deleted = FALSE) THEN 2
                        WHEN EXISTS (SELECT 1 FROM movie_tag mt WHERE mt.movie_id = m.id) THEN 3
                        ELSE 4
                    END,
                    COALESCE(m.popularity, 0) DESC,
                    COALESCE(m.vote_count, 0) DESC,
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
            Long movieId = first.movieId() != null ? first.movieId() : findMovieIdByTitle(first.title());
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

    private void seedWatchLinksIfNeeded(Long movieId) {
        String title = jdbcTemplate.query(
            "SELECT COALESCE(NULLIF(title,''), NULLIF(movie_name,'')) FROM movie WHERE id = ?",
            rs -> rs.next() ? rs.getString(1) : null, movieId);
        if (title == null) return;
        String norm = title.trim().toLowerCase(Locale.ROOT);
        List<SeedLink> seeds = null;
        for (Map.Entry<String, List<SeedLink>> e : WATCH_LINK_SEEDS.entrySet()) {
            if (e.getKey().trim().toLowerCase(Locale.ROOT).equals(norm)) {
                seeds = e.getValue();
                break;
            }
        }
        if (seeds == null) return;
        Integer count = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM movie_ott_link WHERE movie_id = ?", Integer.class, movieId);
        if (count != null && count > 0) return;
        Map<String, SeedLink> byProvider = new LinkedHashMap<>();
        for (SeedLink s : seeds) byProvider.putIfAbsent(normalizeProviderName(s.provider()), s);
        int order = 0;
        for (SeedLink s : byProvider.values()) {
            jdbcTemplate.update("""
                MERGE INTO movie_ott_link (movie_id, provider_name, watch_url, source, display_order)
                KEY (movie_id, provider_name)
                VALUES (?, ?, ?, 'SEED', ?)
                """, movieId, s.provider(), s.watchUrl(), order++);
        }
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
        public String fallbackLabel() {
            return providerName != null && !providerName.isEmpty()
                    ? String.valueOf(providerName.charAt(0)).toUpperCase(Locale.ROOT)
                    : "?";
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
