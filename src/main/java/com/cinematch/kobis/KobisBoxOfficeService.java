package com.cinematch.kobis;

import com.cinematch.recommendation.RecommendationFeaturePolicy;
import com.cinematch.recommendation.RecommendationMovieFilterService;
import com.cinematch.tag.MovieTagService;
import com.cinematch.tmdb.TmdbMovieImportService;
import com.cinematch.tmdb.TmdbMovieNormalizeService;
import com.cinematch.tmdb.TmdbTrendingService.TrendingMovieView;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.LinkedHashSet;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Service
public class KobisBoxOfficeService {

    private static final Logger log = LoggerFactory.getLogger(KobisBoxOfficeService.class);

    private static final DateTimeFormatter KOBIS_DATE = DateTimeFormatter.BASIC_ISO_DATE;
    private static final String TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w500";
    private static final String TMDB_BACKDROP_BASE = "https://image.tmdb.org/t/p/w1280";

    private volatile List<TrendingMovieView> cache = null;
    private volatile LocalDate cacheDate = null;

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    private final JdbcTemplate jdbcTemplate;
    private final TmdbMovieImportService tmdbMovieImportService;
    private final TmdbMovieNormalizeService tmdbMovieNormalizeService;
    private final MovieTagService movieTagService;
    private final RecommendationMovieFilterService recommendationMovieFilterService;
    private final RecommendationFeaturePolicy recommendationFeaturePolicy;
    private final String baseUrl;
    private final String apiKey;

    public KobisBoxOfficeService(
            ObjectMapper objectMapper,
            JdbcTemplate jdbcTemplate,
            TmdbMovieImportService tmdbMovieImportService,
            TmdbMovieNormalizeService tmdbMovieNormalizeService,
            MovieTagService movieTagService,
            RecommendationMovieFilterService recommendationMovieFilterService,
            RecommendationFeaturePolicy recommendationFeaturePolicy,
            @Value("${kobis.base-url:https://www.kobis.or.kr/kobisopenapi/webservice/rest}") String baseUrl,
            @Value("${kobis.api-key:}") String apiKey,
            @Value("${kobis.connect-timeout-ms:5000}") int connectTimeoutMs,
            @Value("${kobis.read-timeout-ms:10000}") int readTimeoutMs
    ) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(connectTimeoutMs);
        factory.setReadTimeout(readTimeoutMs);
        this.restTemplate = new RestTemplate(factory);
        this.objectMapper = objectMapper;
        this.jdbcTemplate = jdbcTemplate;
        this.tmdbMovieImportService = tmdbMovieImportService;
        this.tmdbMovieNormalizeService = tmdbMovieNormalizeService;
        this.movieTagService = movieTagService;
        this.recommendationMovieFilterService = recommendationMovieFilterService;
        this.recommendationFeaturePolicy = recommendationFeaturePolicy;
        this.baseUrl = baseUrl;
        this.apiKey = apiKey;
    }

    public List<TrendingMovieView> fetchBoxOffice(int limit) {
        if (apiKey == null || apiKey.isBlank()) {
            return List.of();
        }
        int normalized = Math.min(Math.max(limit, 1), 10);
        LocalDate today = LocalDate.now();
        if (cache != null && today.equals(cacheDate)) {
            return cache.subList(0, Math.min(normalized, cache.size()));
        }
        try {
            List<TrendingMovieView> result = loadFromKobis(normalized);
            if (!result.isEmpty()) {
                cache = result;
                cacheDate = today;
            }
            return result;
        } catch (Exception e) {
            log.error("fetchBoxOffice failed: {}", e.getMessage(), e);
            return cache != null ? cache.subList(0, Math.min(normalized, cache.size())) : List.of();
        }
    }

    private List<TrendingMovieView> loadFromKobis(int limit) {
        // 후보 수집: 일별 박스오피스를 우선하고, 부족분은 주간 박스오피스에서 중복 아닌 항목으로 보충한다.
        // (KOBIS 박스오피스는 일별·주간 모두 최대 10위까지만 제공하므로, 공연물 제외 후 10개를 채우려면 두 소스를 합친다.)
        List<KobisCandidate> candidates = new ArrayList<>();
        Set<String> seenNames = new LinkedHashSet<>();
        collectCandidates(candidates, seenNames, fetchDailyBoxOffice());
        collectCandidates(candidates, seenNames, fetchWeeklyBoxOffice());

        List<TrendingMovieView> views = new ArrayList<>();
        for (KobisCandidate candidate : candidates) {
            if (views.size() >= limit) break;
            BuiltMovie built = buildMovie(candidate);
            // 공연/콘서트/아이돌 영상물은 제외하고 다음 순위를 끌어올린다 (추천 엔진과 동일 기준).
            if (built.performance()) continue;
            views.add(built.view());
        }
        return views;
    }

    /** KOBIS 박스오피스 한 행에서 뽑아낸 후보 영화 (제목 + 개봉정보). */
    private record KobisCandidate(String movieNm, LocalDate releaseDate, String releaseYear) {}

    /** TMDB 매칭까지 끝난 노출용 뷰 + 공연물 여부. */
    private record BuiltMovie(TrendingMovieView view, boolean performance) {}

    private List<KobisCandidate> fetchDailyBoxOffice() {
        LocalDate targetDt = LocalDate.now().minusDays(1);
        URI uri = UriComponentsBuilder.fromUriString(baseUrl)
                .path("/boxoffice/searchDailyBoxOfficeList.json")
                .queryParam("key", apiKey)
                .queryParam("targetDt", targetDt.format(KOBIS_DATE))
                .build()
                .toUri();
        return parseCandidates(restTemplate.getForObject(uri, String.class), "dailyBoxOfficeList");
    }

    private List<KobisCandidate> fetchWeeklyBoxOffice() {
        // 주간 박스오피스는 보조 소스이므로 실패해도 일별 결과만으로 동작하도록 방어한다.
        try {
            LocalDate targetDt = LocalDate.now().minusDays(7);
            URI uri = UriComponentsBuilder.fromUriString(baseUrl)
                    .path("/boxoffice/searchWeeklyBoxOfficeList.json")
                    .queryParam("key", apiKey)
                    .queryParam("targetDt", targetDt.format(KOBIS_DATE))
                    .queryParam("weekGb", "0")
                    .build()
                    .toUri();
            return parseCandidates(restTemplate.getForObject(uri, String.class), "weeklyBoxOfficeList");
        } catch (Exception e) {
            log.warn("주간 박스오피스 조회 실패(일별만 사용): {}", e.getMessage());
            return List.of();
        }
    }

    private List<KobisCandidate> parseCandidates(String response, String listKey) {
        JsonNode root = readTree(response);
        if (root == null) {
            return List.of();
        }
        JsonNode list = root.path("boxOfficeResult").path(listKey);
        if (!list.isArray()) {
            return List.of();
        }
        List<KobisCandidate> candidates = new ArrayList<>();
        for (JsonNode entry : list) {
            String movieNm = entry.path("movieNm").asText(null);
            if (movieNm == null || movieNm.isBlank()) continue;

            String openDt = entry.path("openDt").asText(null);
            LocalDate releaseDate = null;
            String releaseYear = null;
            if (openDt != null && !openDt.isBlank()) {
                try {
                    releaseDate = LocalDate.parse(openDt);
                    releaseYear = String.valueOf(releaseDate.getYear());
                } catch (Exception ignored) {
                }
            }
            candidates.add(new KobisCandidate(movieNm.trim(), releaseDate, releaseYear));
        }
        return candidates;
    }

    private void collectCandidates(List<KobisCandidate> target, Set<String> seenNames, List<KobisCandidate> source) {
        for (KobisCandidate candidate : source) {
            if (seenNames.add(candidate.movieNm())) {
                target.add(candidate);
            }
        }
    }

    private BuiltMovie buildMovie(KobisCandidate candidate) {
        String movieNm = candidate.movieNm();
        String posterImageUrl = null;
        String backdropImageUrl = null;
        String detailUrl = null;
        Long localMovieId = null;
        try {
            JsonNode tmdbMatch = tmdbMovieImportService.findBestTmdbMatch(movieNm, candidate.releaseDate());
            if (tmdbMatch != null) {
                String posterPath = tmdbMatch.path("poster_path").asText(null);
                if (posterPath != null && !posterPath.isBlank()) {
                    posterImageUrl = TMDB_IMAGE_BASE + posterPath;
                }
                String backdropPath = tmdbMatch.path("backdrop_path").asText(null);
                if (backdropPath != null && !backdropPath.isBlank()) {
                    backdropImageUrl = TMDB_BACKDROP_BASE + backdropPath;
                }
                long tmdbId = tmdbMatch.path("id").asLong(0);
                if (tmdbId > 0) {
                    String movieCode = ensureLocalMovieCode(tmdbId);
                    if (movieCode != null) {
                        detailUrl = "/movies/" + movieCode;
                        localMovieId = findLocalMovieId(tmdbId);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("박스오피스 영화 '{}' TMDB 매칭 실패: {}", movieNm, e.getMessage());
        }

        TrendingMovieView view = new TrendingMovieView(
                detailUrl, movieNm, candidate.releaseYear(), posterImageUrl, backdropImageUrl);
        return new BuiltMovie(view, isPerformanceContent(localMovieId, movieNm));
    }

    private boolean isPerformanceContent(Long localMovieId, String movieNm) {
        if (localMovieId != null) {
            // DB 장르·키워드 기반 정밀 판별 (추천 후보 필터와 완전히 동일한 기준).
            return !recommendationMovieFilterService
                    .filterRecommendableMovieIds(Set.of(localMovieId))
                    .contains(localMovieId);
        }
        // TMDB 매칭 실패로 DB 메타데이터가 없으면 제목 기반으로만 판별한다.
        return recommendationFeaturePolicy.isPerformanceContent(Set.of(), Set.of(), movieNm, null);
    }

    private String findLocalMovieCode(long tmdbMovieId) {
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

    private Long findLocalMovieId(long tmdbMovieId) {
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

    private String ensureLocalMovieCode(long tmdbMovieId) {
        String movieCode = findLocalMovieCode(tmdbMovieId);
        if (movieCode != null) {
            tagIfMissing(tmdbMovieId);
            return movieCode;
        }

        try {
            tmdbMovieImportService.importMovieIds(List.of(tmdbMovieId));
            tmdbMovieNormalizeService.normalizeMovieIds(List.of(tmdbMovieId));
            tagIfMissing(tmdbMovieId);
            return findLocalMovieCode(tmdbMovieId);
        } catch (Exception ignored) {
            return null;
        }
    }

    private void tagIfMissing(long tmdbMovieId) {
        Long movieId = findLocalMovieId(tmdbMovieId);
        if (movieId == null || hasMovieTags(movieId)) {
            return;
        }

        Set<Long> movieIds = new LinkedHashSet<>();
        movieIds.add(movieId);
        movieTagService.tagMovies(movieIds);
    }

    private boolean hasMovieTags(Long movieId) {
        Integer count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM movie_tag
                WHERE movie_id = ?
                """, Integer.class, movieId);
        return count != null && count > 0;
    }

    private JsonNode readTree(String json) {
        if (json == null || json.isBlank()) return null;
        try {
            return objectMapper.readTree(json);
        } catch (JacksonException e) {
            return null;
        }
    }
}
