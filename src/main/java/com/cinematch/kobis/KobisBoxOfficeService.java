package com.cinematch.kobis;

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

import java.net.URI;
import java.util.LinkedHashSet;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Service
public class KobisBoxOfficeService {

    private static final DateTimeFormatter KOBIS_DATE = DateTimeFormatter.BASIC_ISO_DATE;
    private static final String TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w500";

    private volatile List<TrendingMovieView> cache = null;
    private volatile LocalDate cacheDate = null;

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    private final JdbcTemplate jdbcTemplate;
    private final TmdbMovieImportService tmdbMovieImportService;
    private final TmdbMovieNormalizeService tmdbMovieNormalizeService;
    private final MovieTagService movieTagService;
    private final String baseUrl;
    private final String apiKey;

    public KobisBoxOfficeService(
            ObjectMapper objectMapper,
            JdbcTemplate jdbcTemplate,
            TmdbMovieImportService tmdbMovieImportService,
            TmdbMovieNormalizeService tmdbMovieNormalizeService,
            MovieTagService movieTagService,
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
            return cache != null ? cache.subList(0, Math.min(normalized, cache.size())) : List.of();
        }
    }

    private List<TrendingMovieView> loadFromKobis(int limit) {
        LocalDate targetDt = LocalDate.now().minusDays(1);
        URI uri = UriComponentsBuilder.fromUriString(baseUrl)
                .path("/boxoffice/searchDailyBoxOfficeList.json")
                .queryParam("key", apiKey)
                .queryParam("targetDt", targetDt.format(KOBIS_DATE))
                .build()
                .toUri();

        String response = restTemplate.getForObject(uri, String.class);
        JsonNode root = readTree(response);
        if (root == null) {
            return List.of();
        }

        JsonNode list = root.path("boxOfficeResult").path("dailyBoxOfficeList");
        if (!list.isArray()) {
            return List.of();
        }

        List<TrendingMovieView> views = new ArrayList<>();
        for (JsonNode entry : list) {
            if (views.size() >= limit) break;

            String movieNm = entry.path("movieNm").asText(null);
            String openDt = entry.path("openDt").asText(null);
            if (movieNm == null || movieNm.isBlank()) continue;

            LocalDate releaseDate = null;
            String releaseYear = null;
            if (openDt != null && !openDt.isBlank()) {
                try {
                    releaseDate = LocalDate.parse(openDt);
                    releaseYear = String.valueOf(releaseDate.getYear());
                } catch (Exception ignored) {
                }
            }

            String posterImageUrl = null;
            String detailUrl = null;
            JsonNode tmdbMatch = tmdbMovieImportService.findBestTmdbMatch(movieNm, releaseDate);
            if (tmdbMatch != null) {
                String posterPath = tmdbMatch.path("poster_path").asText(null);
                if (posterPath != null && !posterPath.isBlank()) {
                    posterImageUrl = TMDB_IMAGE_BASE + posterPath;
                }
                long tmdbId = tmdbMatch.path("id").asLong(0);
                if (tmdbId > 0) {
                    String movieCode = ensureLocalMovieCode(tmdbId);
                    if (movieCode != null) {
                        detailUrl = "/movies/" + movieCode;
                    }
                }
            }

            views.add(new TrendingMovieView(detailUrl, movieNm, releaseYear, posterImageUrl));
        }
        return views;
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
