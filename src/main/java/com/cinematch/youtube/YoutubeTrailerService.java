package com.cinematch.youtube;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.util.List;
import java.util.Map;

@Service
public class YoutubeTrailerService {

    private static final String YOUTUBE_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search";

    private final JdbcTemplate jdbcTemplate;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    private final String apiKey;

    public YoutubeTrailerService(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            @Value("${youtube.api-key:}") String apiKey
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.restTemplate = new RestTemplate();  // TMDB 인터셉터 없는 plain RestTemplate
        this.objectMapper = objectMapper;
        this.apiKey = apiKey;
    }

    public record FillResult(int processed, int found, int notFound, int skipped) {}
    public record JobStatus(boolean running, int processed, int found, int notFound, int total) {}

    private volatile boolean jobRunning = false;
    private volatile int jobProcessed = 0;
    private volatile int jobFound = 0;
    private volatile int jobNotFound = 0;
    private volatile int jobTotal = 0;

    public JobStatus getStatus() {
        return new JobStatus(jobRunning, jobProcessed, jobFound, jobNotFound, jobTotal);
    }

    public String searchForMovie(long movieId) {
        Map<String, Object> row;
        try {
            row = jdbcTemplate.queryForMap("""
                    SELECT COALESCE(m.title, m.movie_name) AS title, m.original_title,
                           YEAR(m.release_date) AS release_year,
                           (SELECT p.name FROM movie_director md JOIN person p ON md.person_id = p.id
                            WHERE md.movie_id = m.id ORDER BY md.display_order LIMIT 1) AS director_name,
                           (SELECT p.name FROM movie_actor ma JOIN person p ON ma.person_id = p.id
                            WHERE ma.movie_id = m.id ORDER BY ma.display_order LIMIT 1) AS actor_name
                    FROM movie m WHERE m.id = ?
                    """, movieId);
        } catch (Exception e) {
            return null;
        }
        String title = (String) row.get("title");
        String originalTitle = (String) row.get("original_title");
        Integer year = row.get("release_year") != null ? ((Number) row.get("release_year")).intValue() : null;
        String directorName = (String) row.get("director_name");
        String actorName = (String) row.get("actor_name");

        String videoKey = searchTrailerKey(title, originalTitle, year, directorName, actorName);
        if (videoKey != null) {
            jdbcTemplate.update("""
                    INSERT INTO movie_video (movie_id, video_key, video_name, video_type, is_official, source, display_order)
                    VALUES (?, ?, '예고편', 'Trailer', FALSE, 'YOUTUBE', 1)
                    """, movieId, videoKey);
        }
        return videoKey;
    }

    public JobStatus startFillAsync(int limit) {
        if (jobRunning) {
            return getStatus();
        }
        Thread.ofVirtual().start(() -> fillMissingTrailers(limit));
        return new JobStatus(true, 0, 0, 0, limit);
    }

    public FillResult fillMissingTrailers(int limit) {
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalStateException("youtube.api-key가 설정되지 않았습니다.");
        }
        try {
            jdbcTemplate.execute("ALTER TABLE movie_video ADD COLUMN IF NOT EXISTS source VARCHAR(20)");
        } catch (Exception ignored) {}

        List<Map<String, Object>> targets = jdbcTemplate.queryForList("""
                SELECT m.id, COALESCE(m.title, m.movie_name) AS title, m.original_title,
                       YEAR(m.release_date) AS release_year,
                       (SELECT p.name FROM movie_director md JOIN person p ON md.person_id = p.id
                        WHERE md.movie_id = m.id ORDER BY md.display_order LIMIT 1) AS director_name,
                       (SELECT p.name FROM movie_actor ma JOIN person p ON ma.person_id = p.id
                        WHERE ma.movie_id = m.id ORDER BY ma.display_order LIMIT 1) AS actor_name
                FROM movie m
                WHERE NOT EXISTS (
                    SELECT 1 FROM movie_video v WHERE v.movie_id = m.id
                )
                AND EXISTS (
                    SELECT 1 FROM movie_source ms WHERE ms.movie_id = m.id AND ms.source_type = 'TMDB'
                )
                ORDER BY m.popularity DESC NULLS LAST
                LIMIT ?
                """, limit);

        jobRunning = true;
        jobProcessed = 0;
        jobFound = 0;
        jobNotFound = 0;
        jobTotal = targets.size();

        try {
            for (Map<String, Object> row : targets) {
                Long movieId = ((Number) row.get("id")).longValue();
                String title = (String) row.get("title");
                String originalTitle = (String) row.get("original_title");
                Integer year = row.get("release_year") != null ? ((Number) row.get("release_year")).intValue() : null;
                String directorName = (String) row.get("director_name");
                String actorName = (String) row.get("actor_name");

                String videoKey = searchTrailerKey(title, originalTitle, year, directorName, actorName);
                if (videoKey != null) {
                    jdbcTemplate.update("""
                            INSERT INTO movie_video (movie_id, video_key, video_name, video_type, is_official, source, display_order)
                            VALUES (?, ?, '예고편', 'Trailer', FALSE, 'YOUTUBE', 1)
                            """, movieId, videoKey);
                    jobFound++;
                } else {
                    jobNotFound++;
                }
                jobProcessed++;
            }
        } finally {
            jobRunning = false;
        }

        return new FillResult(jobTotal, jobFound, jobNotFound, 0);
    }

    private String searchTrailerKey(String title, String originalTitle, Integer year, String directorName, String actorName) {
        String yearStr = (year != null && year > 0) ? " " + year : "";
        // 감독 또는 주연 배우 이름 (둘 중 하나)
        String personStr = (directorName != null && !directorName.isBlank()) ? " " + directorName
                         : (actorName != null && !actorName.isBlank()) ? " " + actorName : "";

        // 한국어 제목 + 연도 + 인물명으로 먼저 시도
        if (title != null && !title.isBlank()) {
            String key = queryYoutube(title + yearStr + personStr + " 공식 예고편", title);
            if (key != null) return key;
            // 인물명 없이 재시도
            if (!personStr.isBlank()) {
                key = queryYoutube(title + yearStr + " 공식 예고편", title);
                if (key != null) return key;
            }
            // 연도도 없이 재시도
            if (!yearStr.isBlank()) {
                key = queryYoutube(title + " 공식 예고편", title);
                if (key != null) return key;
            }
        }
        // 원제 + 연도 + 인물명으로 재시도
        if (originalTitle != null && !originalTitle.isBlank() && !originalTitle.equals(title)) {
            String key = queryYoutube(originalTitle + yearStr + personStr + " official trailer", originalTitle);
            if (key != null) return key;
            if (!personStr.isBlank()) {
                key = queryYoutube(originalTitle + yearStr + " official trailer", originalTitle);
                if (key != null) return key;
            }
            if (!yearStr.isBlank()) {
                key = queryYoutube(originalTitle + " official trailer", originalTitle);
                if (key != null) return key;
            }
        }
        return null;
    }

    @SuppressWarnings("deprecation")
    private String nodeText(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) return null;
        String t = node.asText();
        return t.isBlank() ? null : t;
    }

    private String queryYoutube(String query, String titleHint) {
        URI uri = UriComponentsBuilder.fromUriString(YOUTUBE_SEARCH_URL)
                .queryParam("part", "snippet")
                .queryParam("q", query)
                .queryParam("type", "video")
                .queryParam("videoEmbeddable", "true")
                .queryParam("maxResults", "3")
                .queryParam("key", apiKey)
                .build()
                .toUri();

        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                String response = restTemplate.getForObject(uri, String.class);
                if (response == null) return null;

                JsonNode root = objectMapper.readTree(response);
                JsonNode items = root.path("items");
                if (!items.isArray() || items.isEmpty()) return null;

                // 영화 제목이 포함된 영상인지 검증 후 반환
                String normalizedHint = normalize(titleHint);
                for (JsonNode item : items) {
                    String videoId = nodeText(item.path("id").path("videoId"));
                    if (videoId == null) continue;
                    String videoTitle = nodeText(item.path("snippet").path("title"));
                    if (videoTitle != null && normalize(videoTitle).contains(normalizedHint)) {
                        return videoId;
                    }
                }
                return null;

            } catch (HttpClientErrorException e) {
                // 429(분당 제한) or 403(일일 쿼터 초과) → 10초 대기 후 재시도
                if (e.getStatusCode() == HttpStatus.TOO_MANY_REQUESTS
                        || e.getStatusCode() == HttpStatus.FORBIDDEN) {
                    try { Thread.sleep(10000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); return null; }
                } else {
                    return null;
                }
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }

    private String normalize(String s) {
        if (s == null) return "";
        return s.toLowerCase().replaceAll("[^a-z0-9가-힣]", "");
    }
}
