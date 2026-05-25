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
                SELECT m.id, COALESCE(m.title, m.movie_name) AS title, m.original_title
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

                String videoKey = searchTrailerKey(title, originalTitle);
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

    private String searchTrailerKey(String title, String originalTitle) {
        // 한국어 제목으로 먼저 시도
        if (title != null && !title.isBlank()) {
            String key = queryYoutube(title + " 공식 예고편");
            if (key != null) return key;
        }
        // 원제로 재시도
        if (originalTitle != null && !originalTitle.isBlank() && !originalTitle.equals(title)) {
            String key = queryYoutube(originalTitle + " official trailer");
            if (key != null) return key;
        }
        return null;
    }

    @SuppressWarnings("deprecation")
    private String nodeText(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) return null;
        String t = node.asText();
        return t.isBlank() ? null : t;
    }

    private String queryYoutube(String query) {
        URI uri = UriComponentsBuilder.fromUriString(YOUTUBE_SEARCH_URL)
                .queryParam("part", "snippet")
                .queryParam("q", query)
                .queryParam("type", "video")
                .queryParam("videoEmbeddable", "true")
                .queryParam("maxResults", "1")
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

                String key = nodeText(items.get(0).path("id").path("videoId"));
                return (key == null || key.isBlank()) ? null : key;

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
}
