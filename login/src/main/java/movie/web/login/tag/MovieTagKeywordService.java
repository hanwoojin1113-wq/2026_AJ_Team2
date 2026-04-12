package movie.web.login.tag;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class MovieTagKeywordService {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public MovieTagKeywordService(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    public KeywordSampleResult sampleKeywords(String tagType, String tagName, int limit) {
        int safeLimit = Math.max(1, Math.min(limit, 30));

        List<MovieKeywordSample> movies = findMovieKeywordSamples(tagType, tagName, safeLimit);

        Map<String, Integer> keywordCounts = new LinkedHashMap<>();
        for (MovieKeywordSample movie : movies) {
            for (String keyword : movie.keywords()) {
                keywordCounts.merge(keyword, 1, Integer::sum);
            }
        }

        List<KeywordCount> topKeywords = keywordCounts.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue(Comparator.reverseOrder())
                        .thenComparing(Map.Entry::getKey))
                .limit(30)
                .map(entry -> new KeywordCount(entry.getKey(), entry.getValue()))
                .toList();

        return new KeywordSampleResult(tagType, tagName, movies.size(), topKeywords, movies);
    }

    public List<MovieKeywordSample> movieKeywords(String tagType, String tagName, int limit) {
        int safeLimit = Math.max(1, Math.min(limit, 50));
        return findMovieKeywordSamples(tagType, tagName, safeLimit);
    }

    private List<MovieKeywordSample> findMovieKeywordSamples(String tagType, String tagName, int limit) {
        return jdbcTemplate.query("""
                SELECT
                    m.id AS movie_id,
                    COALESCE(m.title, m.movie_name) AS display_title,
                    COALESCE(m.release_date, m.movie_info_open_date) AS release_date,
                    tm.keywords_json AS keywords_json
                FROM movie_tag mt
                JOIN tag t ON t.id = mt.tag_id
                JOIN movie m ON m.id = mt.movie_id
                JOIN movie_source ms ON ms.movie_id = m.id AND ms.source_type = 'TMDB'
                JOIN tmdb_movie tm ON tm.tmdb_movie_id = CAST(ms.source_key AS BIGINT)
                WHERE t.tag_type = ?
                  AND t.tag_name = ?
                ORDER BY display_title
                LIMIT ?
                """, (rs, rowNum) -> {
            Date releaseDate = rs.getDate("release_date");
            List<String> keywords = extractKeywords(rs.getString("keywords_json"));
            return new MovieKeywordSample(
                    rs.getLong("movie_id"),
                    rs.getString("display_title"),
                    releaseDate == null ? null : releaseDate.toLocalDate(),
                    keywords
            );
        }, tagType, tagName, limit);
    }

    private List<String> extractKeywords(String keywordsJson) {
        if (keywordsJson == null || keywordsJson.isBlank()) {
            return List.of();
        }

        try {
            JsonNode root = objectMapper.readTree(keywordsJson);
            JsonNode keywordArray = root.path("keywords");
            if (!keywordArray.isArray()) {
                keywordArray = root.path("results");
            }

            if (!keywordArray.isArray()) {
                return List.of();
            }

            List<String> keywords = new ArrayList<>();
            for (JsonNode keywordNode : keywordArray) {
                String name = textValue(keywordNode, "name");
                if (name != null && !name.isBlank()) {
                    keywords.add(name);
                }
            }
            return keywords;
        } catch (Exception e) {
            return List.of();
        }
    }

    private String textValue(JsonNode node, String fieldName) {
        JsonNode value = node.get(fieldName);
        return value == null || value.isNull() ? null : value.asText();
    }

    public record KeywordSampleResult(
            String tagType,
            String tagName,
            int sampleCount,
            List<KeywordCount> topKeywords,
            List<MovieKeywordSample> movies
    ) {
    }

    public record KeywordCount(
            String keyword,
            int count
    ) {
    }

    public record MovieKeywordSample(
            long movieId,
            String displayTitle,
            LocalDate releaseDate,
            List<String> keywords
    ) {
    }
}
