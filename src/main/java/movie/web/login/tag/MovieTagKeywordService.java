package movie.web.login.tag;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

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

    public MovieTagKeywordService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
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
        List<MovieKeywordSampleRow> rows = jdbcTemplate.query("""
                SELECT
                    m.id AS movie_id,
                    COALESCE(m.title, m.movie_name) AS display_title,
                    COALESCE(m.release_date, m.movie_info_open_date) AS release_date,
                    k.name AS keyword_name
                FROM movie_tag mt
                JOIN tag t ON t.id = mt.tag_id
                JOIN movie m ON m.id = mt.movie_id
                LEFT JOIN movie_keyword mk ON mk.movie_id = m.id
                LEFT JOIN keyword k ON k.id = mk.keyword_id
                WHERE t.tag_type = ?
                  AND t.tag_name = ?
                ORDER BY display_title, k.name
                """, (rs, rowNum) -> {
            Date releaseDate = rs.getDate("release_date");
            return new MovieKeywordSampleRow(
                    rs.getLong("movie_id"),
                    rs.getString("display_title"),
                    releaseDate == null ? null : releaseDate.toLocalDate(),
                    rs.getString("keyword_name")
            );
        }, tagType, tagName);

        Map<Long, MovieKeywordSampleBuilder> grouped = new LinkedHashMap<>();
        for (MovieKeywordSampleRow row : rows) {
            MovieKeywordSampleBuilder builder = grouped.computeIfAbsent(
                    row.movieId(),
                    ignored -> new MovieKeywordSampleBuilder(row.movieId(), row.displayTitle(), row.releaseDate())
            );
            if (row.keywordName() != null && !row.keywordName().isBlank()) {
                builder.keywords().add(row.keywordName());
            }
        }

        return grouped.values().stream()
                .limit(limit)
                .map(MovieKeywordSampleBuilder::build)
                .toList();
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

    private record MovieKeywordSampleRow(
            long movieId,
            String displayTitle,
            LocalDate releaseDate,
            String keywordName
    ) {
    }

    private record MovieKeywordSampleBuilder(
            long movieId,
            String displayTitle,
            LocalDate releaseDate,
            List<String> keywords
    ) {
        private MovieKeywordSampleBuilder(long movieId, String displayTitle, LocalDate releaseDate) {
            this(movieId, displayTitle, releaseDate, new ArrayList<>());
        }

        private MovieKeywordSample build() {
            return new MovieKeywordSample(movieId, displayTitle, releaseDate, List.copyOf(keywords));
        }
    }
}
