package com.cinematch.recommendation;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class MovieCoOccurrenceService {

    private static final int MIN_CO_COUNT = 2;

    private final JdbcTemplate jdbcTemplate;

    public MovieCoOccurrenceService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public CoOccurrenceRebuildResult rebuildCoOccurrence() {
        ensureTable();
        jdbcTemplate.update("DELETE FROM movie_co_occurrence");

        int inserted = jdbcTemplate.update("""
                INSERT INTO movie_co_occurrence (movie_id_a, movie_id_b, co_count, updated_at)
                SELECT a.movie_id, b.movie_id, COUNT(DISTINCT a.user_id), CURRENT_TIMESTAMP
                FROM (
                    SELECT user_id, movie_id FROM user_movie_life
                    UNION
                    SELECT user_id, movie_id FROM user_movie_like WHERE liked = TRUE
                    UNION
                    SELECT user_id, movie_id FROM user_movie_watched
                    WHERE COALESCE(rating, 0) >= 4
                ) a
                JOIN (
                    SELECT user_id, movie_id FROM user_movie_life
                    UNION
                    SELECT user_id, movie_id FROM user_movie_like WHERE liked = TRUE
                    UNION
                    SELECT user_id, movie_id FROM user_movie_watched
                    WHERE COALESCE(rating, 0) >= 4
                ) b ON a.user_id = b.user_id AND a.movie_id < b.movie_id
                GROUP BY a.movie_id, b.movie_id
                HAVING COUNT(DISTINCT a.user_id) >= ?
                """, MIN_CO_COUNT);

        return new CoOccurrenceRebuildResult(inserted);
    }

    public Map<Long, Double> loadCFScores(Set<Long> interactedMovieIds, Set<Long> excludedMovieIds, int topN) {
        if (interactedMovieIds.isEmpty()) {
            return Map.of();
        }
        ensureTable();

        String ph = interactedMovieIds.stream().map(id -> "?").collect(Collectors.joining(", "));
        String sql = """
                SELECT movie_id, SUM(co_count) AS total_co FROM (
                    SELECT movie_id_b AS movie_id, co_count
                    FROM movie_co_occurrence
                    WHERE movie_id_a IN (%s)
                    UNION ALL
                    SELECT movie_id_a AS movie_id, co_count
                    FROM movie_co_occurrence
                    WHERE movie_id_b IN (%s)
                ) combined
                GROUP BY movie_id
                """.formatted(ph, ph);

        List<Object> params = new ArrayList<>();
        params.addAll(interactedMovieIds);
        params.addAll(interactedMovieIds);

        Map<Long, Double> rawScores = new LinkedHashMap<>();
        jdbcTemplate.query(sql, (org.springframework.jdbc.core.RowCallbackHandler) rs -> {
            long movieId = rs.getLong("movie_id");
            if (!excludedMovieIds.contains(movieId) && !interactedMovieIds.contains(movieId)) {
                rawScores.merge(movieId, rs.getDouble("total_co"), Double::sum);
            }
        }, params.toArray());

        if (rawScores.isEmpty()) {
            return Map.of();
        }

        double maxScore = rawScores.values().stream().mapToDouble(Double::doubleValue).max().orElse(1.0);

        return rawScores.entrySet().stream()
                .sorted(Map.Entry.<Long, Double>comparingByValue(Comparator.reverseOrder()))
                .limit(topN)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue() / maxScore,
                        (a, b) -> a,
                        LinkedHashMap::new
                ));
    }

    private void ensureTable() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS movie_co_occurrence (
                    movie_id_a BIGINT NOT NULL,
                    movie_id_b BIGINT NOT NULL,
                    co_count   INT    NOT NULL DEFAULT 1,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (movie_id_a, movie_id_b),
                    CONSTRAINT fk_movie_co_occurrence_a FOREIGN KEY (movie_id_a) REFERENCES movie(id),
                    CONSTRAINT fk_movie_co_occurrence_b FOREIGN KEY (movie_id_b) REFERENCES movie(id)
                )
                """);
    }

    public record CoOccurrenceRebuildResult(int insertedPairCount) {
    }
}
