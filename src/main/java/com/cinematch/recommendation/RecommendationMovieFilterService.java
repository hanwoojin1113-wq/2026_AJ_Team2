package com.cinematch.recommendation;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class RecommendationMovieFilterService {

    private final JdbcTemplate jdbcTemplate;
    private final RecommendationFeaturePolicy recommendationFeaturePolicy;

    public RecommendationMovieFilterService(
            JdbcTemplate jdbcTemplate,
            RecommendationFeaturePolicy recommendationFeaturePolicy
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.recommendationFeaturePolicy = recommendationFeaturePolicy;
    }

    public Set<Long> filterRecommendableMovieIds(Set<Long> movieIds) {
        if (movieIds == null || movieIds.isEmpty()) {
            return Collections.emptySet();
        }

        Map<Long, MovieMetadata> metadataByMovieId = loadMovieMetadata(movieIds);
        return movieIds.stream()
                .filter(movieId -> {
                    MovieMetadata metadata = metadataByMovieId.get(movieId);
                    return metadata != null && recommendationFeaturePolicy.isRecommendationEligible(
                            metadata.genres(),
                            metadata.keywords(),
                            metadata.title(),
                            metadata.originalTitle()
                    );
                })
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    public Set<Long> filterActorEligibleMovieIds(Set<Long> movieIds) {
        if (movieIds == null || movieIds.isEmpty()) {
            return Collections.emptySet();
        }

        Map<Long, MovieMetadata> metadataByMovieId = loadMovieMetadata(movieIds);
        return movieIds.stream()
                .filter(movieId -> {
                    MovieMetadata metadata = metadataByMovieId.get(movieId);
                    return metadata != null
                            && recommendationFeaturePolicy.isRecommendationEligible(
                            metadata.genres(),
                            metadata.keywords(),
                            metadata.title(),
                            metadata.originalTitle()
                    )
                            && !recommendationFeaturePolicy.isAnimationMovie(metadata.genres());
                })
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private Map<Long, MovieMetadata> loadMovieMetadata(Set<Long> movieIds) {
        Map<Long, MutableMetadata> mutableMetadataByMovieId = new LinkedHashMap<>();
        List<Object> params = new ArrayList<>(movieIds);
        String placeholders = placeholders(movieIds.size());

        jdbcTemplate.query("""
                SELECT
                    m.id,
                    COALESCE(m.title, m.movie_name) AS title,
                    COALESCE(m.original_title, m.movie_name_original, m.movie_name_en) AS original_title
                FROM movie m
                WHERE m.id IN (%s)
                """.formatted(placeholders), rs -> {
            long movieId = rs.getLong("id");
            mutableMetadataByMovieId.put(movieId, new MutableMetadata(
                    rs.getString("title"),
                    rs.getString("original_title")
            ));
        }, params.toArray());

        jdbcTemplate.query("""
                SELECT mg.movie_id, g.name AS genre_name
                FROM movie_genre mg
                JOIN genre g ON g.id = mg.genre_id
                WHERE mg.movie_id IN (%s)
                """.formatted(placeholders), (org.springframework.jdbc.core.RowCallbackHandler) rs -> mutableMetadataByMovieId
                .computeIfAbsent(rs.getLong("movie_id"), ignored -> new MutableMetadata(null, null))
                .genres.add(rs.getString("genre_name")), params.toArray());

        jdbcTemplate.query("""
                SELECT mk.movie_id, k.name AS keyword_name
                FROM movie_keyword mk
                JOIN keyword k ON k.id = mk.keyword_id
                WHERE mk.movie_id IN (%s)
                """.formatted(placeholders), (org.springframework.jdbc.core.RowCallbackHandler) rs -> mutableMetadataByMovieId
                .computeIfAbsent(rs.getLong("movie_id"), ignored -> new MutableMetadata(null, null))
                .keywords.add(rs.getString("keyword_name")), params.toArray());

        return mutableMetadataByMovieId.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().toImmutable(),
                        (left, right) -> left,
                        LinkedHashMap::new
                ));
    }

    private String placeholders(int count) {
        return java.util.stream.IntStream.range(0, count)
                .mapToObj(index -> "?")
                .collect(Collectors.joining(", "));
    }

    private static final class MutableMetadata {
        private final String title;
        private final String originalTitle;
        private final Set<String> genres = new LinkedHashSet<>();
        private final Set<String> keywords = new LinkedHashSet<>();

        private MutableMetadata(String title, String originalTitle) {
            this.title = title;
            this.originalTitle = originalTitle;
        }

        private MovieMetadata toImmutable() {
            return new MovieMetadata(title, originalTitle, Set.copyOf(genres), Set.copyOf(keywords));
        }
    }

    public record MovieMetadata(
            String title,
            String originalTitle,
            Set<String> genres,
            Set<String> keywords
    ) {
    }
}
