package com.cinematch.recommendation;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class UserPreferenceProfileService {

    private final JdbcTemplate jdbcTemplate;
    private final RecommendationFeaturePolicy recommendationFeaturePolicy;
    private final RecommendationMovieFilterService recommendationMovieFilterService;
    private volatile boolean watchedTableInitialized = false;

    public UserPreferenceProfileService(
            JdbcTemplate jdbcTemplate,
            RecommendationFeaturePolicy recommendationFeaturePolicy,
            RecommendationMovieFilterService recommendationMovieFilterService
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.recommendationFeaturePolicy = recommendationFeaturePolicy;
        this.recommendationMovieFilterService = recommendationMovieFilterService;
    }

    public ProfileRebuildResult rebuildProfile(Long userId) {
        initializeProfileTable();

        Map<Long, MovieSignalWeights> signalWeightsByMovie = loadSignalWeights(userId);
        jdbcTemplate.update("""
                DELETE FROM user_preference_profile
                WHERE user_id = ?
                """, userId);

        if (signalWeightsByMovie.isEmpty()) {
            return new ProfileRebuildResult(userId, 0, 0, Map.of());
        }

        signalWeightsByMovie.keySet().retainAll(
                recommendationMovieFilterService.filterRecommendableMovieIds(signalWeightsByMovie.keySet())
        );
        if (signalWeightsByMovie.isEmpty()) {
            return new ProfileRebuildResult(userId, 0, 0, Map.of());
        }

        Set<Long> actorEligibleMovieIds =
                recommendationMovieFilterService.filterActorEligibleMovieIds(signalWeightsByMovie.keySet());

        LocalDateTime updatedAt = LocalDateTime.now();
        Map<String, Integer> featureCounts = new LinkedHashMap<>();
        int insertedRows = 0;

        Map<Long, List<String>> tagFeatures = loadMovieTagFeatures(signalWeightsByMovie.keySet(), false);
        Map<Long, List<String>> cautionTagFeatures = loadMovieTagFeatures(signalWeightsByMovie.keySet(), true);
        Map<Long, List<String>> genreFeatures = loadNamedFeatures(signalWeightsByMovie.keySet(), """
                SELECT mg.movie_id, g.name AS feature_name
                FROM movie_genre mg
                JOIN genre g ON g.id = mg.genre_id
                WHERE mg.movie_id IN (%s)
                """);
        Map<Long, List<String>> directorFeatures = loadNamedFeatures(signalWeightsByMovie.keySet(), """
                SELECT md.movie_id, p.name AS feature_name
                FROM movie_director md
                JOIN person p ON p.id = md.person_id
                WHERE md.movie_id IN (%s)
                """);
        Map<Long, List<String>> actorFeatures = loadNamedFeatures(signalWeightsByMovie.keySet(), """
                SELECT ma.movie_id, p.name AS feature_name
                FROM movie_actor ma
                JOIN person p ON p.id = ma.person_id
                WHERE ma.movie_id IN (%s)
                  AND ma.display_order <= %d
                """.formatted("%s", recommendationFeaturePolicy.actorLimit()));
        Map<Long, List<String>> keywordFeatures = loadNamedFeatures(signalWeightsByMovie.keySet(), """
                SELECT mk.movie_id, k.name AS feature_name
                FROM movie_keyword mk
                JOIN keyword k ON k.id = mk.keyword_id
                WHERE mk.movie_id IN (%s)
                  AND mk.display_order <= %d
                  AND LOWER(k.name) NOT IN (%s)
                """.formatted(
                "%s",
                recommendationFeaturePolicy.keywordLimit(),
                recommendationFeaturePolicy.keywordBlacklistSqlLiteralList()
        ));
        Map<Long, List<String>> providerFeatures = loadNamedFeatures(signalWeightsByMovie.keySet(), """
                SELECT DISTINCT mp.movie_id, p.provider_name AS feature_name
                FROM movie_provider mp
                JOIN provider p ON p.id = mp.provider_id
                WHERE mp.movie_id IN (%s)
                  AND mp.provider_type = '%s'
                  AND mp.region_code = '%s'
                """.formatted(
                "%s",
                recommendationFeaturePolicy.preferredProviderType(),
                recommendationFeaturePolicy.preferredProviderRegionCode()
        ));

        insertedRows += saveFeatureScores(userId, "TAG",
                applyTagDominanceControl(
                        aggregateFeatureScores(
                                "TAG",
                                signalWeightsByMovie,
                                tagFeatures,
                                recommendationFeaturePolicy.profileMultiplier("TAG"),
                                feature -> 1.0,
                                null
                        )
                ),
                updatedAt,
                featureCounts
        );

        insertedRows += saveFeatureScores(userId, "CAUTION",
                aggregateFeatureScores(
                        "CAUTION",
                        signalWeightsByMovie,
                        cautionTagFeatures,
                        recommendationFeaturePolicy.profileMultiplier("CAUTION"),
                        feature -> 1.0,
                        null
                ),
                updatedAt,
                featureCounts
        );

        insertedRows += saveFeatureScores(userId, "GENRE",
                applyGenreDominanceControl(
                        aggregateFeatureScores(
                                "GENRE",
                                signalWeightsByMovie,
                                genreFeatures,
                                recommendationFeaturePolicy.profileMultiplier("GENRE"),
                                feature -> recommendationFeaturePolicy.featureSpecificWeight("GENRE", feature),
                                null
                        ),
                        buildFeatureEvidence(signalWeightsByMovie, genreFeatures, null)
                ),
                updatedAt,
                featureCounts
        );

        insertedRows += saveFeatureScores(userId, "DIRECTOR",
                aggregateFeatureScores(
                        "DIRECTOR",
                        signalWeightsByMovie,
                        directorFeatures,
                        recommendationFeaturePolicy.profileMultiplier("DIRECTOR"),
                        feature -> 1.0,
                        null
                ),
                updatedAt,
                featureCounts
        );

        insertedRows += saveFeatureScores(userId, "ACTOR",
                aggregateFeatureScores(
                        "ACTOR",
                        signalWeightsByMovie,
                        actorFeatures,
                        recommendationFeaturePolicy.profileMultiplier("ACTOR"),
                        feature -> 1.0,
                        actorEligibleMovieIds
                ),
                updatedAt,
                featureCounts
        );

        insertedRows += saveFeatureScores(userId, "KEYWORD",
                aggregateFeatureScores(
                        "KEYWORD",
                        signalWeightsByMovie,
                        keywordFeatures,
                        recommendationFeaturePolicy.profileMultiplier("KEYWORD"),
                        feature -> 1.0,
                        null
                ),
                updatedAt,
                featureCounts
        );

        insertedRows += saveFeatureScores(userId, "PROVIDER",
                aggregateFeatureScores(
                        "PROVIDER",
                        signalWeightsByMovie,
                        providerFeatures,
                        recommendationFeaturePolicy.profileMultiplier("PROVIDER"),
                        feature -> 1.0,
                        null
                ),
                updatedAt,
                featureCounts
        );

        return new ProfileRebuildResult(userId, signalWeightsByMovie.size(), insertedRows, featureCounts);
    }

    private void initializeProfileTable() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS user_preference_profile (
                    user_id BIGINT NOT NULL,
                    feature_type VARCHAR(20) NOT NULL,
                    feature_name VARCHAR(255) NOT NULL,
                    score DOUBLE PRECISION NOT NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, feature_type, feature_name),
                    CONSTRAINT fk_user_preference_profile_user FOREIGN KEY (user_id) REFERENCES "USER"(id)
                )
                """);
    }

    private Map<Long, MovieSignalWeights> loadSignalWeights(Long userId) {
        Map<Long, MovieSignalWeights> signalWeightsByMovie = new LinkedHashMap<>();

        initializeWatchedSignalTable();

        mergeFixedSignal(signalWeightsByMovie, userId, """
                SELECT movie_id
                FROM user_movie_life
                WHERE user_id = ?
                """, recommendationFeaturePolicy.lifeSignalWeight());

        mergeFixedSignal(signalWeightsByMovie, userId, """
                SELECT movie_id
                FROM user_movie_like
                WHERE user_id = ?
                  AND liked = TRUE
                """, recommendationFeaturePolicy.likeSignalWeight());

        jdbcTemplate.query("""
                SELECT movie_id, rating
                FROM user_movie_watched
                WHERE user_id = ?
                  AND COALESCE(status, 'WATCHED') = 'WATCHED'
                """, (org.springframework.jdbc.core.RowCallbackHandler) rs -> {
            long movieId = rs.getLong("movie_id");
            double watchedSignal = recommendationFeaturePolicy.watchedSignalWeight(
                    rs.getObject("rating", Integer.class)
            );
            signalWeightsByMovie.compute(
                    movieId,
                    (ignored, current) -> {
                        MovieSignalWeights base = current == null ? MovieSignalWeights.empty() : current;
                        return base.addWatchedSignal(watchedSignal);
                    }
            );
        }, userId);

        mergeFixedSignal(signalWeightsByMovie, userId, """
                SELECT movie_id
                FROM user_movie_store
                WHERE user_id = ?
                """, recommendationFeaturePolicy.storeSignalWeight());

        signalWeightsByMovie.entrySet().removeIf(entry -> !entry.getValue().hasAnySignal());
        return signalWeightsByMovie;
    }

    private void mergeFixedSignal(
            Map<Long, MovieSignalWeights> signalWeightsByMovie,
            Long userId,
            String sql,
            double weight
    ) {
        jdbcTemplate.query(sql, (org.springframework.jdbc.core.RowCallbackHandler) rs -> {
            signalWeightsByMovie.compute(
                    rs.getLong("movie_id"),
                    (ignored, current) -> {
                        MovieSignalWeights base = current == null ? MovieSignalWeights.empty() : current;
                        return base.addPositive(weight);
                    }
            );
        }, userId);
    }

    private synchronized void initializeWatchedSignalTable() {
        if (watchedTableInitialized) {
            return;
        }
        watchedTableInitialized = true;
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS user_movie_watched (
                    user_id BIGINT NOT NULL,
                    movie_id BIGINT NOT NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'WATCHED',
                    rating INT,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, movie_id),
                    CONSTRAINT fk_user_movie_watched_user FOREIGN KEY (user_id) REFERENCES "USER"(id),
                    CONSTRAINT fk_user_movie_watched_movie FOREIGN KEY (movie_id) REFERENCES movie(id)
                )
                """);
        jdbcTemplate.execute("""
                ALTER TABLE user_movie_watched
                ADD COLUMN IF NOT EXISTS rating INT
                """);
        jdbcTemplate.execute("""
                ALTER TABLE user_movie_watched
                ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'WATCHED'
                """);
        jdbcTemplate.update("""
                UPDATE user_movie_watched
                SET status = 'WATCHED'
                WHERE status IS NULL
                """);
    }

    private Map<Long, List<String>> loadMovieTagFeatures(Set<Long> movieIds, boolean cautionOnly) {
        String sql = """
                SELECT mt.movie_id, t.tag_name AS feature_name
                FROM movie_tag mt
                JOIN tag t ON t.id = mt.tag_id
                WHERE mt.movie_id IN (%s)
                  AND t.tag_type %s 'CAUTION'
                """.formatted(
                placeholders(movieIds.size()),
                cautionOnly ? "=" : "<>"
        );
        return queryFeatureMap(sql, movieIds);
    }

    private Map<Long, List<String>> loadNamedFeatures(Set<Long> movieIds, String template) {
        String sql = template.formatted(placeholders(movieIds.size()));
        return queryFeatureMap(sql, movieIds);
    }

    private Map<Long, List<String>> queryFeatureMap(String sql, Set<Long> movieIds) {
        Map<Long, Set<String>> featuresByMovie = new LinkedHashMap<>();
        List<Object> params = new ArrayList<>(movieIds);

        jdbcTemplate.query(sql, rs -> {
            long movieId = rs.getLong("movie_id");
            String featureName = rs.getString("feature_name");
            if (featureName == null || featureName.isBlank()) {
                return;
            }
            featuresByMovie
                    .computeIfAbsent(movieId, ignored -> new LinkedHashSet<>())
                    .add(featureName);
        }, params.toArray());

        return featuresByMovie.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> new ArrayList<>(entry.getValue()),
                        (left, right) -> left,
                        LinkedHashMap::new
                ));
    }

    private Map<String, Double> aggregateFeatureScores(
            String featureType,
            Map<Long, MovieSignalWeights> signalWeightsByMovie,
            Map<Long, List<String>> featuresByMovie,
            double featureMultiplier,
            ToDoubleFunction<String> featureWeightResolver,
            Set<Long> includedMovieIds
    ) {
        Map<String, Double> scores = new LinkedHashMap<>();

        for (Map.Entry<Long, MovieSignalWeights> entry : signalWeightsByMovie.entrySet()) {
            Long movieId = entry.getKey();
            if (includedMovieIds != null && !includedMovieIds.contains(movieId)) {
                continue;
            }

            List<String> features = featuresByMovie.get(movieId);
            if (features == null || features.isEmpty()) {
                continue;
            }

            double positiveContribution = featureMultiplier * entry.getValue().positiveWeight() / features.size();
            double negativeContribution = featureMultiplier * entry.getValue().negativeWeight() / features.size();

            for (String feature : features) {
                double featureWeight = Math.max(0.0, featureWeightResolver.applyAsDouble(feature));
                if (featureWeight <= 0.0) {
                    continue;
                }

                double adjustedPositive = positiveContribution * featureWeight;
                double adjustedNegative = negativeContribution
                        * featureWeight
                        * recommendationFeaturePolicy.negativeFeatureScale(featureType);

                double totalContribution = adjustedPositive + adjustedNegative;
                if (totalContribution == 0.0) {
                    continue;
                }
                scores.merge(feature, totalContribution, Double::sum);
            }
        }

        return scores;
    }

    private Map<String, Double> applyTagDominanceControl(Map<String, Double> rawScores) {
        Map<String, Double> adjustedScores = new LinkedHashMap<>();
        rawScores.forEach((feature, rawScore) -> {
            double adjustedScore = applySignedSaturation(rawScore, recommendationFeaturePolicy::saturateTagScore);
            if (adjustedScore != 0.0) {
                adjustedScores.put(feature, adjustedScore);
            }
        });
        return adjustedScores;
    }

    private Map<String, Double> applyGenreDominanceControl(
            Map<String, Double> rawScores,
            Map<String, FeatureEvidence> evidenceByFeature
    ) {
        Map<String, Double> adjustedScores = new LinkedHashMap<>();
        rawScores.forEach((feature, rawScore) -> {
            FeatureEvidence evidence = evidenceByFeature.getOrDefault(feature, FeatureEvidence.empty());
            double adjustedScore = recommendationFeaturePolicy.applyGenreDominanceControl(
                    feature,
                    rawScore,
                    evidence.movieCount(),
                    evidence.signalWeightSum()
            );
            if (adjustedScore != 0.0) {
                adjustedScores.put(feature, adjustedScore);
            }
        });
        return adjustedScores;
    }

    private Map<String, FeatureEvidence> buildFeatureEvidence(
            Map<Long, MovieSignalWeights> signalWeightsByMovie,
            Map<Long, List<String>> featuresByMovie,
            Set<Long> includedMovieIds
    ) {
        Map<String, FeatureEvidence> evidenceByFeature = new LinkedHashMap<>();

        for (Map.Entry<Long, MovieSignalWeights> entry : signalWeightsByMovie.entrySet()) {
            Long movieId = entry.getKey();
            if (includedMovieIds != null && !includedMovieIds.contains(movieId)) {
                continue;
            }
            if (entry.getValue().positiveWeight() <= 0.0) {
                continue;
            }

            List<String> features = featuresByMovie.get(movieId);
            if (features == null || features.isEmpty()) {
                continue;
            }

            for (String feature : features) {
                evidenceByFeature.compute(feature, (ignored, current) -> {
                    FeatureEvidence base = current == null ? FeatureEvidence.empty() : current;
                    return base.add(entry.getValue().positiveWeight());
                });
            }
        }

        return evidenceByFeature;
    }

    private int saveFeatureScores(
            Long userId,
            String featureType,
            Map<String, Double> scores,
            LocalDateTime updatedAt,
            Map<String, Integer> featureCounts
    ) {
        if (scores.isEmpty()) {
            featureCounts.put(featureType, 0);
            return 0;
        }

        List<Map.Entry<String, Double>> entries = scores.entrySet().stream()
                .filter(entry -> entry.getValue() != 0.0)
                .sorted(Comparator
                        .comparingDouble((Map.Entry<String, Double> entry) -> Math.abs(entry.getValue())).reversed()
                        .thenComparing(Map.Entry.<String, Double>comparingByValue().reversed())
                        .thenComparing(Map.Entry::getKey))
                .limit(recommendationFeaturePolicy.profileFeatureLimit(featureType))
                .toList();

        jdbcTemplate.batchUpdate("""
                INSERT INTO user_preference_profile (
                    user_id,
                    feature_type,
                    feature_name,
                    score,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?)
                """, entries, entries.size(), (PreparedStatement ps, Map.Entry<String, Double> entry) -> {
            ps.setLong(1, userId);
            ps.setString(2, featureType);
            ps.setString(3, entry.getKey());
            ps.setDouble(4, roundScore(entry.getValue()));
            ps.setTimestamp(5, Timestamp.valueOf(updatedAt));
        });

        featureCounts.put(featureType, entries.size());
        return entries.size();
    }

    private String placeholders(int count) {
        return java.util.stream.IntStream.range(0, count)
                .mapToObj(index -> "?")
                .collect(Collectors.joining(", "));
    }

    private double applySignedSaturation(double rawScore, ToDoubleFunction<Double> saturationFunction) {
        if (rawScore == 0.0) {
            return 0.0;
        }
        return Math.signum(rawScore) * saturationFunction.applyAsDouble(Math.abs(rawScore));
    }

    private double roundScore(double value) {
        return Math.round(value * 1000.0) / 1000.0;
    }

    public record ProfileRebuildResult(
            Long userId,
            int sourceMovieCount,
            int insertedFeatureCount,
            Map<String, Integer> featureCounts
    ) {
    }

    private record FeatureEvidence(int movieCount, double signalWeightSum) {
        private static FeatureEvidence empty() {
            return new FeatureEvidence(0, 0.0);
        }

        private FeatureEvidence add(double signalWeight) {
            return new FeatureEvidence(movieCount + 1, signalWeightSum + signalWeight);
        }
    }

    private record MovieSignalWeights(double positiveWeight, double negativeWeight) {
        private static MovieSignalWeights empty() {
            return new MovieSignalWeights(0.0, 0.0);
        }

        private MovieSignalWeights addPositive(double weight) {
            return new MovieSignalWeights(positiveWeight + Math.max(0.0, weight), negativeWeight);
        }

        private MovieSignalWeights addWatchedSignal(double watchedSignal) {
            if (watchedSignal > 0.0) {
                return new MovieSignalWeights(positiveWeight + watchedSignal, negativeWeight);
            }
            if (watchedSignal < 0.0) {
                return new MovieSignalWeights(positiveWeight, negativeWeight + watchedSignal);
            }
            return this;
        }

        private boolean hasAnySignal() {
            return positiveWeight > 0.0 || negativeWeight < 0.0;
        }
    }
}
