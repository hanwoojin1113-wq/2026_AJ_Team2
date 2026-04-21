package com.cinematch.recommendation;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
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
    /*
     * 이 서비스는 사용자의 명시적 행동을 "취향 프로필"로 압축한다.
     * 영화 단위 행동을 그대로 쓰지 않고, 그 영화들에 공통으로 나타난
     * 태그/장르/감독/배우/키워드/OTT feature를 score로 누적해 user_preference_profile에 저장한다.
     */

    private final JdbcTemplate jdbcTemplate;
    private final RecommendationFeaturePolicy recommendationFeaturePolicy;

    public UserPreferenceProfileService(
            JdbcTemplate jdbcTemplate,
            RecommendationFeaturePolicy recommendationFeaturePolicy
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.recommendationFeaturePolicy = recommendationFeaturePolicy;
    }

    public ProfileRebuildResult rebuildProfile(Long userId) {
        // 프로필은 파생 데이터라서 매번 재생성하는 편이 규칙 변경과 디버깅에 유리하다.
        initializeProfileTable();

        Map<Long, Double> signalWeightsByMovie = loadSignalWeights(userId);
        jdbcTemplate.update("""
                DELETE FROM user_preference_profile
                WHERE user_id = ?
                """, userId);

        if (signalWeightsByMovie.isEmpty()) {
            return new ProfileRebuildResult(userId, 0, 0, Map.of());
        }

        LocalDateTime updatedAt = LocalDateTime.now();
        Map<String, Integer> featureCounts = new LinkedHashMap<>();
        int insertedRows = 0;

        // TAG는 가장 핵심 feature라서 별도 multiplier와 limit를 가장 보수적으로 준다.
        insertedRows += saveFeatureScores(userId, "TAG",
                aggregateFeatureScores(signalWeightsByMovie,
                        loadMovieTagFeatures(signalWeightsByMovie.keySet(), false),
                        recommendationFeaturePolicy.profileMultiplier("TAG"),
                        feature -> 1.0),
                updatedAt, featureCounts);

        // CAUTION은 "좋아함"이 아니라 회피 경향을 담는 보조 profile이다.
        insertedRows += saveFeatureScores(userId, "CAUTION",
                aggregateFeatureScores(signalWeightsByMovie,
                        loadMovieTagFeatures(signalWeightsByMovie.keySet(), true),
                        recommendationFeaturePolicy.profileMultiplier("CAUTION"),
                        feature -> 1.0),
                updatedAt, featureCounts);

        // 장르는 broad genre 확산을 줄이기 위해 feature별 감쇠(weight)를 한 번 더 적용한다.
        insertedRows += saveFeatureScores(userId, "GENRE",
                aggregateFeatureScores(signalWeightsByMovie,
                        loadNamedFeatures(signalWeightsByMovie.keySet(), """
                                SELECT mg.movie_id, g.name AS feature_name
                                FROM movie_genre mg
                                JOIN genre g ON g.id = mg.genre_id
                                WHERE mg.movie_id IN (%s)
                                """),
                        recommendationFeaturePolicy.profileMultiplier("GENRE"),
                        feature -> recommendationFeaturePolicy.featureSpecificWeight("GENRE", feature)),
                updatedAt, featureCounts);

        insertedRows += saveFeatureScores(userId, "DIRECTOR",
                aggregateFeatureScores(signalWeightsByMovie,
                        loadNamedFeatures(signalWeightsByMovie.keySet(), """
                                SELECT md.movie_id, p.name AS feature_name
                                FROM movie_director md
                                JOIN person p ON p.id = md.person_id
                                WHERE md.movie_id IN (%s)
                                """),
                        recommendationFeaturePolicy.profileMultiplier("DIRECTOR"),
                        feature -> 1.0),
                updatedAt, featureCounts);

        insertedRows += saveFeatureScores(userId, "ACTOR",
                aggregateFeatureScores(signalWeightsByMovie,
                        loadNamedFeatures(signalWeightsByMovie.keySet(), """
                                SELECT ma.movie_id, p.name AS feature_name
                                FROM movie_actor ma
                                JOIN person p ON p.id = ma.person_id
                                WHERE ma.movie_id IN (%s)
                                  AND ma.display_order <= %d
                                """.formatted("%s", recommendationFeaturePolicy.actorLimit())),
                        recommendationFeaturePolicy.profileMultiplier("ACTOR"),
                        feature -> 1.0),
                updatedAt, featureCounts);

        insertedRows += saveFeatureScores(userId, "KEYWORD",
                aggregateFeatureScores(signalWeightsByMovie,
                        loadNamedFeatures(signalWeightsByMovie.keySet(), """
                                SELECT mk.movie_id, k.name AS feature_name
                                FROM movie_keyword mk
                                JOIN keyword k ON k.id = mk.keyword_id
                                WHERE mk.movie_id IN (%s)
                                  AND mk.display_order <= %d
                                  AND LOWER(k.name) NOT IN (%s)
                                """.formatted("%s", recommendationFeaturePolicy.keywordLimit(),
                                        recommendationFeaturePolicy.keywordBlacklistSqlLiteralList())),
                        recommendationFeaturePolicy.profileMultiplier("KEYWORD"),
                        feature -> 1.0),
                updatedAt, featureCounts);

        insertedRows += saveFeatureScores(userId, "PROVIDER",
                aggregateFeatureScores(signalWeightsByMovie,
                        loadNamedFeatures(signalWeightsByMovie.keySet(), """
                                SELECT DISTINCT mp.movie_id, p.provider_name AS feature_name
                                FROM movie_provider mp
                                JOIN provider p ON p.id = mp.provider_id
                                WHERE mp.movie_id IN (%s)
                                  AND mp.provider_type = '%s'
                                  AND mp.region_code = '%s'
                                """.formatted("%s",
                                        recommendationFeaturePolicy.preferredProviderType(),
                                        recommendationFeaturePolicy.preferredProviderRegionCode())),
                        recommendationFeaturePolicy.profileMultiplier("PROVIDER"),
                        feature -> 1.0),
                updatedAt, featureCounts);

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

    private Map<Long, Double> loadSignalWeights(Long userId) {
        Map<Long, Double> signalWeightsByMovie = new LinkedHashMap<>();

        initializeWatchedSignalTable();

        // 강한 선호 신호부터 약한 신호 순서로 가중치를 합산한다.
        // 같은 영화에 여러 행동이 겹치면 점수가 누적되어 "이 영화가 취향 앵커"라는 의미가 강해진다.
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
                    signalWeightsByMovie.merge(
                            rs.getLong("movie_id"),
                            recommendationFeaturePolicy.watchedSignalWeight(rs.getObject("rating", Integer.class)),
                            Double::sum
                    );
                }, userId);

        mergeFixedSignal(signalWeightsByMovie, userId, """
                SELECT movie_id
                FROM user_movie_store
                WHERE user_id = ?
                """, recommendationFeaturePolicy.storeSignalWeight());

        return signalWeightsByMovie;
    }

    private void mergeFixedSignal(Map<Long, Double> signalWeightsByMovie, Long userId, String sql, double weight) {
        jdbcTemplate.query(sql, (org.springframework.jdbc.core.RowCallbackHandler) rs -> {
            signalWeightsByMovie.merge(
                    rs.getLong("movie_id"),
                    weight,
                    Double::sum
            );
        }, userId);
    }

    private void initializeWatchedSignalTable() {
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

    private Map<String, Double> aggregateFeatureScores(Map<Long, Double> signalWeightsByMovie,
                                                       Map<Long, List<String>> featuresByMovie,
                                                       double featureMultiplier,
                                                       ToDoubleFunction<String> featureWeightResolver) {
        Map<String, Double> scores = new LinkedHashMap<>();

        for (Map.Entry<Long, Double> entry : signalWeightsByMovie.entrySet()) {
            Long movieId = entry.getKey();
            List<String> features = featuresByMovie.get(movieId);
            if (features == null || features.isEmpty()) {
                continue;
            }

            // 한 영화의 행동 신호는 해당 영화에 달린 feature 개수만큼 분산해서 과도한 편향을 줄인다.
            double contribution = featureMultiplier * entry.getValue() / features.size();
            for (String feature : features) {
                double adjustedContribution = contribution * Math.max(0.0, featureWeightResolver.applyAsDouble(feature));
                if (adjustedContribution <= 0.0) {
                    continue;
                }
                scores.merge(feature, adjustedContribution, Double::sum);
            }
        }

        return scores;
    }

    private int saveFeatureScores(Long userId,
                                  String featureType,
                                  Map<String, Double> scores,
                                  LocalDateTime updatedAt,
                                  Map<String, Integer> featureCounts) {
        if (scores.isEmpty()) {
            featureCounts.put(featureType, 0);
            return 0;
        }

        List<Map.Entry<String, Double>> entries = scores.entrySet().stream()
                .filter(entry -> entry.getValue() > 0.0)
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed()
                        .thenComparing(Map.Entry::getKey))
                .limit(recommendationFeaturePolicy.profileFeatureLimit(featureType))
                .toList();

        // feature_type별 상위 일부만 저장해서 노이즈를 줄이고, 이후 랭킹 계산 비용도 함께 낮춘다.
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
}
