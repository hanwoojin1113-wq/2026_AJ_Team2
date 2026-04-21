package com.cinematch.recommendation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class RecommendationBlockService {
    /*
     * 블록 서비스는 "새 추천 엔진"이 아니라, 이미 계산된 전체 랭킹을 재사용하는 뷰 계층이다.
     * 먼저 user_recommendation_result 상위 slice를 가져오고,
     * 그 안에서 태그/장르/배우/감독/OTT feature로 다시 묶어 홈 화면 row를 만든다.
     */

    private final JdbcTemplate jdbcTemplate;
    private final RecommendationFeaturePolicy recommendationFeaturePolicy;

    public RecommendationBlockService(
            JdbcTemplate jdbcTemplate,
            RecommendationFeaturePolicy recommendationFeaturePolicy
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.recommendationFeaturePolicy = recommendationFeaturePolicy;
    }

    public RecommendationBlockResponse buildBlocks(Long userId) {
        return buildBlocks(userId, recommendationFeaturePolicy.blockSliceLimit(), recommendationFeaturePolicy.blockItemLimit());
    }

    public RecommendationBlockResponse buildBlocks(Long userId, Integer sliceLimit, Integer itemLimit) {
        int normalizedSliceLimit = Math.max(20, Math.min(sliceLimit == null ? recommendationFeaturePolicy.blockSliceLimit() : sliceLimit, 160));
        int normalizedItemLimit = Math.max(4, Math.min(itemLimit == null ? recommendationFeaturePolicy.blockItemLimit() : itemLimit, 12));

        // tail 구간 잡음을 줄이기 위해 전체 랭킹 일부(slice)만 블록 후보로 사용한다.
        List<RankedMovie> rankingSlice = loadRankingSlice(userId, normalizedSliceLimit);
        if (rankingSlice.isEmpty()) {
            return new RecommendationBlockResponse(userId, normalizedSliceLimit, normalizedItemLimit, 0, List.of());
        }

        Set<Long> movieIds = rankingSlice.stream()
                .map(RankedMovie::movieId)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Map<Long, Set<String>> tagsByMovie = loadNamedFeatureSetMap(movieIds, """
                SELECT mt.movie_id, t.tag_name AS feature_name
                FROM movie_tag mt
                JOIN tag t ON t.id = mt.tag_id
                WHERE mt.movie_id IN (%s)
                  AND t.tag_type <> 'CAUTION'
                """);
        Map<Long, Set<String>> genresByMovie = loadNamedFeatureSetMap(movieIds, """
                SELECT mg.movie_id, g.name AS feature_name
                FROM movie_genre mg
                JOIN genre g ON g.id = mg.genre_id
                WHERE mg.movie_id IN (%s)
                """);
        Map<Long, Set<String>> directorsByMovie = loadNamedFeatureSetMap(movieIds, """
                SELECT md.movie_id, p.name AS feature_name
                FROM movie_director md
                JOIN person p ON p.id = md.person_id
                WHERE md.movie_id IN (%s)
                """);
        Map<Long, Set<String>> actorsByMovie = loadNamedFeatureSetMap(movieIds, """
                SELECT ma.movie_id, p.name AS feature_name
                FROM movie_actor ma
                JOIN person p ON p.id = ma.person_id
                WHERE ma.movie_id IN (%s)
                  AND ma.display_order <= %d
                """.formatted("%s", recommendationFeaturePolicy.actorLimit()));
        Map<Long, Set<String>> providersByMovie = loadNamedFeatureSetMap(movieIds, """
                SELECT DISTINCT mp.movie_id, p.provider_name AS feature_name
                FROM movie_provider mp
                JOIN provider p ON p.id = mp.provider_id
                WHERE mp.movie_id IN (%s)
                  AND mp.provider_type = '%s'
                  AND mp.region_code = '%s'
                """.formatted("%s",
                recommendationFeaturePolicy.preferredProviderType(),
                recommendationFeaturePolicy.preferredProviderRegionCode()));

        Map<String, Double> tagProfileScores = loadProfileScores(userId, "TAG");
        Map<String, Double> genreProfileScores = loadProfileScores(userId, "GENRE");
        Map<String, Double> directorProfileScores = loadProfileScores(userId, "DIRECTOR");
        Map<String, Double> actorProfileScores = loadProfileScores(userId, "ACTOR");
        Map<String, Double> providerProfileScores = loadProfileScores(userId, "PROVIDER");

        List<RecommendationBlock> blocks = new ArrayList<>();
        // 기본 블록은 전체 랭킹 상위 그대로 노출하는 personalized 블록이다.
        blocks.add(new RecommendationBlock(
                "PERSONALIZED",
                "회원님을 위한 추천",
                "개인화 랭킹 상위 영화",
                rankingSlice.stream()
                        .limit(normalizedItemLimit)
                        .map(RankedMovie::toBlockMovie)
                        .toList()
        ));

        addFeatureBlocks(blocks, "TAG", rankingSlice, tagsByMovie, tagProfileScores, normalizedItemLimit, 2);
        addFeatureBlocks(blocks, "GENRE", rankingSlice, genresByMovie, genreProfileScores, normalizedItemLimit, 1);
        addFeatureBlocks(blocks, "DIRECTOR", rankingSlice, directorsByMovie, directorProfileScores, normalizedItemLimit, 1);
        addFeatureBlocks(blocks, "ACTOR", rankingSlice, actorsByMovie, actorProfileScores, normalizedItemLimit, 1);
        addFeatureBlocks(blocks, "PROVIDER", rankingSlice, providersByMovie, providerProfileScores, normalizedItemLimit, 1);

        return new RecommendationBlockResponse(userId, normalizedSliceLimit, normalizedItemLimit, blocks.size(), blocks);
    }

    private void addFeatureBlocks(
            List<RecommendationBlock> blocks,
            String featureType,
            List<RankedMovie> rankingSlice,
            Map<Long, Set<String>> featuresByMovie,
            Map<String, Double> profileScores,
            int itemLimit,
            int maxBlocks
    ) {
        // feature별 후보를 모으되, 프로필 점수 + 랭킹 내 표본 수를 함께 보고 대표 블록만 추린다.
        List<FeatureAggregate> candidates = collectFeatureAggregates(rankingSlice, featuresByMovie, profileScores).stream()
                .filter(candidate -> isBlockCandidateAllowed(featureType, candidate))
                .sorted(Comparator
                        .comparingDouble(FeatureAggregate::profileScore).reversed()
                        .thenComparing(Comparator.comparingInt(FeatureAggregate::sampleCount).reversed())
                        .thenComparingDouble(FeatureAggregate::averageRank))
                .limit(maxBlocks)
                .toList();

        for (FeatureAggregate candidate : candidates) {
            List<BlockMovie> items = rankingSlice.stream()
                    .filter(movie -> featuresByMovie.getOrDefault(movie.movieId(), Collections.emptySet()).contains(candidate.featureName()))
                    .limit(itemLimit)
                    .map(RankedMovie::toBlockMovie)
                    .toList();
            if (items.size() < recommendationFeaturePolicy.blockMinimumSize()) {
                continue;
            }

            blocks.add(new RecommendationBlock(
                    featureType + ":" + candidate.featureName(),
                    blockTitle(featureType, candidate.featureName()),
                    blockDescription(featureType, candidate.featureName()),
                    items
            ));
        }
    }

    private boolean isBlockCandidateAllowed(String featureType, FeatureAggregate candidate) {
        // 표본이 너무 적은 블록은 숨기고, broad genre는 더 엄격한 기준을 적용한다.
        if (candidate.sampleCount() < recommendationFeaturePolicy.blockMinimumSize()) {
            return false;
        }
        if ("GENRE".equals(featureType)) {
            return recommendationFeaturePolicy.isGenreBlockAllowed(candidate.featureName(), candidate.sampleCount());
        }
        return true;
    }

    private List<FeatureAggregate> collectFeatureAggregates(
            List<RankedMovie> rankingSlice,
            Map<Long, Set<String>> featuresByMovie,
            Map<String, Double> profileScores
    ) {
        Map<String, MutableAggregate> aggregates = new LinkedHashMap<>();

        for (RankedMovie movie : rankingSlice) {
            for (String featureName : featuresByMovie.getOrDefault(movie.movieId(), Collections.emptySet())) {
                MutableAggregate aggregate = aggregates.computeIfAbsent(featureName, ignored -> new MutableAggregate(featureName));
                aggregate.sampleCount++;
                aggregate.totalRank += movie.rankNo();
                aggregate.profileScore = Math.max(aggregate.profileScore, profileScores.getOrDefault(featureName, 0.0));
            }
        }

        return aggregates.values().stream()
                .map(value -> new FeatureAggregate(
                        value.featureName,
                        value.sampleCount,
                        value.totalRank / (double) value.sampleCount,
                        value.profileScore
                ))
                .toList();
    }

    private List<RankedMovie> loadRankingSlice(Long userId, int sliceLimit) {
        return jdbcTemplate.query("""
                SELECT
                    urr.movie_id,
                    urr.rank_no,
                    COALESCE(m.title, m.movie_name) AS display_title,
                    COALESCE(m.movie_name_en, m.original_title, m.movie_name_original, m.movie_cd) AS display_subtitle,
                    m.movie_cd,
                    m.poster_image_url,
                    urr.reason_summary
                FROM user_recommendation_result urr
                JOIN movie m ON m.id = urr.movie_id
                WHERE urr.user_id = ?
                ORDER BY urr.rank_no
                LIMIT ?
                """, (rs, rowNum) -> new RankedMovie(
                rs.getLong("movie_id"),
                rs.getInt("rank_no"),
                rs.getString("display_title"),
                rs.getString("display_subtitle"),
                rs.getString("movie_cd"),
                rs.getString("poster_image_url"),
                rs.getString("reason_summary")
        ), userId, sliceLimit);
    }

    private Map<String, Double> loadProfileScores(Long userId, String featureType) {
        Map<String, Double> scores = new LinkedHashMap<>();
        jdbcTemplate.query("""
                SELECT feature_name, score
                FROM user_preference_profile
                WHERE user_id = ?
                  AND feature_type = ?
                ORDER BY score DESC, feature_name ASC
                """, (org.springframework.jdbc.core.RowCallbackHandler) rs ->
                scores.put(rs.getString("feature_name"), rs.getDouble("score")), userId, featureType);
        return scores;
    }

    private Map<Long, Set<String>> loadNamedFeatureSetMap(Set<Long> movieIds, String template) {
        if (movieIds.isEmpty()) {
            return Collections.emptyMap();
        }

        String sql = template.formatted(placeholders(movieIds.size()));
        Map<Long, Set<String>> featuresByMovie = new LinkedHashMap<>();
        List<Object> params = new ArrayList<>(movieIds);

        jdbcTemplate.query(sql, (org.springframework.jdbc.core.RowCallbackHandler) rs -> {
            long movieId = rs.getLong("movie_id");
            String featureName = rs.getString("feature_name");
            if (featureName == null || featureName.isBlank()) {
                return;
            }
            featuresByMovie.computeIfAbsent(movieId, ignored -> new LinkedHashSet<>()).add(featureName);
        }, params.toArray());

        return featuresByMovie;
    }

    private String blockTitle(String featureType, String featureName) {
        return switch (featureType) {
            case "TAG" -> recommendationFeaturePolicy.tagBlockTitle(featureName);
            case "GENRE" -> featureName + " 영화 추천";
            case "DIRECTOR" -> featureName + " 감독 작품 추천";
            case "ACTOR" -> featureName + " 출연작 추천";
            case "PROVIDER" -> featureName + "에서 볼 수 있는 추천";
            default -> featureName + " 추천";
        };
    }

    private String blockDescription(String featureType, String featureName) {
        return switch (featureType) {
            case "TAG" -> "전체 개인화 랭킹 상위 결과에서 추린 태그 블록";
            case "GENRE" -> "개인화 랭킹 기반 장르 블록";
            case "DIRECTOR" -> "선호 감독과 연결되는 작품";
            case "ACTOR" -> "선호 배우와 연결되는 작품";
            case "PROVIDER" -> "선호 OTT 제공 작품";
            default -> featureName;
        };
    }

    private String placeholders(int count) {
        return java.util.stream.IntStream.range(0, count)
                .mapToObj(index -> "?")
                .collect(Collectors.joining(", "));
    }

    private static final class MutableAggregate {
        private final String featureName;
        private int sampleCount;
        private int totalRank;
        private double profileScore;

        private MutableAggregate(String featureName) {
            this.featureName = featureName;
        }
    }

    private record FeatureAggregate(
            String featureName,
            int sampleCount,
            double averageRank,
            double profileScore
    ) {
    }

    private record RankedMovie(
            Long movieId,
            int rankNo,
            String displayTitle,
            String displaySubtitle,
            String movieCode,
            String posterImageUrl,
            String reasonSummary
    ) {
        private BlockMovie toBlockMovie() {
            return new BlockMovie(rankNo, movieCode, displayTitle, displaySubtitle, posterImageUrl, reasonSummary);
        }
    }

    public record BlockMovie(
            int rankNo,
            String movieCode,
            String title,
            String subtitle,
            String posterImageUrl,
            String reasonSummary
    ) {
    }

    public record RecommendationBlock(
            String key,
            String title,
            String description,
            List<BlockMovie> items
    ) {
    }

    public record RecommendationBlockResponse(
            Long userId,
            int sliceLimit,
            int itemLimit,
            int blockCount,
            List<RecommendationBlock> blocks
    ) {
    }
}
