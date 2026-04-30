package com.cinematch.recommendation;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class CollaborativeLifeMovieRecommendationService {

    private static final double SIMILARITY_THRESHOLD = 0.55;
    private static final int MAX_SIMILAR_USERS = 5;
    private static final int MIN_CANDIDATE_COUNT = 3;
    private static final int MAX_RESULT_COUNT = 10;

    private static final String BLOCK_KEY = "COLLABORATIVE_LIFE";
    private static final String BLOCK_TITLE = "나와 취향이 비슷한 사람들이 고른 인생영화";
    private static final String BLOCK_DESCRIPTION = "취향 프로필이 비슷한 사용자가 인생영화로 고른 작품만 모은 추천입니다.";
    private static final String BLOCK_REASON = "유사 취향 사용자들이 선택한 인생영화";

    private final JdbcTemplate jdbcTemplate;
    private final RecommendationMovieFilterService recommendationMovieFilterService;

    public CollaborativeLifeMovieRecommendationService(
            JdbcTemplate jdbcTemplate,
            RecommendationMovieFilterService recommendationMovieFilterService
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.recommendationMovieFilterService = recommendationMovieFilterService;
    }

    public Optional<RecommendationBlockService.RecommendationBlock> buildBlock(Long userId) {
        try {
            if (userId == null) {
                return Optional.empty();
            }

            Map<String, Double> currentProfile = loadPositiveProfile(userId);
            if (currentProfile.isEmpty()) {
                return Optional.empty();
            }

            List<SimilarUser> similarUsers = findSimilarUsers(userId, currentProfile);
            if (similarUsers.isEmpty()) {
                return Optional.empty();
            }

            Map<Long, CandidateScore> candidateScores = collectLifeMovieScores(similarUsers);
            if (candidateScores.isEmpty()) {
                return Optional.empty();
            }

            candidateScores.keySet().removeAll(loadExcludedMovieIds(userId));
            if (candidateScores.isEmpty()) {
                return Optional.empty();
            }

            Set<Long> recommendableMovieIds =
                    recommendationMovieFilterService.filterRecommendableMovieIds(candidateScores.keySet());
            candidateScores.keySet().retainAll(recommendableMovieIds);
            if (candidateScores.size() < MIN_CANDIDATE_COUNT) {
                return Optional.empty();
            }

            Map<Long, MovieCardMetadata> metadataByMovieId = loadMovieMetadata(candidateScores.keySet());
            List<RecommendationBlockService.BlockMovie> items = candidateScores.entrySet().stream()
                    .filter(entry -> metadataByMovieId.containsKey(entry.getKey()))
                    .sorted((left, right) -> compareCandidates(
                            left.getKey(),
                            left.getValue(),
                            right.getKey(),
                            right.getValue(),
                            metadataByMovieId
                    ))
                    .limit(MAX_RESULT_COUNT)
                    .map(Map.Entry::getKey)
                    .map(metadataByMovieId::get)
                    .filter(metadata -> metadata != null && metadata.movieCode() != null)
                    .toList()
                    .stream()
                    .map(metadata -> metadata.toBlockMovie(BLOCK_REASON))
                    .toList();

            if (items.size() < MIN_CANDIDATE_COUNT) {
                return Optional.empty();
            }

            List<RecommendationBlockService.BlockMovie> rankedItems = new ArrayList<>();
            for (int index = 0; index < items.size(); index++) {
                RecommendationBlockService.BlockMovie item = items.get(index);
                rankedItems.add(new RecommendationBlockService.BlockMovie(
                        index + 1,
                        item.movieCode(),
                        item.title(),
                        item.subtitle(),
                        item.posterImageUrl(),
                        item.reasonSummary()
                ));
            }

            return Optional.of(new RecommendationBlockService.RecommendationBlock(
                    BLOCK_KEY,
                    BLOCK_TITLE,
                    BLOCK_DESCRIPTION,
                    rankedItems
            ));
        } catch (Exception ignored) {
            return Optional.empty();
        }
    }

    private List<SimilarUser> findSimilarUsers(Long currentUserId, Map<String, Double> currentProfile) {
        Map<Long, Map<String, Double>> otherProfiles = loadOtherPositiveProfiles(currentUserId);
        return otherProfiles.entrySet().stream()
                .map(entry -> new SimilarUser(entry.getKey(), cosineSimilarity(currentProfile, entry.getValue())))
                .filter(similarUser -> similarUser.similarity() >= SIMILARITY_THRESHOLD)
                .sorted(Comparator.comparingDouble(SimilarUser::similarity).reversed()
                        .thenComparing(SimilarUser::userId))
                .limit(MAX_SIMILAR_USERS)
                .toList();
    }

    private Map<Long, CandidateScore> collectLifeMovieScores(List<SimilarUser> similarUsers) {
        if (similarUsers.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<Long, Double> similarityByUserId = similarUsers.stream()
                .collect(Collectors.toMap(
                        SimilarUser::userId,
                        SimilarUser::similarity,
                        (left, right) -> left,
                        LinkedHashMap::new
                ));

        Map<Long, CandidateScore> scoresByMovieId = new LinkedHashMap<>();
        List<Object> params = new ArrayList<>(similarityByUserId.keySet());
        jdbcTemplate.query("""
                SELECT user_id, movie_id
                FROM user_movie_life
                WHERE user_id IN (%s)
                """.formatted(placeholders(similarityByUserId.size())), rs -> {
            long userId = rs.getLong("user_id");
            long movieId = rs.getLong("movie_id");
            double similarity = similarityByUserId.getOrDefault(userId, 0.0);
            if (similarity <= 0.0) {
                return;
            }
            CandidateScore score = scoresByMovieId.computeIfAbsent(movieId, ignored -> new CandidateScore());
            score.totalScore += similarity;
            score.supportCount += 1;
        }, params.toArray());

        return scoresByMovieId;
    }

    private Map<String, Double> loadPositiveProfile(Long userId) {
        Map<String, Double> profile = new LinkedHashMap<>();
        jdbcTemplate.query("""
                SELECT feature_type, feature_name, score
                FROM user_preference_profile
                WHERE user_id = ?
                  AND feature_type <> 'CAUTION'
                  AND score > 0
                """, (org.springframework.jdbc.core.RowCallbackHandler) rs -> profile.put(
                vectorKey(rs.getString("feature_type"), rs.getString("feature_name")),
                rs.getDouble("score")
        ), userId);
        return profile;
    }

    private Map<Long, Map<String, Double>> loadOtherPositiveProfiles(Long currentUserId) {
        Map<Long, Map<String, Double>> profiles = new LinkedHashMap<>();
        jdbcTemplate.query("""
                SELECT upp.user_id, upp.feature_type, upp.feature_name, upp.score
                FROM user_preference_profile upp
                WHERE upp.user_id <> ?
                  AND upp.feature_type <> 'CAUTION'
                  AND upp.score > 0
                  AND EXISTS (
                        SELECT 1
                        FROM user_movie_life uml
                        WHERE uml.user_id = upp.user_id
                  )
                """, (org.springframework.jdbc.core.RowCallbackHandler) rs -> profiles
                .computeIfAbsent(rs.getLong("user_id"), ignored -> new LinkedHashMap<>())
                .put(vectorKey(rs.getString("feature_type"), rs.getString("feature_name")), rs.getDouble("score")), currentUserId);
        return profiles;
    }

    private Set<Long> loadExcludedMovieIds(Long userId) {
        Set<Long> excludedMovieIds = new LinkedHashSet<>();
        mergeMovieIds(excludedMovieIds, """
                SELECT movie_id
                FROM user_movie_life
                WHERE user_id = ?
                """, userId);
        mergeMovieIds(excludedMovieIds, """
                SELECT movie_id
                FROM user_movie_like
                WHERE user_id = ?
                """, userId);
        mergeMovieIds(excludedMovieIds, """
                SELECT movie_id
                FROM user_movie_store
                WHERE user_id = ?
                """, userId);
        mergeMovieIds(excludedMovieIds, """
                SELECT movie_id
                FROM user_movie_watched
                WHERE user_id = ?
                """, userId);
        return excludedMovieIds;
    }

    private void mergeMovieIds(Set<Long> movieIds, String sql, Long userId) {
        jdbcTemplate.query(sql, (org.springframework.jdbc.core.RowCallbackHandler) rs -> movieIds.add(rs.getLong("movie_id")), userId);
    }

    private Map<Long, MovieCardMetadata> loadMovieMetadata(Collection<Long> movieIds) {
        if (movieIds.isEmpty()) {
            return Collections.emptyMap();
        }

        List<Object> params = new ArrayList<>(movieIds);
        Map<Long, MovieCardMetadata> metadataByMovieId = new LinkedHashMap<>();
        jdbcTemplate.query("""
                SELECT
                    m.id,
                    m.movie_cd,
                    COALESCE(m.title, m.movie_name) AS title,
                    COALESCE(m.movie_name_en, m.original_title, m.movie_name_original, m.movie_cd) AS subtitle,
                    m.poster_image_url,
                    m.popularity,
                    COALESCE(m.release_date, m.movie_info_open_date, m.box_office_open_date) AS release_date
                FROM movie m
                WHERE m.id IN (%s)
                """.formatted(placeholders(movieIds.size())), (org.springframework.jdbc.core.RowCallbackHandler) rs -> metadataByMovieId.put(
                rs.getLong("id"),
                new MovieCardMetadata(
                        rs.getLong("id"),
                        rs.getString("movie_cd"),
                        rs.getString("title"),
                        rs.getString("subtitle"),
                        rs.getString("poster_image_url"),
                        rs.getObject("popularity", Double.class),
                        rs.getObject("release_date", LocalDate.class)
                )
        ), params.toArray());
        return metadataByMovieId;
    }

    private int compareCandidates(
            Long leftMovieId,
            CandidateScore leftScore,
            Long rightMovieId,
            CandidateScore rightScore,
            Map<Long, MovieCardMetadata> metadataByMovieId
    ) {
        int scoreCompare = Double.compare(rightScore.totalScore, leftScore.totalScore);
        if (scoreCompare != 0) {
            return scoreCompare;
        }

        int supportCompare = Integer.compare(rightScore.supportCount, leftScore.supportCount);
        if (supportCompare != 0) {
            return supportCompare;
        }

        MovieCardMetadata leftMetadata = metadataByMovieId.get(leftMovieId);
        MovieCardMetadata rightMetadata = metadataByMovieId.get(rightMovieId);

        int popularityCompare = Double.compare(
                rightMetadata == null || rightMetadata.popularity() == null ? 0.0 : rightMetadata.popularity(),
                leftMetadata == null || leftMetadata.popularity() == null ? 0.0 : leftMetadata.popularity()
        );
        if (popularityCompare != 0) {
            return popularityCompare;
        }

        LocalDate leftReleaseDate = leftMetadata == null ? null : leftMetadata.releaseDate();
        LocalDate rightReleaseDate = rightMetadata == null ? null : rightMetadata.releaseDate();
        if (leftReleaseDate == null && rightReleaseDate != null) {
            return 1;
        }
        if (leftReleaseDate != null && rightReleaseDate == null) {
            return -1;
        }
        if (leftReleaseDate != null) {
            int releaseCompare = rightReleaseDate.compareTo(leftReleaseDate);
            if (releaseCompare != 0) {
                return releaseCompare;
            }
        }

        return Long.compare(leftMovieId, rightMovieId);
    }

    private double cosineSimilarity(Map<String, Double> left, Map<String, Double> right) {
        if (left.isEmpty() || right.isEmpty()) {
            return 0.0;
        }

        double dotProduct = 0.0;
        for (Map.Entry<String, Double> entry : left.entrySet()) {
            Double otherScore = right.get(entry.getKey());
            if (otherScore != null) {
                dotProduct += entry.getValue() * otherScore;
            }
        }

        if (dotProduct <= 0.0) {
            return 0.0;
        }

        double leftNorm = vectorNorm(left);
        double rightNorm = vectorNorm(right);
        if (leftNorm == 0.0 || rightNorm == 0.0) {
            return 0.0;
        }

        return dotProduct / (leftNorm * rightNorm);
    }

    private double vectorNorm(Map<String, Double> vector) {
        double sumSquares = 0.0;
        for (double value : vector.values()) {
            sumSquares += value * value;
        }
        return Math.sqrt(sumSquares);
    }

    private String vectorKey(String featureType, String featureName) {
        return featureType + "|" + featureName;
    }

    private String placeholders(int count) {
        return java.util.stream.IntStream.range(0, count)
                .mapToObj(index -> "?")
                .collect(Collectors.joining(", "));
    }

    private static final class CandidateScore {
        private double totalScore;
        private int supportCount;
    }

    private record SimilarUser(Long userId, double similarity) {
    }

    private record MovieCardMetadata(
            Long movieId,
            String movieCode,
            String title,
            String subtitle,
            String posterImageUrl,
            Double popularity,
            LocalDate releaseDate
    ) {
        private RecommendationBlockService.BlockMovie toBlockMovie(String reasonSummary) {
            return new RecommendationBlockService.BlockMovie(
                    0,
                    movieCode,
                    title,
                    subtitle,
                    posterImageUrl,
                    reasonSummary
            );
        }
    }
}
