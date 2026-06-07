package com.cinematch.recommendation;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class CollaborativeLifeMovieRecommendationService {

    private static final double SIMILARITY_THRESHOLD = 0.45;
    private static final int MAX_SIMILAR_USERS = 5;
    private static final int MIN_LIFE_MOVIES = 3;
    private static final int MAX_RESULT_COUNT = 10;

    private static final String BLOCK_KEY = "COLLABORATIVE_LIFE";
    private static final String BLOCK_REASON = "취향이 비슷한 사용자가 인생영화로 고른 작품";

    private final JdbcTemplate jdbcTemplate;
    private final RecommendationMovieFilterService recommendationMovieFilterService;

    public CollaborativeLifeMovieRecommendationService(
            JdbcTemplate jdbcTemplate,
            RecommendationMovieFilterService recommendationMovieFilterService
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.recommendationMovieFilterService = recommendationMovieFilterService;
    }

    public Optional<CollaborativeLifeSection> buildSection(Long userId) {
        try {
            if (userId == null) {
                return Optional.empty();
            }

            // 후보 풀 = (1) 내가 팔로우하는 친구 + (2) 취향이 비슷한 사용자(코사인 유사도 >= threshold).
            // 둘을 합친 뒤, 인생영화를 MIN_LIFE_MOVIES개 이상 지정한 사용자만 남겨 그중 한 명을 랜덤으로 노출한다.
            Map<Long, Double> similarityByUser = new LinkedHashMap<>();

            Map<String, Double> currentProfile = loadPositiveProfile(userId);
            if (!currentProfile.isEmpty()) {
                for (SimilarUser similarUser : findSimilarUsers(userId, currentProfile)) {
                    similarityByUser.put(similarUser.userId(), similarUser.similarity());
                }
            }

            Set<Long> candidateUserIds = new LinkedHashSet<>(similarityByUser.keySet());
            candidateUserIds.addAll(loadFollowingUserIds(userId));

            List<Long> qualifiedUserIds = candidateUserIds.stream()
                    .filter(candidateId -> countLifeMovies(candidateId) >= MIN_LIFE_MOVIES)
                    .collect(Collectors.toList());
            if (qualifiedUserIds.isEmpty()) {
                return Optional.empty();
            }

            // 랜덤 한 명 선택: 셔플 후 섹션 구성이 가능한 첫 사용자를 사용.
            Collections.shuffle(qualifiedUserIds);
            for (Long candidateId : qualifiedUserIds) {
                double similarity = similarityByUser.getOrDefault(candidateId, 0.0);
                Optional<CollaborativeLifeSection> section =
                        buildRepresentativeSection(new SimilarUser(candidateId, similarity));
                if (section.isPresent()) {
                    return section;
                }
            }
            return Optional.empty();
        } catch (Exception ignored) {
            return Optional.empty();
        }
    }

    private List<Long> loadFollowingUserIds(Long userId) {
        return jdbcTemplate.query("""
                SELECT following_user_id
                FROM user_follow
                WHERE follower_user_id = ?
                """, (rs, rowNum) -> rs.getLong("following_user_id"), userId);
    }

    private int countLifeMovies(Long userId) {
        Integer count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM user_movie_life
                WHERE user_id = ?
                """, Integer.class, userId);
        return count == null ? 0 : count;
    }

    private Optional<CollaborativeLifeSection> buildRepresentativeSection(SimilarUser similarUser) {
        RepresentativeUser representativeUser = loadRepresentativeUser(similarUser.userId(), similarUser.similarity());
        if (representativeUser == null) {
            return Optional.empty();
        }

        // 인생영화를 MIN_LIFE_MOVIES개 이상 지정한 사용자만 노출. 내 활동 기반 중복 제외는 하지 않는다.
        List<Long> candidateMovieIds = loadLifeMovieIds(similarUser.userId());
        if (candidateMovieIds.size() < MIN_LIFE_MOVIES) {
            return Optional.empty();
        }

        Set<Long> recommendableMovieIds = recommendationMovieFilterService
                .filterRecommendableMovieIds(new LinkedHashSet<>(candidateMovieIds));
        if (recommendableMovieIds.isEmpty()) {
            return Optional.empty();
        }

        Map<Long, MovieCardMetadata> metadataByMovieId = loadMovieMetadata(candidateMovieIds);
        List<RecommendationBlockService.BlockMovie> items = candidateMovieIds.stream()
                .filter(recommendableMovieIds::contains)
                .map(metadataByMovieId::get)
                .filter(metadata -> metadata != null && metadata.movieCode() != null)
                .limit(MAX_RESULT_COUNT)
                .map(metadata -> metadata.toBlockMovie(BLOCK_REASON))
                .toList();

        if (items.isEmpty()) {
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

        String displayName = representativeUser.nickname() != null && !representativeUser.nickname().isBlank()
                ? representativeUser.nickname()
                : representativeUser.loginId();
        String title = displayName + "님의 인생영화";
        String description = "취향이 비슷한 사용자의 인생영화만 골라 보여드려요.";

        return Optional.of(new CollaborativeLifeSection(
                new RecommendationBlockService.RecommendationBlock(
                        BLOCK_KEY,
                        title,
                        description,
                        rankedItems
                ),
                representativeUser
        ));
    }

    private List<SimilarUser> findSimilarUsers(Long currentUserId, Map<String, Double> currentProfile) {
        Map<Long, Map<String, Double>> otherProfiles = loadOtherPositiveProfiles(currentUserId);
        return otherProfiles.entrySet().stream()
                .map(entry -> new SimilarUser(entry.getKey(), cosineSimilarity(currentProfile, entry.getValue())))
                .filter(similarUser -> similarUser.similarity() >= SIMILARITY_THRESHOLD)
                .sorted((left, right) -> {
                    int similarityCompare = Double.compare(right.similarity(), left.similarity());
                    if (similarityCompare != 0) {
                        return similarityCompare;
                    }
                    return Long.compare(left.userId(), right.userId());
                })
                .limit(MAX_SIMILAR_USERS)
                .toList();
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

    private List<Long> loadLifeMovieIds(Long userId) {
        return jdbcTemplate.query("""
                SELECT movie_id
                FROM user_movie_life
                WHERE user_id = ?
                ORDER BY created_at DESC, movie_id DESC
                """, (rs, rowNum) -> rs.getLong("movie_id"), userId);
    }

    private RepresentativeUser loadRepresentativeUser(Long userId, double similarity) {
        return jdbcTemplate.query("""
                SELECT id, login_id, nickname, profile_image_url
                FROM "USER"
                WHERE id = ?
                """, rs -> rs.next()
                ? new RepresentativeUser(
                        rs.getLong("id"),
                        rs.getString("login_id"),
                        rs.getString("nickname"),
                        rs.getString("profile_image_url"),
                        similarity
                )
                : null, userId);
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
                  AND m.poster_image_url IS NOT NULL
                  AND m.poster_image_url <> ''
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

    public record CollaborativeLifeSection(
            RecommendationBlockService.RecommendationBlock block,
            RepresentativeUser representativeUser
    ) {
    }

    public record RepresentativeUser(
            Long userId,
            String loginId,
            String nickname,
            String profileImageUrl,
            double similarity
    ) {
        public int similarityPercent() {
            return (int) Math.round(similarity * 100.0);
        }
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
