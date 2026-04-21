package com.cinematch.recommendation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.cinematch.admin.DummyUserSeedService;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class RecommendationValidationService {

    private final JdbcTemplate jdbcTemplate;
    private final DummyUserSeedService dummyUserSeedService;
    private final RecommendationMaintenanceService recommendationMaintenanceService;
    private final RecommendationRefreshStateService recommendationRefreshStateService;
    private final RecommendationBlockService recommendationBlockService;

    public RecommendationValidationService(
            JdbcTemplate jdbcTemplate,
            DummyUserSeedService dummyUserSeedService,
            RecommendationMaintenanceService recommendationMaintenanceService,
            RecommendationRefreshStateService recommendationRefreshStateService,
            RecommendationBlockService recommendationBlockService
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.dummyUserSeedService = dummyUserSeedService;
        this.recommendationMaintenanceService = recommendationMaintenanceService;
        this.recommendationRefreshStateService = recommendationRefreshStateService;
        this.recommendationBlockService = recommendationBlockService;
    }

    public ValidationResponse inspectTestUsers(String loginId, boolean refreshIfDirty, Integer topRecommendationLimit) {
        Map<String, DummyUserSeedService.DummyUserPersona> personasByLogin = dummyUserSeedService.catalog().stream()
                .collect(Collectors.toMap(
                        DummyUserSeedService.DummyUserPersona::loginId,
                        persona -> persona,
                        (left, right) -> left,
                        LinkedHashMap::new
                ));

        List<String> targetLoginIds = loginId == null || loginId.isBlank()
                ? new ArrayList<>(personasByLogin.keySet())
                : List.of(loginId);

        List<UserValidationSnapshot> users = new ArrayList<>();
        int normalizedTopRecommendationLimit = Math.max(3, Math.min(topRecommendationLimit == null ? 6 : topRecommendationLimit, 12));

        for (String targetLoginId : targetLoginIds) {
            DummyUserSeedService.DummyUserPersona persona = personasByLogin.get(targetLoginId);
            if (persona == null) {
                continue;
            }

            Long userId = findUserId(targetLoginId);
            if (userId == null) {
                users.add(UserValidationSnapshot.missing(persona));
                continue;
            }

            if (refreshIfDirty) {
                recommendationMaintenanceService.ensureRecommendations(userId, 200);
            }

            RecommendationRefreshStateService.RefreshState refreshState = recommendationRefreshStateService.findState(userId);
            users.add(new UserValidationSnapshot(
                    targetLoginId,
                    persona.nickname(),
                    persona.preferenceTypeSummary(),
                    new ActivityCount(
                            countRows("user_movie_like", userId),
                            countRows("user_movie_store", userId),
                            countRows("user_movie_life", userId),
                            countRows("user_movie_watched", userId)
                    ),
                    refreshState == null ? null : new RefreshStateSummary(
                            refreshState.dirty(),
                            refreshState.pendingEventCount(),
                            refreshState.algorithmVersion(),
                            refreshState.lastRefreshedAt(),
                            refreshState.lastEventAt()
                    ),
                    loadTopProfileFeatures(userId),
                    loadTopRecommendations(userId, normalizedTopRecommendationLimit),
                    summarizeBlocks(userId)
            ));
        }

        return new ValidationResponse(
                refreshIfDirty,
                normalizedTopRecommendationLimit,
                users.size(),
                users
        );
    }

    private Long findUserId(String loginId) {
        return jdbcTemplate.query("""
                SELECT id
                FROM "USER"
                WHERE login_id = ?
                """, rs -> rs.next() ? rs.getLong("id") : null, loginId);
    }

    private int countRows(String tableName, Long userId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM " + tableName + " WHERE user_id = ?",
                Integer.class,
                userId
        );
        return count == null ? 0 : count;
    }

    private Map<String, List<ProfileFeature>> loadTopProfileFeatures(Long userId) {
        Map<String, List<ProfileFeature>> profileByType = new LinkedHashMap<>();
        jdbcTemplate.query("""
                SELECT feature_type, feature_name, score
                FROM user_preference_profile
                WHERE user_id = ?
                ORDER BY feature_type ASC, score DESC, feature_name ASC
                """, rs -> {
            String featureType = rs.getString("feature_type");
            List<ProfileFeature> bucket = profileByType.computeIfAbsent(featureType, ignored -> new ArrayList<>());
            if (bucket.size() < 3) {
                bucket.add(new ProfileFeature(rs.getString("feature_name"), rs.getDouble("score")));
            }
        }, userId);
        return profileByType;
    }

    private List<TopRecommendation> loadTopRecommendations(Long userId, int limit) {
        return jdbcTemplate.query("""
                SELECT
                    urr.rank_no,
                    COALESCE(m.title, m.movie_name) AS display_title,
                    urr.final_score,
                    urr.reason_summary
                FROM user_recommendation_result urr
                JOIN movie m ON m.id = urr.movie_id
                WHERE urr.user_id = ?
                ORDER BY urr.rank_no
                LIMIT ?
                """, (rs, rowNum) -> new TopRecommendation(
                rs.getInt("rank_no"),
                rs.getString("display_title"),
                rs.getDouble("final_score"),
                rs.getString("reason_summary")
        ), userId, limit);
    }

    private List<BlockSummary> summarizeBlocks(Long userId) {
        return recommendationBlockService.buildBlocks(userId, 120, 6).blocks().stream()
                .map(block -> new BlockSummary(
                        block.key(),
                        block.title(),
                        block.items().size(),
                        block.items().stream().limit(3).map(RecommendationBlockService.BlockMovie::title).toList()
                ))
                .toList();
    }

    public record ValidationResponse(
            boolean refreshIfDirty,
            int topRecommendationLimit,
            int userCount,
            List<UserValidationSnapshot> users
    ) {
    }

    public record UserValidationSnapshot(
            String loginId,
            String nickname,
            String preferenceTypeSummary,
            ActivityCount activityCount,
            RefreshStateSummary refreshState,
            Map<String, List<ProfileFeature>> topProfileFeatures,
            List<TopRecommendation> recommendations,
            List<BlockSummary> blocks
    ) {
        private static UserValidationSnapshot missing(DummyUserSeedService.DummyUserPersona persona) {
            return new UserValidationSnapshot(
                    persona.loginId(),
                    persona.nickname(),
                    persona.preferenceTypeSummary(),
                    new ActivityCount(0, 0, 0, 0),
                    null,
                    Map.of(),
                    List.of(),
                    List.of()
            );
        }
    }

    public record ActivityCount(int likeCount, int storeCount, int lifeCount, int watchedCount) {
    }

    public record RefreshStateSummary(
            boolean dirty,
            int pendingEventCount,
            String algorithmVersion,
            java.time.LocalDateTime lastRefreshedAt,
            java.time.LocalDateTime lastEventAt
    ) {
    }

    public record ProfileFeature(String featureName, double score) {
    }

    public record TopRecommendation(
            int rankNo,
            String title,
            double finalScore,
            String reasonSummary
    ) {
    }

    public record BlockSummary(
            String key,
            String title,
            int itemCount,
            List<String> previewTitles
    ) {
    }
}
