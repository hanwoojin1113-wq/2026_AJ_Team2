package com.cinematch.recommendation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class RecommendationMaintenanceService {
    /*
     * 추천 갱신의 진입점을 담당한다.
     * 활동이 생길 때마다 무거운 계산을 즉시 돌리지 않고,
     * dirty 상태를 보고 "지금 재계산이 필요한가"를 판단한 뒤
     * 프로필 재생성 -> 랭킹 재생성 순서로 실행한다.
     */

    private static final int DEFAULT_RANKING_LIMIT = 200;

    private final JdbcTemplate jdbcTemplate;
    private final UserPreferenceProfileService userPreferenceProfileService;
    private final RecommendationRankingService recommendationRankingService;
    private final RecommendationRefreshStateService recommendationRefreshStateService;

    public RecommendationMaintenanceService(
            JdbcTemplate jdbcTemplate,
            UserPreferenceProfileService userPreferenceProfileService,
            RecommendationRankingService recommendationRankingService,
            RecommendationRefreshStateService recommendationRefreshStateService
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.userPreferenceProfileService = userPreferenceProfileService;
        this.recommendationRankingService = recommendationRankingService;
        this.recommendationRefreshStateService = recommendationRefreshStateService;
    }

    @Transactional
    public EnsureRecommendationResult ensureRecommendations(Long userId, Integer rankingLimit) {
        RecommendationRefreshStateService.RefreshState refreshState =
                recommendationRefreshStateService.findState(userId);
        int resultCount = countSavedRecommendations(userId);
        // dirty거나, 결과가 없거나, 알고리즘 버전이 바뀐 경우에만 재계산한다.
        boolean needsRefresh = refreshState == null
                || refreshState.dirty()
                || resultCount == 0
                || !RecommendationFeaturePolicy.ALGORITHM_VERSION.equals(refreshState.algorithmVersion());

        if (!needsRefresh) {
            return new EnsureRecommendationResult(
                    userId,
                    false,
                    resultCount,
                    RecommendationFeaturePolicy.ALGORITHM_VERSION
            );
        }

        userPreferenceProfileService.rebuildProfile(userId);
        RecommendationRankingService.RankingRebuildResult rankingResult =
                recommendationRankingService.rebuildRanking(userId, normalizeRankingLimit(rankingLimit));

        return new EnsureRecommendationResult(
                userId,
                true,
                rankingResult.savedCount(),
                rankingResult.algorithmVersion()
        );
    }

    @Transactional
    public RefreshDirtyUsersResult refreshDirtyUsers(int batchSize, Integer rankingLimit) {
        // 관리자 배치에서는 dirty user만 골라 순차적으로 재계산한다.
        List<Long> dirtyUserIds = recommendationRefreshStateService.findDirtyUserIds(batchSize);
        List<EnsureRecommendationResult> refreshedUsers = new ArrayList<>();

        for (Long userId : dirtyUserIds) {
            refreshedUsers.add(ensureRecommendations(userId, rankingLimit));
        }

        return new RefreshDirtyUsersResult(
                dirtyUserIds.size(),
                normalizeRankingLimit(rankingLimit),
                refreshedUsers
        );
    }

    private int countSavedRecommendations(Long userId) {
        Integer count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM user_recommendation_result
                WHERE user_id = ?
                """, Integer.class, userId);
        return count == null ? 0 : count;
    }

    private int normalizeRankingLimit(Integer rankingLimit) {
        return rankingLimit == null ? DEFAULT_RANKING_LIMIT : rankingLimit;
    }

    public record EnsureRecommendationResult(
            Long userId,
            boolean refreshed,
            int savedRecommendationCount,
            String algorithmVersion
    ) {
    }

    public record RefreshDirtyUsersResult(
            int targetUserCount,
            int rankingLimit,
            List<EnsureRecommendationResult> users
    ) {
    }
}
