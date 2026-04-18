package movie.web.login.recommendation;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class RecommendationRefreshStateService {

    private final JdbcTemplate jdbcTemplate;
    private final RecommendationFeaturePolicy recommendationFeaturePolicy;

    public RecommendationRefreshStateService(
            JdbcTemplate jdbcTemplate,
            RecommendationFeaturePolicy recommendationFeaturePolicy
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.recommendationFeaturePolicy = recommendationFeaturePolicy;
    }

    public void markDirty(Long userId) {
        initializeRefreshStateTable();

        Integer existingCount = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM user_recommendation_refresh_state
                WHERE user_id = ?
                """, Integer.class, userId);

        if (existingCount != null && existingCount > 0) {
            jdbcTemplate.update("""
                    UPDATE user_recommendation_refresh_state
                    SET pending_event_count = pending_event_count + 1,
                        last_event_at = CURRENT_TIMESTAMP,
                        algorithm_version = ?,
                        dirty = TRUE
                    WHERE user_id = ?
                    """, RecommendationFeaturePolicy.ALGORITHM_VERSION, userId);
            return;
        }

        jdbcTemplate.update("""
                INSERT INTO user_recommendation_refresh_state (
                    user_id,
                    pending_event_count,
                    last_refreshed_at,
                    last_event_at,
                    last_refresh_event_count,
                    algorithm_version,
                    dirty
                )
                VALUES (?, 1, NULL, CURRENT_TIMESTAMP, 0, ?, TRUE)
                """, userId, RecommendationFeaturePolicy.ALGORITHM_VERSION);
    }

    public void markRefreshed(Long userId, String algorithmVersion) {
        initializeRefreshStateTable();

        Integer existingCount = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM user_recommendation_refresh_state
                WHERE user_id = ?
                """, Integer.class, userId);

        if (existingCount == null || existingCount == 0) {
            jdbcTemplate.update("""
                    INSERT INTO user_recommendation_refresh_state (
                        user_id,
                        pending_event_count,
                        last_refreshed_at,
                        last_event_at,
                        last_refresh_event_count,
                        algorithm_version,
                        dirty
                    )
                    VALUES (?, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0, ?, FALSE)
                    """, userId, algorithmVersion);
            return;
        }

        Integer pendingEventCount = jdbcTemplate.queryForObject("""
                SELECT pending_event_count
                FROM user_recommendation_refresh_state
                WHERE user_id = ?
                """, Integer.class, userId);

        jdbcTemplate.update("""
                UPDATE user_recommendation_refresh_state
                SET pending_event_count = 0,
                    last_refreshed_at = CURRENT_TIMESTAMP,
                    last_refresh_event_count = ?,
                    algorithm_version = ?,
                    dirty = FALSE
                WHERE user_id = ?
                """, pendingEventCount == null ? 0 : pendingEventCount, algorithmVersion, userId);
    }

    public RefreshState findState(Long userId) {
        initializeRefreshStateTable();
        return jdbcTemplate.query("""
                SELECT user_id, pending_event_count, last_refreshed_at, last_event_at,
                       last_refresh_event_count, algorithm_version, dirty
                FROM user_recommendation_refresh_state
                WHERE user_id = ?
                """, rs -> rs.next()
                ? new RefreshState(
                        rs.getLong("user_id"),
                        rs.getInt("pending_event_count"),
                        rs.getTimestamp("last_refreshed_at") == null ? null : rs.getTimestamp("last_refreshed_at").toLocalDateTime(),
                        rs.getTimestamp("last_event_at").toLocalDateTime(),
                        rs.getInt("last_refresh_event_count"),
                        rs.getString("algorithm_version"),
                        rs.getBoolean("dirty"))
                : null, userId);
    }

    public List<Long> findDirtyUserIds(int limit) {
        initializeRefreshStateTable();
        int normalizedLimit = Math.max(1, Math.min(limit, 200));
        return jdbcTemplate.query("""
                SELECT user_id
                FROM user_recommendation_refresh_state
                WHERE dirty = TRUE
                   OR algorithm_version <> ?
                ORDER BY last_event_at DESC, user_id ASC
                LIMIT ?
                """, (rs, rowNum) -> rs.getLong("user_id"),
                RecommendationFeaturePolicy.ALGORITHM_VERSION, normalizedLimit);
    }

    private void initializeRefreshStateTable() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS user_recommendation_refresh_state (
                    user_id BIGINT NOT NULL PRIMARY KEY,
                    pending_event_count INT NOT NULL DEFAULT 0,
                    last_refreshed_at TIMESTAMP,
                    last_event_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    last_refresh_event_count INT NOT NULL DEFAULT 0,
                    algorithm_version VARCHAR(50) NOT NULL,
                    dirty BOOLEAN NOT NULL DEFAULT TRUE,
                    CONSTRAINT fk_user_recommendation_refresh_state_user FOREIGN KEY (user_id) REFERENCES "USER"(id)
                )
                """);
        jdbcTemplate.execute("""
                CREATE INDEX IF NOT EXISTS idx_recommendation_refresh_state_dirty
                ON user_recommendation_refresh_state (dirty, last_event_at)
                """);
    }

    public record RefreshState(
            Long userId,
            int pendingEventCount,
            LocalDateTime lastRefreshedAt,
            LocalDateTime lastEventAt,
            int lastRefreshEventCount,
            String algorithmVersion,
            boolean dirty
    ) {
    }
}
