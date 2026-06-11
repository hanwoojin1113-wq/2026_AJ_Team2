package com.cinematch;

import com.cinematch.notification.NotificationService;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.FORBIDDEN;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@Service
public class MovieThrowService {

    private static final Set<String> ALLOWED_REACTIONS = Set.of("THANKS", "WILL_WATCH", "NOT_INTERESTED");

    private final JdbcTemplate jdbcTemplate;
    private final NotificationService notificationService;

    public MovieThrowService(JdbcTemplate jdbcTemplate, NotificationService notificationService) {
        this.jdbcTemplate = jdbcTemplate;
        this.notificationService = notificationService;
    }

    /**
     * 두 사용자의 취향 교집합 영화 최대 3개 반환.
     * 1차: user_recommendation_result JOIN 활용
     * 2차(fallback): user_preference_profile TAG 교집합 기반
     */
    public List<Map<String, Object>> getSuggestedMovies(Long userIdA, Long userIdB) {
        initializeThrowTable();
        requireFollowing(userIdA, userIdB);
        String primarySql =
            "SELECT m.id, m.movie_cd," +
            "       COALESCE(m.title, m.movie_name) AS title," +
            "       m.poster_image_url, m.production_year," +
            "       (r_a.final_score + r_b.final_score) / 2.0 AS combined_score" +
            " FROM user_recommendation_result r_a" +
            " JOIN user_recommendation_result r_b ON r_a.movie_id = r_b.movie_id AND r_b.user_id = ?" +
            " JOIN movie m ON m.id = r_a.movie_id" +
            " WHERE r_a.user_id = ?" +
            "   AND COALESCE(m.popularity, 0) >= 2.0" +
            "   AND m.id NOT IN (" +
            "       SELECT movie_id FROM user_movie_watched WHERE user_id IN (?, ?)" +
            "       UNION ALL" +
            "       SELECT movie_id FROM user_movie_life   WHERE user_id IN (?, ?)" +
            "   )" +
            "   AND m.id NOT IN (" +
            "       SELECT movie_id FROM movie_throw" +
            "       WHERE (sender_user_id = ? AND receiver_user_id = ?)" +
            "          OR (sender_user_id = ? AND receiver_user_id = ?)" +
            "   )" +
            " ORDER BY combined_score DESC LIMIT 10";

        List<Map<String, Object>> primary = jdbcTemplate.query(primarySql,
                (rs, rowNum) -> toMovieRow(rs),
                userIdB, userIdA,
                userIdA, userIdB, userIdA, userIdB,
                userIdA, userIdB, userIdB, userIdA);

        if (primary.size() >= 3) {
            return primary.subList(0, 3);
        }

        String fallbackSql =
            "SELECT * FROM (" +
            "SELECT m.id, m.movie_cd," +
            "       COALESCE(m.title, m.movie_name) AS title," +
            "       m.poster_image_url, m.production_year, m.popularity," +
            "       (" +
            "           (SELECT COUNT(*) FROM movie_tag mt" +
            "            JOIN tag t ON t.id = mt.tag_id" +
            "            JOIN user_preference_profile ua" +
            "              ON ua.user_id = ? AND ua.feature_type = 'TAG'" +
            "             AND ua.feature_name = t.tag_name AND ua.score > 0.2" +
            "            WHERE mt.movie_id = m.id)" +
            "         +" +
            "           (SELECT COUNT(*) FROM movie_tag mt" +
            "            JOIN tag t ON t.id = mt.tag_id" +
            "            JOIN user_preference_profile ub" +
            "              ON ub.user_id = ? AND ub.feature_type = 'TAG'" +
            "             AND ub.feature_name = t.tag_name AND ub.score > 0.2" +
            "            WHERE mt.movie_id = m.id)" +
            "       ) AS combined_score" +
            " FROM movie m" +
            " WHERE COALESCE(m.popularity, 0) >= 3.0" +
            "   AND m.id NOT IN (" +
            "       SELECT movie_id FROM user_movie_watched WHERE user_id IN (?, ?)" +
            "       UNION ALL" +
            "       SELECT movie_id FROM user_movie_life   WHERE user_id IN (?, ?)" +
            "   )" +
            "   AND m.id NOT IN (" +
            "       SELECT movie_id FROM movie_throw" +
            "       WHERE (sender_user_id = ? AND receiver_user_id = ?)" +
            "          OR (sender_user_id = ? AND receiver_user_id = ?)" +
            "   )" +
            ") ranked" +
            " WHERE combined_score > 0" +
            " ORDER BY combined_score DESC, popularity DESC LIMIT 10";

        List<Map<String, Object>> fallback = jdbcTemplate.query(fallbackSql,
                (rs, rowNum) -> toMovieRow(rs),
                userIdA, userIdB,
                userIdA, userIdB, userIdA, userIdB,
                userIdA, userIdB, userIdB, userIdA);

        List<Map<String, Object>> merged = new ArrayList<>(primary);
        for (Map<String, Object> row : fallback) {
            if (merged.stream().noneMatch(r -> r.get("id").equals(row.get("id")))) {
                merged.add(row);
            }
            if (merged.size() >= 3) break;
        }
        return merged.subList(0, Math.min(3, merged.size()));
    }

    /**
     * 내가 던질 수 있는 영화 목록 (내 인생영화 + 좋아요). 자유 선택 모달의 "내 목록" 탭용.
     */
    public List<Map<String, Object>> getMyThrowableMovies(Long userId) {
        return jdbcTemplate.query("""
                SELECT m.id, m.movie_cd,
                       COALESCE(m.title, m.movie_name) AS title,
                       m.poster_image_url, m.production_year
                FROM movie m
                WHERE m.id IN (
                    SELECT movie_id FROM user_movie_life WHERE user_id = ?
                    UNION
                    SELECT movie_id FROM user_movie_like WHERE user_id = ?
                )
                ORDER BY m.production_year DESC NULLS LAST
                LIMIT 30
                """, (rs, rowNum) -> toMovieRow(rs), userId, userId);
    }

    /**
     * 두 사용자의 TAG+GENRE 벡터 코사인 유사도 (0~100%).
     */
    public int getSimilarityPercent(Long userIdA, Long userIdB) {
        Map<String, Double> vecA = loadVector(userIdA);
        Map<String, Double> vecB = loadVector(userIdB);
        if (vecA.isEmpty() || vecB.isEmpty()) return 0;
        double dot = 0, normA = 0, normB = 0;
        for (Map.Entry<String, Double> e : vecA.entrySet()) {
            normA += e.getValue() * e.getValue();
            Double bVal = vecB.get(e.getKey());
            if (bVal != null) dot += e.getValue() * bVal;
        }
        for (double v : vecB.values()) normB += v * v;
        if (normA == 0 || normB == 0) return 0;
        double sim = dot / (Math.sqrt(normA) * Math.sqrt(normB));
        return (int) Math.round(Math.max(0, sim) * 100);
    }

    /**
     * 영화 던지기 실행: movie_throw INSERT + 알림 발송.
     */
    public void throwMovie(Long senderUserId, String targetLoginId, String movieCode, String message) {
        initializeThrowTable();
        Long receiverUserId = jdbcTemplate.query(
                "SELECT id FROM \"USER\" WHERE login_id = ?",
                rs -> rs.next() ? rs.getLong("id") : null, targetLoginId);
        if (receiverUserId == null) {
            throw new ResponseStatusException(NOT_FOUND, "대상 사용자를 찾을 수 없습니다.");
        }
        if (senderUserId.equals(receiverUserId)) {
            throw new ResponseStatusException(BAD_REQUEST, "자신에게 던질 수 없습니다.");
        }
        requireFollowing(senderUserId, receiverUserId);
        Long movieId = jdbcTemplate.query(
                "SELECT id FROM movie WHERE movie_cd = ?",
                rs -> rs.next() ? rs.getLong("id") : null, movieCode);
        if (movieId == null) {
            throw new ResponseStatusException(NOT_FOUND, "영화를 찾을 수 없습니다.");
        }
        String trimmedMessage = normalizeMessage(message);
        jdbcTemplate.update("""
                INSERT INTO movie_throw (sender_user_id, receiver_user_id, movie_id, movie_cd, message)
                VALUES (?, ?, ?, ?, ?)
                """, senderUserId, receiverUserId, movieId, movieCode, trimmedMessage);
        notificationService.createMovieThrowNotification(senderUserId, receiverUserId, movieId, movieCode, trimmedMessage);
    }

    private static String normalizeMessage(String message) {
        if (message == null) return null;
        String trimmed = message.trim();
        if (trimmed.isEmpty()) return null;
        return trimmed.length() > 200 ? trimmed.substring(0, 200) : trimmed;
    }

    /**
     * 내가 던진 영화 목록.
     */
    public List<Map<String, Object>> getSentThrows(Long userId) {
        initializeThrowTable();
        return jdbcTemplate.query("""
                SELECT mt.id, mt.created_at, mt.movie_cd, mt.message, mt.reaction,
                       COALESCE(m.title, m.movie_name) AS movie_title,
                       m.poster_image_url,
                       u.nickname          AS receiver_nickname,
                       u.login_id          AS receiver_login_id,
                       u.profile_image_url AS receiver_profile_image_url
                FROM movie_throw mt
                JOIN movie m ON m.id = mt.movie_id
                JOIN "USER" u ON u.id = mt.receiver_user_id
                WHERE mt.sender_user_id = ?
                  AND NOT (
                      EXISTS (SELECT 1 FROM user_movie_watched WHERE user_id = mt.sender_user_id   AND movie_id = mt.movie_id AND status = 'WATCHED')
                  AND EXISTS (SELECT 1 FROM user_movie_watched WHERE user_id = mt.receiver_user_id AND movie_id = mt.movie_id AND status = 'WATCHED')
                  )
                ORDER BY mt.created_at DESC
                LIMIT 20
                """, (rs, rowNum) -> toThrowRow(rs, "receiver"), userId);
    }

    /**
     * 내가 받은 영화 목록.
     */
    public List<Map<String, Object>> getReceivedThrows(Long userId) {
        initializeThrowTable();
        return jdbcTemplate.query("""
                SELECT mt.id, mt.created_at, mt.movie_cd, mt.message, mt.reaction,
                       COALESCE(m.title, m.movie_name) AS movie_title,
                       m.poster_image_url,
                       u.nickname          AS sender_nickname,
                       u.login_id          AS sender_login_id,
                       u.profile_image_url AS sender_profile_image_url
                FROM movie_throw mt
                JOIN movie m ON m.id = mt.movie_id
                JOIN "USER" u ON u.id = mt.sender_user_id
                WHERE mt.receiver_user_id = ?
                  AND NOT (
                      EXISTS (SELECT 1 FROM user_movie_watched WHERE user_id = mt.sender_user_id   AND movie_id = mt.movie_id AND status = 'WATCHED')
                  AND EXISTS (SELECT 1 FROM user_movie_watched WHERE user_id = mt.receiver_user_id AND movie_id = mt.movie_id AND status = 'WATCHED')
                  )
                ORDER BY mt.created_at DESC
                LIMIT 20
                """, (rs, rowNum) -> toThrowRow(rs, "sender"), userId);
    }

    /**
     * 수신자가 movieId 영화를 받은 가장 최근 던지기의 발신자 userId 반환.
     */
    public Long findThrowSender(Long receiverUserId, Long movieId) {
        initializeThrowTable();
        return jdbcTemplate.query("""
                SELECT sender_user_id FROM movie_throw
                WHERE receiver_user_id = ? AND movie_id = ?
                ORDER BY created_at DESC
                LIMIT 1
                """, rs -> rs.next() ? rs.getLong("sender_user_id") : null,
                receiverUserId, movieId);
    }

    /**
     * 받은 영화에 대한 반응 저장 (수신자 본인만 가능). reaction 값은 화이트리스트 검증.
     */
    public void reactToThrow(Long currentUserId, Long throwId, String reaction) {
        initializeThrowTable();
        if (reaction == null || !ALLOWED_REACTIONS.contains(reaction)) {
            throw new ResponseStatusException(BAD_REQUEST, "유효하지 않은 반응입니다.");
        }
        int updated = jdbcTemplate.update("""
                UPDATE movie_throw
                SET reaction = ?, reacted_at = CURRENT_TIMESTAMP
                WHERE id = ? AND receiver_user_id = ?
                """, reaction, throwId, currentUserId);
        if (updated == 0) {
            throw new ResponseStatusException(NOT_FOUND, "반응할 수 있는 던지기를 찾을 수 없습니다.");
        }
    }

    private void initializeThrowTable() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS movie_throw (
                    id               BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    sender_user_id   BIGINT NOT NULL,
                    receiver_user_id BIGINT NOT NULL,
                    movie_id         BIGINT,
                    movie_cd         VARCHAR(20),
                    created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT fk_throw_sender   FOREIGN KEY (sender_user_id)   REFERENCES "USER"(id),
                    CONSTRAINT fk_throw_receiver FOREIGN KEY (receiver_user_id) REFERENCES "USER"(id)
                )
                """);
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_throw_sender   ON movie_throw (sender_user_id,   created_at DESC)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_throw_receiver ON movie_throw (receiver_user_id, created_at DESC)");
        jdbcTemplate.execute("ALTER TABLE movie_throw ADD COLUMN IF NOT EXISTS message    VARCHAR(200)");
        jdbcTemplate.execute("ALTER TABLE movie_throw ADD COLUMN IF NOT EXISTS reaction   VARCHAR(20)");
        jdbcTemplate.execute("ALTER TABLE movie_throw ADD COLUMN IF NOT EXISTS reacted_at TIMESTAMP");
    }

    /**
     * 단방향 팔로우 검증: 발신자가 수신자를 팔로우하고 있어야 던질 수 있다.
     * (인자 순서 중요 — sender → receiver 방향)
     */
    private void requireFollowing(Long senderUserId, Long receiverUserId) {
        Boolean follows = jdbcTemplate.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM user_follow WHERE follower_user_id = ? AND following_user_id = ?)",
                Boolean.class, senderUserId, receiverUserId);
        if (!Boolean.TRUE.equals(follows)) {
            throw new ResponseStatusException(FORBIDDEN, "팔로우한 친구에게만 영화 던지기가 가능합니다.");
        }
    }

    private Map<String, Double> loadVector(Long userId) {
        Map<String, Double> vec = new LinkedHashMap<>();
        jdbcTemplate.query("""
                SELECT feature_type, feature_name, score
                FROM user_preference_profile
                WHERE user_id = ?
                  AND feature_type IN ('TAG', 'GENRE')
                  AND score > 0
                """, rs -> {
            String key = rs.getString("feature_type") + "|" + rs.getString("feature_name");
            vec.put(key, rs.getDouble("score"));
        }, userId);
        return vec;
    }

    private Map<String, Object> toMovieRow(ResultSet rs) throws SQLException {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", rs.getLong("id"));
        row.put("movieCode", rs.getString("movie_cd"));
        row.put("title", rs.getString("title"));
        row.put("posterImageUrl", rs.getString("poster_image_url"));
        row.put("year", rs.getString("production_year"));
        return row;
    }

    private Map<String, Object> toThrowRow(ResultSet rs, String party) throws SQLException {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", rs.getLong("id"));
        row.put("movieCode", rs.getString("movie_cd"));
        row.put("movieTitle", rs.getString("movie_title"));
        row.put("posterImageUrl", rs.getString("poster_image_url"));
        row.put(party + "Nickname", rs.getString(party + "_nickname"));
        row.put(party + "LoginId", rs.getString(party + "_login_id"));
        row.put(party + "ProfileImageUrl", rs.getString(party + "_profile_image_url"));
        row.put("message", rs.getString("message"));
        row.put("reaction", rs.getString("reaction"));
        Object ca = rs.getObject("created_at");
        row.put("createdAt", ca != null ? ca.toString() : null);
        return row;
    }
}
