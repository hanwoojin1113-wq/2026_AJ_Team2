package com.cinematch;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.HttpStatus.UNAUTHORIZED;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.server.ResponseStatusException;

import com.cinematch.notification.NotificationService;

@Slf4j
@Controller
@RequiredArgsConstructor
public class ReviewController {

    private static final String LOGIN_SESSION_KEY = "loginUserId";
    private static final DateTimeFormatter REVIEW_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy.MM.dd");

    private final JdbcTemplate jdbcTemplate;
    private final NotificationService notificationService;

    @PostMapping("/api/movies/{movieCode}/reviews")
    @ResponseBody
    public Map<String, Object> upsertReview(@PathVariable String movieCode,
                                            @RequestParam String content,
                                            HttpSession session) {
        initializeReviewTables();

        String loginId = (String) session.getAttribute(LOGIN_SESSION_KEY);
        if (loginId == null || loginId.isBlank()) {
            throw new ResponseStatusException(UNAUTHORIZED, "로그인이 필요합니다.");
        }

        String normalizedContent = content == null ? "" : content.trim();
        if (normalizedContent.isBlank()) {
            throw new ResponseStatusException(BAD_REQUEST, "리뷰 내용을 입력해주세요.");
        }
        if (normalizedContent.length() > 1000) {
            throw new ResponseStatusException(BAD_REQUEST, "리뷰는 최대 1000자까지 입력할 수 있습니다.");
        }

        Long userId = findUserIdByLoginId(loginId);
        if (userId == null) {
            throw new ResponseStatusException(UNAUTHORIZED, "사용자 정보를 찾을 수 없습니다.");
        }

        Long movieId = findMovieIdByCode(movieCode);
        if (movieId == null) {
            throw new ResponseStatusException(NOT_FOUND, "영화를 찾을 수 없습니다.");
        }

        Integer watchedCount = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM user_movie_watched
                WHERE user_id = ?
                  AND movie_id = ?
                  AND COALESCE(status, 'WATCHED') = 'WATCHED'
                """, Integer.class, userId, movieId);
        if (watchedCount == null || watchedCount < 1) {
            throw new ResponseStatusException(BAD_REQUEST, "봤어요 상태에서만 리뷰를 남길 수 있습니다.");
        }

        jdbcTemplate.update("""
                MERGE INTO movie_review (user_id, movie_id, content)
                KEY (user_id, movie_id)
                VALUES (?, ?, ?)
                """, userId, movieId, normalizedContent);

        return Map.of("ok", true);
    }

    @PostMapping("/api/reviews/{reviewId}/like")
    @ResponseBody
    public Map<String, Object> toggleReviewLike(@PathVariable Long reviewId, HttpSession session) {
        initializeReviewTables();

        String loginId = (String) session.getAttribute(LOGIN_SESSION_KEY);
        if (loginId == null || loginId.isBlank()) {
            throw new ResponseStatusException(UNAUTHORIZED, "로그인이 필요합니다.");
        }

        Long userId = findUserIdByLoginId(loginId);
        if (userId == null) {
            throw new ResponseStatusException(UNAUTHORIZED, "사용자 정보를 찾을 수 없습니다.");
        }

        Integer reviewExists = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM movie_review
                WHERE id = ?
                """, Integer.class, reviewId);
        if (reviewExists == null || reviewExists < 1) {
            throw new ResponseStatusException(NOT_FOUND, "리뷰를 찾을 수 없습니다.");
        }

        Integer existingLike = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM review_like
                WHERE user_id = ?
                  AND review_id = ?
                """, Integer.class, userId, reviewId);

        boolean liked;
        if (existingLike != null && existingLike > 0) {
            jdbcTemplate.update("""
                    DELETE FROM review_like
                    WHERE user_id = ?
                      AND review_id = ?
                    """, userId, reviewId);
            liked = false;
        } else {
            jdbcTemplate.update("""
                    INSERT INTO review_like (user_id, review_id)
                    VALUES (?, ?)
                    """, userId, reviewId);
            liked = true;
            try {
                Map<String, Object> reviewInfo = jdbcTemplate.query("""
                        SELECT r.user_id AS ownerId, m.movie_cd AS movieCode
                        FROM movie_review r
                        JOIN movie m ON m.id = r.movie_id
                        WHERE r.id = ?
                        """, rs -> {
                    if (!rs.next()) {
                        return null;
                    }
                    Map<String, Object> info = new LinkedHashMap<>();
                    info.put("ownerId", rs.getLong("ownerId"));
                    info.put("movieCode", rs.getString("movieCode"));
                    return info;
                }, reviewId);
                if (reviewInfo != null) {
                    Long ownerId = ((Number) reviewInfo.get("ownerId")).longValue();
                    String movieCode = (String) reviewInfo.get("movieCode");
                    notificationService.createReviewLikeNotification(userId, reviewId, ownerId, movieCode);
                }
            } catch (Exception ex) {
                log.warn("Review like notification failed: {}", ex.getMessage());
            }
        }

        Integer likeCount = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM review_like
                WHERE review_id = ?
                """, Integer.class, reviewId);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("liked", liked);
        response.put("likeCount", likeCount == null ? 0 : likeCount);
        return response;
    }

    @GetMapping("/api/movies/{movieCode}/reviews")
    @ResponseBody
    public List<Map<String, Object>> fetchMovieReviews(@PathVariable String movieCode, HttpSession session) {
        initializeReviewTables();

        Long movieId = findMovieIdByCode(movieCode);
        if (movieId == null) {
            throw new ResponseStatusException(NOT_FOUND, "영화를 찾을 수 없습니다.");
        }

        Long currentUserId = null;
        String loginId = (String) session.getAttribute(LOGIN_SESSION_KEY);
        if (loginId != null && !loginId.isBlank()) {
            currentUserId = findUserIdByLoginId(loginId);
        }
        long likedByMeUserId = currentUserId == null ? -1L : currentUserId;

        return jdbcTemplate.query("""
                SELECT
                    r.id AS reviewId,
                    u.nickname,
                    u.login_id AS loginId,
                    umw.rating,
                    r.content,
                    r.created_at AS createdAt,
                    COUNT(DISTINCT rl.id) AS likeCount,
                    MAX(CASE WHEN rl2.user_id IS NOT NULL THEN 1 ELSE 0 END) AS likedByMe
                FROM movie_review r
                JOIN "USER" u ON u.id = r.user_id
                LEFT JOIN user_movie_watched umw
                       ON umw.user_id = r.user_id
                      AND umw.movie_id = r.movie_id
                LEFT JOIN review_like rl
                       ON rl.review_id = r.id
                LEFT JOIN review_like rl2
                       ON rl2.review_id = r.id
                      AND rl2.user_id = ?
                WHERE r.movie_id = ?
                GROUP BY r.id, u.nickname, u.login_id, umw.rating, r.content, r.created_at
                ORDER BY COUNT(DISTINCT rl.id) DESC, r.created_at DESC
                """, (rs, rowNum) -> {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("reviewId", rs.getLong("reviewId"));
            item.put("nickname", rs.getString("nickname"));
            item.put("loginId", rs.getString("loginId"));
            item.put("rating", rs.getObject("rating", Integer.class));
            item.put("content", rs.getString("content"));
            item.put("likeCount", rs.getInt("likeCount"));
            item.put("likedByMe", rs.getInt("likedByMe") > 0);

            Timestamp createdAt = rs.getTimestamp("createdAt");
            item.put("createdAt", createdAt == null ? "" : createdAt.toLocalDateTime().format(REVIEW_DATE_FORMAT));
            return item;
        }, likedByMeUserId, movieId);
    }

    private Long findUserIdByLoginId(String loginId) {
        return jdbcTemplate.query("""
                SELECT id
                FROM "USER"
                WHERE login_id = ?
                """, rs -> rs.next() ? rs.getLong("id") : null, loginId);
    }

    private Long findMovieIdByCode(String movieCode) {
        return jdbcTemplate.query("""
                SELECT id
                FROM movie
                WHERE movie_cd = ?
                """, rs -> rs.next() ? rs.getLong("id") : null, movieCode);
    }

    private void initializeReviewTables() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS movie_review (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    movie_id BIGINT NOT NULL,
                    content TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT uk_movie_review_user_movie UNIQUE (user_id, movie_id)
                )
                """);

        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS review_like (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    review_id BIGINT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT uk_review_like UNIQUE (user_id, review_id)
                )
                """);
    }
}
