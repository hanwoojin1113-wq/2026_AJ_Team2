package com.cinematch.notification;

import static org.springframework.http.HttpStatus.UNAUTHORIZED;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import jakarta.servlet.http.HttpSession;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/notifications")
public class NotificationController {

    private static final String LOGIN_SESSION_KEY = "loginUserId";

    private final NotificationService notificationService;
    private final JdbcTemplate jdbcTemplate;

    public NotificationController(NotificationService notificationService, JdbcTemplate jdbcTemplate) {
        this.notificationService = notificationService;
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping
    public Map<String, Object> listNotifications(HttpSession session) {
        Long userId = resolveCurrentUserId(session);
        if (userId == null) {
            throw new ResponseStatusException(UNAUTHORIZED);
        }

        List<Map<String, Object>> raw = notificationService.listNotifications(userId, 20);
        List<Map<String, Object>> items = new ArrayList<>();
        for (Map<String, Object> row : raw) {
            Map<String, Object> item = new LinkedHashMap<>();
            String notifType = (String) row.get("NOTIFICATION_TYPE");
            String actorLoginId = (String) row.get("ACTOR_LOGIN_ID");
            String movieCd = (String) row.get("MOVIE_CD");
            Long postId = row.get("POST_ID") instanceof Number number ? number.longValue() : null;
            Long battleId = row.get("BATTLE_ID") instanceof Number number ? number.longValue() : null;
            String actor = row.get("ACTOR_NICKNAME") instanceof String nickname && !nickname.isBlank()
                    ? nickname
                    : "누군가";

            item.put("id", row.get("ID"));
            item.put("type", notifType);
            item.put("actorNickname", row.get("ACTOR_NICKNAME"));
            item.put("actorLoginId", actorLoginId);
            item.put("actorProfileImageUrl", row.get("ACTOR_PROFILE_IMAGE_URL"));
            item.put("movieCode", row.get("MOVIE_CODE"));
            item.put("movieName", row.get("MOVIE_NAME"));
            item.put("moviePosterUrl", row.get("MOVIE_POSTER_URL"));
            item.put("isRead", row.get("IS_READ"));
            Object createdAt = row.get("CREATED_AT");
            item.put("createdAt", createdAt != null ? createdAt.toString() : null);
            String movieName = row.get("MOVIE_NAME") instanceof String s ? s : null;
            item.put("linkUrl", buildLinkUrl(notifType, postId, battleId, movieCd, actorLoginId));
            item.put("notifText", buildNotificationText(notifType, actor, movieName));
            items.add(item);
        }

        int unreadCount = notificationService.countUnread(userId);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("unreadCount", unreadCount);
        result.put("items", items);
        return result;
    }

    @PostMapping("/read-all")
    public Map<String, Object> markAllRead(HttpSession session) {
        Long userId = resolveCurrentUserId(session);
        if (userId == null) {
            throw new ResponseStatusException(UNAUTHORIZED);
        }
        notificationService.markAllRead(userId);
        return Map.of("ok", true);
    }

    private String buildLinkUrl(String notifType, Long postId, Long battleId, String movieCd, String actorLoginId) {
        return switch (notifType != null ? notifType : "") {
            case "NEW_POST" -> (movieCd != null && postId != null)
                    ? "/movies/" + movieCd + "/posts#post-" + postId
                    : "#";
            case "POST_LIKE" -> postId != null ? "/posts/" + postId : "#";
            case "POST_COMMENT" -> postId != null ? "/posts/" + postId + "#comments" : "#";
            case "REVIEW_LIKE" -> movieCd != null ? "/movies/" + movieCd : "#";
            case "FOLLOW" -> actorLoginId != null ? "/users/" + actorLoginId : "#";
            case "MOVIE_THROW" -> movieCd != null ? "/movies/" + movieCd : "#";
            case "MOVIE_THROW_WATCHING" -> movieCd != null ? "/movies/" + movieCd : "#";
            case "MOVIE_THROW_WATCHED" -> movieCd != null ? "/movies/" + movieCd : "#";
            case "BATTLE_SHARE" -> battleId != null ? "/battles/" + battleId : "#";
            default -> movieCd != null ? "/movies/" + movieCd : "#";
        };
    }

    private String buildNotificationText(String notifType, String actor, String movieName) {
        return switch (notifType != null ? notifType : "") {
            case "NEW_POST" -> actor + "님이 새 게시물을 올렸습니다.";
            case "POST_LIKE" -> actor + "님이 내 게시물에 좋아요를 눌렀습니다.";
            case "POST_COMMENT" -> actor + "님이 내 게시물에 댓글을 달았습니다.";
            case "REVIEW_LIKE" -> actor + "님이 내 리뷰에 좋아요를 남겼습니다.";
            case "FOLLOW" -> actor + "님이 팔로우하기 시작했습니다.";
            case "LIFE_MOVIE" -> actor + "님이 인생영화를 추가했습니다.";
            case "MOVIE_SHARE" -> {
                String title = movieName != null ? movieName : "영화";
                String particle = eulReul(title);
                yield actor + "님이 " + title + particle + " 공유했습니다.";
            }
            case "MOVIE_THROW" -> {
                String title = movieName != null ? movieName : "영화";
                yield actor + "님이 " + title + eulReul(title) + " 던졌습니다.";
            }
            case "MOVIE_THROW_WATCHING" -> {
                String title = movieName != null ? movieName : "영화";
                yield actor + "님이 " + title + eulReul(title) + " 보는중입니다.";
            }
            case "MOVIE_THROW_WATCHED" -> {
                String title = movieName != null ? movieName : "영화";
                yield actor + "님이 " + title + eulReul(title) + " 봤습니다!";
            }
            case "BATTLE_SHARE" -> actor + "님이 배틀을 공유했습니다.";
            default -> actor + "님이 활동했습니다.";
        };
    }

    private static String eulReul(String text) {
        if (text == null || text.isBlank()) return "을(를)";
        char last = text.charAt(text.length() - 1);
        if (last < 0xAC00 || last > 0xD7A3) return "을(를)";
        return (last - 0xAC00) % 28 == 0 ? "를" : "을";
    }

    private Long resolveCurrentUserId(HttpSession session) {
        Object value = session.getAttribute(LOGIN_SESSION_KEY);
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof String loginId && !loginId.isBlank()) {
            return jdbcTemplate.query("""
                    SELECT id
                    FROM "USER"
                    WHERE login_id = ?
                    """, rs -> rs.next() ? rs.getLong("id") : null, loginId);
        }
        return null;
    }
}
