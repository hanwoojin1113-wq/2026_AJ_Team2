package com.cinematch.notification;

import jakarta.servlet.http.HttpSession;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.springframework.http.HttpStatus.UNAUTHORIZED;

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
        if (userId == null) throw new ResponseStatusException(UNAUTHORIZED);

        List<Map<String, Object>> raw = notificationService.listNotifications(userId, 20);
        List<Map<String, Object>> items = new ArrayList<>();
        for (Map<String, Object> row : raw) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", row.get("ID"));
            String type = (String) row.get("NOTIFICATION_TYPE");
            item.put("type", type);
            item.put("actorNickname", row.get("ACTOR_NICKNAME"));
            item.put("actorLoginId", row.get("ACTOR_LOGIN_ID"));
            item.put("actorProfileImageUrl", row.get("ACTOR_PROFILE_IMAGE_URL"));
            item.put("movieCode", row.get("MOVIE_CODE"));
            item.put("movieName", row.get("MOVIE_NAME"));
            item.put("moviePosterUrl", row.get("MOVIE_POSTER_URL"));
            item.put("isRead", row.get("IS_READ"));
            Object createdAt = row.get("CREATED_AT");
            item.put("createdAt", createdAt != null ? createdAt.toString() : null);

            String linkUrl = null;
            if ("FOLLOW".equals(type)) {
                String loginId = (String) row.get("ACTOR_LOGIN_ID");
                if (loginId != null) linkUrl = "/users/" + loginId;
            } else if ("LIFE_MOVIE".equals(type)) {
                String movieCode = (String) row.get("MOVIE_CODE");
                if (movieCode != null) linkUrl = "/movies/" + movieCode;
            }
            item.put("linkUrl", linkUrl);
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
        if (userId == null) throw new ResponseStatusException(UNAUTHORIZED);
        notificationService.markAllRead(userId);
        return Map.of("ok", true);
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
