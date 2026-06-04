package com.cinematch.tastemap;

import jakarta.servlet.http.HttpSession;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;
import java.util.Map;

@Controller
public class TasteMapController {

    private static final String SESSION_LOGIN_ID = "loginUserId";

    private final TasteMapService tasteMapService;
    private final JdbcTemplate jdbcTemplate;

    public TasteMapController(TasteMapService tasteMapService, JdbcTemplate jdbcTemplate) {
        this.tasteMapService = tasteMapService;
        this.jdbcTemplate    = jdbcTemplate;
    }

    @GetMapping("/api/taste-map/nodes")
    @ResponseBody
    public List<Map<String, Object>> getNodes(HttpSession session) {
        Long userId = getCurrentUserId(session);
        return tasteMapService.getMapNodes(userId);
    }

    private Long getCurrentUserId(HttpSession session) {
        Object value = session.getAttribute(SESSION_LOGIN_ID);
        if (value instanceof Number n) return n.longValue();
        if (value instanceof String loginId && !loginId.isBlank()) {
            return jdbcTemplate.query(
                    "SELECT id FROM \"USER\" WHERE login_id = ?",
                    rs -> rs.next() ? rs.getLong("id") : null, loginId);
        }
        return null;
    }
}
