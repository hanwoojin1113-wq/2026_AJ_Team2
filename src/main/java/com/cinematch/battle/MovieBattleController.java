package com.cinematch.battle;

import jakarta.servlet.http.HttpSession;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;

import static org.springframework.http.HttpStatus.UNAUTHORIZED;

@Controller
public class MovieBattleController {

    private static final String SESSION_LOGIN_ID = "loginUserId";

    private final MovieBattleService battleService;
    private final JdbcTemplate jdbcTemplate;

    public MovieBattleController(MovieBattleService battleService, JdbcTemplate jdbcTemplate) {
        this.battleService = battleService;
        this.jdbcTemplate = jdbcTemplate;
    }

    // ── 페이지 ────────────────────────────────────────────────────────────────

    @GetMapping("/battles")
    public String battlePage(HttpSession session, Model model) {
        Long userId = getCurrentUserId(session);

        battleService.initTables();
        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM movie_battle", Integer.class);
        if (count == null || count == 0) {
            battleService.generateBattles();
        }

        List<Map<String, Object>> battles = battleService.getActiveBattles(userId);
        model.addAttribute("battles", battles);
        model.addAttribute("loginUserId", session.getAttribute(SESSION_LOGIN_ID));
        model.addAttribute("loginUserNickname", session.getAttribute("loginUserNickname"));
        return "battle";
    }

    // ── REST API ──────────────────────────────────────────────────────────────

    @GetMapping("/api/battles")
    @ResponseBody
    public List<Map<String, Object>> getBattles(HttpSession session) {
        Long userId = getCurrentUserId(session);
        return battleService.getActiveBattles(userId);
    }

    @PostMapping("/api/battles/generate")
    @ResponseBody
    public Map<String, Object> generate(HttpSession session) {
        requireLogin(session);
        int generated = battleService.generateBattles();
        return Map.of("generated", generated);
    }

    @PostMapping("/api/battles/{battleId}/vote")
    @ResponseBody
    public Map<String, Object> vote(@PathVariable Long battleId,
                                    @RequestBody Map<String, Long> body,
                                    HttpSession session) {
        Long userId = requireLogin(session);
        Long movieId = body.get("movieId");
        if (movieId == null) {
            throw new ResponseStatusException(org.springframework.http.HttpStatus.BAD_REQUEST, "movieId 필요");
        }
        return battleService.vote(battleId, movieId, userId);
    }

    // ── 헬퍼 ──────────────────────────────────────────────────────────────────

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

    private Long requireLogin(HttpSession session) {
        Long userId = getCurrentUserId(session);
        if (userId == null) throw new ResponseStatusException(UNAUTHORIZED, "로그인이 필요합니다.");
        return userId;
    }
}
