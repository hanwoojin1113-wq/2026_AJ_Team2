package com.cinematch;

import jakarta.servlet.http.HttpSession;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.springframework.http.HttpStatus.UNAUTHORIZED;

@RestController
@RequestMapping("/api/throw")
public class MovieThrowController {

    private static final String LOGIN_SESSION_KEY = "loginUserId";

    private final MovieThrowService movieThrowService;
    private final JdbcTemplate jdbcTemplate;

    public MovieThrowController(MovieThrowService movieThrowService, JdbcTemplate jdbcTemplate) {
        this.movieThrowService = movieThrowService;
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * GET /api/throw/suggest?targetLoginId=xxx
     * 두 사용자의 취향 교집합 영화 3개 + 유사도 % 반환.
     */
    @GetMapping("/suggest")
    public Map<String, Object> suggest(@RequestParam String targetLoginId, HttpSession session) {
        Long currentUserId = requireCurrentUserId(session);
        Long targetUserId = jdbcTemplate.query(
                "SELECT id FROM \"USER\" WHERE login_id = ?",
                rs -> rs.next() ? rs.getLong("id") : null, targetLoginId);
        if (targetUserId == null) {
            throw new ResponseStatusException(org.springframework.http.HttpStatus.NOT_FOUND);
        }
        List<Map<String, Object>> movies = movieThrowService.getSuggestedMovies(currentUserId, targetUserId);
        int similarityPct = movieThrowService.getSimilarityPercent(currentUserId, targetUserId);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("similarityPct", similarityPct);
        result.put("movies", movies);
        return result;
    }

    /**
     * POST /api/throw
     * body: { targetLoginId: "xxx", movieCode: "K-12345" }
     */
    @PostMapping
    public Map<String, Object> throwMovie(@RequestBody Map<String, String> body, HttpSession session) {
        Long currentUserId = requireCurrentUserId(session);
        String targetLoginId = body.get("targetLoginId");
        String movieCode = body.get("movieCode");
        String message = body.get("message");
        if (targetLoginId == null || movieCode == null) {
            throw new ResponseStatusException(org.springframework.http.HttpStatus.BAD_REQUEST);
        }
        movieThrowService.throwMovie(currentUserId, targetLoginId, movieCode, message);
        return Map.of("ok", true);
    }

    /**
     * GET /api/throw/my-picks — 내가 던질 수 있는 영화(인생영화+좋아요) 목록.
     */
    @GetMapping("/my-picks")
    public List<Map<String, Object>> myPicks(HttpSession session) {
        Long currentUserId = requireCurrentUserId(session);
        return movieThrowService.getMyThrowableMovies(currentUserId);
    }

    /**
     * POST /api/throw/{throwId}/react
     * body: { reaction: "THANKS" | "WILL_WATCH" | "NOT_INTERESTED" }
     */
    @PostMapping("/{throwId}/react")
    public Map<String, Object> react(@PathVariable Long throwId,
                                     @RequestBody Map<String, String> body,
                                     HttpSession session) {
        Long currentUserId = requireCurrentUserId(session);
        movieThrowService.reactToThrow(currentUserId, throwId, body.get("reaction"));
        return Map.of("ok", true);
    }

    /**
     * GET /api/throw/sent — 내가 던진 영화 목록
     */
    @GetMapping("/sent")
    public List<Map<String, Object>> sent(HttpSession session) {
        Long currentUserId = requireCurrentUserId(session);
        return movieThrowService.getSentThrows(currentUserId);
    }

    /**
     * GET /api/throw/received — 내가 받은 영화 목록
     */
    @GetMapping("/received")
    public List<Map<String, Object>> received(HttpSession session) {
        Long currentUserId = requireCurrentUserId(session);
        return movieThrowService.getReceivedThrows(currentUserId);
    }

    private Long requireCurrentUserId(HttpSession session) {
        Object value = session.getAttribute(LOGIN_SESSION_KEY);
        if (value instanceof Number n) return n.longValue();
        if (value instanceof String loginId && !loginId.isBlank()) {
            return jdbcTemplate.query(
                    "SELECT id FROM \"USER\" WHERE login_id = ?",
                    rs -> rs.next() ? rs.getLong("id") : null, loginId);
        }
        throw new ResponseStatusException(UNAUTHORIZED);
    }
}
