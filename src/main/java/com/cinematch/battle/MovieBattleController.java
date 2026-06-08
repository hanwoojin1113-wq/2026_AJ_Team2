package com.cinematch.battle;

import com.cinematch.notification.NotificationService;
import jakarta.servlet.http.HttpSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.UNAUTHORIZED;

@Controller
public class MovieBattleController {

    private static final String SESSION_LOGIN_ID = "loginUserId";

    private final MovieBattleService battleService;
    private final NotificationService notificationService;
    private final JdbcTemplate jdbcTemplate;

    @Value("${app.upload.dir:uploads}")
    private String uploadDir;

    public MovieBattleController(MovieBattleService battleService,
                                 NotificationService notificationService,
                                 JdbcTemplate jdbcTemplate) {
        this.battleService = battleService;
        this.notificationService = notificationService;
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping("/battles")
    public String battlePage(HttpSession session, Model model) {
        Long userId = getCurrentUserId(session);
        List<Map<String, Object>> battles = battleService.getActiveBattles(userId);
        model.addAttribute("battles", battles);
        model.addAttribute("loginUserId", session.getAttribute(SESSION_LOGIN_ID));
        model.addAttribute("loginUserNickname", session.getAttribute("loginUserNickname"));
        return "battle";
    }

    @GetMapping("/battles/create")
    public String createPage(HttpSession session, Model model) {
        if (getCurrentUserId(session) == null) {
            return "redirect:/login";
        }
        model.addAttribute("loginUserId", session.getAttribute(SESSION_LOGIN_ID));
        model.addAttribute("loginUserNickname", session.getAttribute("loginUserNickname"));
        return "battle-create";
    }

    @GetMapping("/battles/{battleId}")
    public String detailPage(@PathVariable Long battleId, HttpSession session, Model model) {
        Long userId = getCurrentUserId(session);
        Map<String, Object> battle = battleService.getBattle(battleId, userId);
        model.addAttribute("battle", battle);
        model.addAttribute("loginUserId", session.getAttribute(SESSION_LOGIN_ID));
        model.addAttribute("loginUserNickname", session.getAttribute("loginUserNickname"));
        return "battle-detail";
    }

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

    @PostMapping(value = "/api/battles", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public Map<String, Object> create(@RequestBody Map<String, Object> body, HttpSession session) {
        Long userId = requireLogin(session);
        String title = body.get("title") == null ? "" : String.valueOf(body.get("title"));
        Long movieAId = parseLong(body.get("movieAId"));
        Long movieBId = parseLong(body.get("movieBId"));
        String optionALabel = stringValue(body.get("optionALabel"));
        String optionBLabel = stringValue(body.get("optionBLabel"));
        String optionAImageUrl = stringValue(body.get("optionAImageUrl"));
        String optionBImageUrl = stringValue(body.get("optionBImageUrl"));
        return battleService.createBattle(
                title, movieAId, movieBId,
                optionALabel, optionBLabel,
                optionAImageUrl, optionBImageUrl,
                userId);
    }

    @PostMapping(value = "/api/battles", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @ResponseBody
    public Map<String, Object> createMultipart(@RequestParam String title,
                                               @RequestParam Long movieAId,
                                               @RequestParam(required = false) Long movieBId,
                                               @RequestParam String optionALabel,
                                               @RequestParam String optionBLabel,
                                               @RequestParam(required = false) String optionAImageUrl,
                                               @RequestParam(required = false) String optionBImageUrl,
                                               @RequestParam(required = false) MultipartFile optionAImageFile,
                                               @RequestParam(required = false) MultipartFile optionBImageFile,
                                               HttpSession session) throws IOException {
        Long userId = requireLogin(session);
        String finalImageA = imagePathOrUrl(userId, optionAImageFile, optionAImageUrl);
        String finalImageB = imagePathOrUrl(userId, optionBImageFile, optionBImageUrl);
        return battleService.createBattle(
                title, movieAId, movieBId,
                optionALabel, optionBLabel,
                finalImageA, finalImageB,
                userId);
    }

    @PostMapping("/api/battles/{battleId}/vote")
    @ResponseBody
    public Map<String, Object> vote(@PathVariable Long battleId,
                                    @RequestBody Map<String, Object> body,
                                    HttpSession session) {
        Long userId = requireLogin(session);
        String option = stringValue(body.get("option"));
        if (option == null || option.isBlank()) {
            throw new ResponseStatusException(org.springframework.http.HttpStatus.BAD_REQUEST, "option is required");
        }
        return battleService.vote(battleId, option, userId);
    }

    @GetMapping("/api/battles/{battleId}/comments")
    @ResponseBody
    public List<Map<String, Object>> comments(@PathVariable Long battleId, HttpSession session) {
        Long userId = getCurrentUserId(session);
        return battleService.listComments(battleId, userId);
    }

    @PostMapping("/api/battles/{battleId}/comments")
    @ResponseBody
    public Map<String, Object> addComment(@PathVariable Long battleId,
                                          @RequestBody Map<String, Object> body,
                                          HttpSession session) {
        Long userId = requireLogin(session);
        String content = stringValue(body.get("content"));
        return battleService.addComment(battleId, userId, content);
    }

    @PostMapping("/api/battle-comments/{commentId}/like")
    @ResponseBody
    public Map<String, Object> toggleCommentLike(@PathVariable Long commentId, HttpSession session) {
        Long userId = requireLogin(session);
        return battleService.toggleCommentLike(commentId, userId);
    }

    @GetMapping("/api/battles/{battleId}/share-targets")
    @ResponseBody
    public List<Map<String, Object>> shareTargets(@PathVariable Long battleId, HttpSession session) {
        Long userId = requireLogin(session);
        battleService.getBattle(battleId, userId);
        return battleService.listMutualFollowers(userId);
    }

    @PostMapping("/api/battles/{battleId}/share")
    @ResponseBody
    public Map<String, Object> shareBattle(@PathVariable Long battleId,
                                           @RequestBody Map<String, Object> body,
                                           HttpSession session) {
        Long senderUserId = requireLogin(session);
        Long receiverUserId = parseLong(body.get("receiverUserId"));
        Map<String, Object> result = battleService.shareBattle(battleId, senderUserId, receiverUserId);
        notificationService.createBattleShareNotification(senderUserId, receiverUserId, battleId);
        return result;
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

    private Long requireLogin(HttpSession session) {
        Long userId = getCurrentUserId(session);
        if (userId == null) {
            throw new ResponseStatusException(UNAUTHORIZED, "로그인이 필요합니다.");
        }
        return userId;
    }

    private Long parseLong(Object value) {
        if (value == null) return null;
        if (value instanceof Number n) return n.longValue();
        String text = String.valueOf(value).trim();
        if (text.isBlank()) return null;
        return Long.parseLong(text);
    }

    private String stringValue(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private String imagePathOrUrl(Long userId, MultipartFile file, String url) throws IOException {
        if (file != null && !file.isEmpty()) {
            return storeBattleImage(userId, file);
        }
        return url == null ? "" : url.trim();
    }

    private String storeBattleImage(Long userId, MultipartFile file) throws IOException {
        String extension = extractExtension(file.getOriginalFilename());
        if (!Set.of("jpg", "jpeg", "png", "gif", "webp").contains(extension)) {
            throw new ResponseStatusException(BAD_REQUEST, "지원하지 않는 이미지 형식입니다.");
        }
        String filename = UUID.randomUUID() + "." + extension;
        Path userDir = Paths.get(uploadDir, "battles", String.valueOf(userId)).toAbsolutePath().normalize();
        Files.createDirectories(userDir);
        Path targetFile = userDir.resolve(filename).normalize();
        file.transferTo(targetFile);
        return "/uploads/battles/" + userId + "/" + filename;
    }

    private String extractExtension(String filename) {
        if (filename == null || !filename.contains(".")) {
            throw new ResponseStatusException(BAD_REQUEST, "이미지 파일 확장자가 필요합니다.");
        }
        return filename.substring(filename.lastIndexOf('.') + 1).toLowerCase();
    }
}
