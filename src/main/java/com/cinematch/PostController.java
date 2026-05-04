package com.cinematch;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.HttpStatus.UNAUTHORIZED;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

@Slf4j
@Controller
@RequiredArgsConstructor
public class PostController {

    private static final String LOGIN_SESSION_KEY = "loginUserId";
    private static final String LOGIN_NICKNAME_SESSION_KEY = "loginUserNickname";
    private static final Set<String> ALLOWED_IMAGE_EXTENSIONS = Set.of("jpg", "jpeg", "png", "gif", "webp");
    private static final DateTimeFormatter POST_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy.MM.dd");

    private final JdbcTemplate jdbcTemplate;

    @Value("${app.upload.dir:uploads}")
    private String uploadDir;

    @GetMapping("/posts/create")
    public String createPage(Model model, HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/charts";
        }
        model.addAttribute("loginUserNickname", session.getAttribute(LOGIN_NICKNAME_SESSION_KEY));
        model.addAttribute("loginUserId", session.getAttribute(LOGIN_SESSION_KEY));
        return "post-create";
    }

    @GetMapping("/posts/{postId}")
    public String postDetail(@PathVariable Long postId, Model model, HttpSession session) {
        initializeSocialTables();
        Map<String, Object> post = jdbcTemplate.queryForList("""
                SELECT
                    sp.id AS postId,
                    sp.content,
                    sp.created_at AS createdAt,
                    sp.user_id AS userId,
                    u.nickname,
                    u.login_id AS loginId,
                    u.profile_image_url AS profileImageUrl,
                    m.id AS movieDbId,
                    m.movie_cd AS movieCode,
                    COALESCE(m.title, m.movie_name) AS movieTitle,
                    m.poster_image_url AS moviePosterUrl,
                    spi.image_url AS imageUrl
                FROM social_post sp
                JOIN "USER" u ON u.id = sp.user_id
                JOIN movie m ON m.id = sp.movie_id
                LEFT JOIN social_post_image spi ON spi.post_id = sp.id AND spi.display_order = 0
                WHERE sp.id = ?
                  AND sp.is_deleted = FALSE
                """, postId).stream().findFirst().orElse(null);

        if (post == null) {
            throw new ResponseStatusException(NOT_FOUND, "게시물을 찾을 수 없습니다.");
        }

        Long currentUserId = findOptionalCurrentUserId(session);
        String loginUserNickname = (String) session.getAttribute(LOGIN_NICKNAME_SESSION_KEY);
        String loginUserId = (String) session.getAttribute(LOGIN_SESSION_KEY);

        boolean isFollowing = false;
        boolean isMyPost = false;
        Long postUserId = ((Number) post.get("userId")).longValue();
        if (currentUserId != null) {
            isMyPost = currentUserId.equals(postUserId);
            if (!isMyPost) {
                Integer cnt = jdbcTemplate.queryForObject("""
                        SELECT COUNT(*)
                        FROM user_follow
                        WHERE follower_user_id = ? AND following_user_id = ?
                        """, Integer.class, currentUserId, postUserId);
                isFollowing = cnt != null && cnt > 0;
            }
        }

        Object createdAt = post.get("createdAt");
        if (createdAt instanceof Timestamp timestamp) {
            post.put("createdAtLabel", timestamp.toLocalDateTime().format(POST_DATE_FORMAT));
        } else if (createdAt != null) {
            post.put("createdAtLabel", createdAt.toString());
        } else {
            post.put("createdAtLabel", "");
        }

        model.addAttribute("post", post);
        model.addAttribute("isFollowing", isFollowing);
        model.addAttribute("isMyPost", isMyPost);
        model.addAttribute("loginUserNickname", loginUserNickname);
        model.addAttribute("loginUserId", loginUserId);
        return "post-detail";
    }

    @GetMapping("/api/movies/search")
    @ResponseBody
    public List<Map<String, Object>> searchMovies(@RequestParam String query) {
        String normalized = query == null ? "" : query.trim();
        if (normalized.isBlank()) {
            return List.of();
        }

        String likePattern = "%" + normalized + "%";
        return jdbcTemplate.query("""
                SELECT
                    m.id,
                    m.movie_cd,
                    COALESCE(m.title, m.movie_name) AS title,
                    m.poster_image_url
                FROM movie m
                WHERE UPPER(COALESCE(m.movie_name, '')) LIKE UPPER(?)
                   OR UPPER(COALESCE(m.movie_name_en, '')) LIKE UPPER(?)
                ORDER BY
                    CASE
                        WHEN m.poster_image_url IS NULL OR m.poster_image_url = '' THEN 1
                        ELSE 0
                    END,
                    COALESCE(m.title, m.movie_name) ASC
                LIMIT 10
                """, (rs, rowNum) -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("movieId", rs.getLong("id"));
            row.put("movieCode", rs.getString("movie_cd"));
            row.put("title", rs.getString("title"));
            row.put("posterUrl", rs.getString("poster_image_url"));
            return row;
        }, likePattern, likePattern);
    }

    @PostMapping("/posts")
    @Transactional
    public String createPost(@RequestParam("image") MultipartFile image,
                             @RequestParam String content,
                             @RequestParam Long movieId,
                             HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/charts";
        }
        initializeSocialTables();

        try {
            Long userId = requireCurrentUserId(session);
            String movieCode = findMovieCode(movieId);
            String imageUrl = storeImage(userId, image);
            Long postId = insertPost(userId, movieId, normalizeContent(content));
            insertPostImage(postId, imageUrl);
            return "redirect:/movies/" + movieCode;
        } catch (ResponseStatusException e) {
            throw e;
        } catch (Exception e) {
            log.error("게시물 저장 실패: {}", e.getMessage(), e);
            return "redirect:/posts/create?error=true";
        }
    }

    private boolean isLoggedIn(HttpSession session) {
        return session.getAttribute(LOGIN_SESSION_KEY) != null;
    }

    private Long requireCurrentUserId(HttpSession session) {
        String loginId = (String) session.getAttribute(LOGIN_SESSION_KEY);
        if (loginId == null || loginId.isBlank()) {
            throw new ResponseStatusException(UNAUTHORIZED);
        }
        Long userId = jdbcTemplate.query("""
                SELECT id
                FROM "USER"
                WHERE login_id = ?
                """, rs -> rs.next() ? rs.getLong("id") : null, loginId);
        if (userId == null) {
            throw new ResponseStatusException(NOT_FOUND);
        }
        return userId;
    }

    private Long findOptionalCurrentUserId(HttpSession session) {
        String loginId = (String) session.getAttribute(LOGIN_SESSION_KEY);
        if (loginId == null || loginId.isBlank()) {
            return null;
        }
        return jdbcTemplate.query("""
                SELECT id
                FROM "USER"
                WHERE login_id = ?
                """, rs -> rs.next() ? rs.getLong("id") : null, loginId);
    }

    private String findMovieCode(Long movieId) {
        if (movieId == null) {
            throw new ResponseStatusException(BAD_REQUEST);
        }
        String movieCode = jdbcTemplate.query("""
                SELECT movie_cd
                FROM movie
                WHERE id = ?
                """, rs -> rs.next() ? rs.getString("movie_cd") : null, movieId);
        if (movieCode == null) {
            throw new ResponseStatusException(BAD_REQUEST);
        }
        return movieCode;
    }

    private String storeImage(Long userId, MultipartFile image) throws IOException {
        if (image == null || image.isEmpty()) {
            throw new ResponseStatusException(BAD_REQUEST);
        }

        String originalFilename = image.getOriginalFilename();
        String extension = extractExtension(originalFilename);
        if (!ALLOWED_IMAGE_EXTENSIONS.contains(extension)) {
            throw new ResponseStatusException(BAD_REQUEST, "Unsupported image extension");
        }

        String filename = UUID.randomUUID() + "." + extension;
        Path userDir = Paths.get(uploadDir, "posts", String.valueOf(userId)).toAbsolutePath().normalize();
        Files.createDirectories(userDir);
        Path targetFile = userDir.resolve(filename).normalize();
        image.transferTo(targetFile);
        return "/uploads/posts/" + userId + "/" + filename;
    }

    private Long insertPost(Long userId, Long movieId, String content) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(connection -> {
            PreparedStatement ps = connection.prepareStatement("""
                    INSERT INTO social_post (user_id, movie_id, content)
                    VALUES (?, ?, ?)
                    """, new String[]{"id"});
            ps.setLong(1, userId);
            ps.setLong(2, movieId);
            ps.setString(3, content);
            return ps;
        }, keyHolder);
        Number key = keyHolder.getKey();
        if (key == null) {
            throw new IllegalStateException("Post insert failed");
        }
        return key.longValue();
    }

    private void insertPostImage(Long postId, String imageUrl) {
        jdbcTemplate.update("""
                INSERT INTO social_post_image (post_id, image_url, display_order)
                VALUES (?, ?, 0)
                """, postId, imageUrl);
    }

    private String normalizeContent(String content) {
        String normalized = content == null ? "" : content.trim();
        return normalized.isEmpty() ? null : normalized;
    }

    private String extractExtension(String originalFilename) {
        if (originalFilename == null) {
            throw new ResponseStatusException(BAD_REQUEST);
        }
        String sanitized = originalFilename.replace("\\", "").replace("/", "").trim();
        int dotIndex = sanitized.lastIndexOf('.');
        if (dotIndex < 0 || dotIndex == sanitized.length() - 1) {
            throw new ResponseStatusException(BAD_REQUEST, "Missing image extension");
        }
        return sanitized.substring(dotIndex + 1).toLowerCase(Locale.ROOT);
    }

    private void initializeSocialTables() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS social_post (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    movie_id BIGINT NOT NULL,
                    content TEXT,
                    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """);
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS social_post_image (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    post_id BIGINT NOT NULL,
                    image_url VARCHAR(500) NOT NULL,
                    display_order INT NOT NULL DEFAULT 0,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """);
    }
}
