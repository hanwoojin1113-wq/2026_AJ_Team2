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
import java.util.ArrayList;
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

import com.cinematch.kobis.KobisBoxOfficeService;
import com.cinematch.notification.NotificationService;

@Slf4j
@Controller
@RequiredArgsConstructor
public class PostController {

    private static final String LOGIN_SESSION_KEY = "loginUserId";
    private static final String LOGIN_NICKNAME_SESSION_KEY = "loginUserNickname";
    private static final Set<String> ALLOWED_IMAGE_EXTENSIONS = Set.of("jpg", "jpeg", "png", "gif", "webp");
    private static final Set<String> ALLOWED_VIDEO_EXTENSIONS = Set.of("mp4", "webm", "mov", "avi");
    private static final Set<String> ALLOWED_ATTACH_EXTENSIONS = Set.of(
            "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "txt", "zip", "hwp");
    private static final DateTimeFormatter POST_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy.MM.dd");
    private static final int FEED_PAGE_SIZE = 10;
    private static final int FEED_MAX_PAGE_SIZE = 20;

    private final JdbcTemplate jdbcTemplate;
    private final NotificationService notificationService;
    private final KobisBoxOfficeService kobisBoxOfficeService;
    private final BadgeService badgeService;

    @Value("${app.upload.dir:uploads}")
    private String uploadDir;

    // ── Page endpoints ─────────────────────────────────────────────────────────

    @GetMapping("/posts/create")
    public String createPage(Model model, HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/charts";
        }
        model.addAttribute("loginUserNickname", session.getAttribute(LOGIN_NICKNAME_SESSION_KEY));
        model.addAttribute("loginUserId", session.getAttribute(LOGIN_SESSION_KEY));
        return "post-create";
    }

    @GetMapping("/feed")
    public String feedPage(Model model, HttpSession session, jakarta.servlet.http.HttpServletResponse response) {
        response.setHeader("Cache-Control", "no-store, no-cache, must-revalidate");
        response.setHeader("Pragma", "no-cache");
        if (!isLoggedIn(session)) {
            return "redirect:/login";
        }
        initializeSocialTables();

        Long currentUserId = findOptionalCurrentUserId(session);
        FeedSlice feedSlice = loadFeedSlice(null, FEED_PAGE_SIZE, currentUserId);

        model.addAttribute("posts", feedSlice.posts());
        model.addAttribute("hasMore", feedSlice.hasMore());
        model.addAttribute("nextCursor", feedSlice.nextCursor());
        model.addAttribute("loginUserNickname", session.getAttribute(LOGIN_NICKNAME_SESSION_KEY));
        model.addAttribute("loginUserId", session.getAttribute(LOGIN_SESSION_KEY));
        model.addAttribute("currentUserId", currentUserId);
        model.addAttribute("trendingMovies", kobisBoxOfficeService.fetchBoxOffice(5));
        model.addAttribute("suggestedUsers", fetchFeedSuggestedUsers(currentUserId, 3));
        model.addAttribute("storyUsers", fetchStoryUsers(currentUserId, 15));
        return "feed";
    }

    @GetMapping("/posts/{postId}")
    public String postDetail(@PathVariable Long postId, Model model, HttpSession session) {
        initializeSocialTables();

        Map<String, Object> post = jdbcTemplate.queryForList("""
                SELECT
                    sp.id AS postId,
                    sp.content,
                    sp.post_type AS postType,
                    sp.created_at AS createdAt,
                    sp.user_id AS userId,
                    u.nickname,
                    u.login_id AS loginId,
                    u.profile_image_url AS profileImageUrl,
                    m.id AS movieDbId,
                    m.movie_cd AS movieCode,
                    COALESCE(m.title, m.movie_name) AS movieTitle,
                    m.poster_image_url AS moviePosterUrl
                FROM social_post sp
                JOIN "USER" u ON u.id = sp.user_id
                JOIN movie m ON m.id = sp.movie_id
                WHERE sp.id = ?
                  AND sp.is_deleted = FALSE
                """, postId).stream().findFirst().orElse(null);

        if (post == null) {
            throw new ResponseStatusException(NOT_FOUND, "게시물을 찾을 수 없습니다.");
        }

        Long currentUserId = findOptionalCurrentUserId(session);
        enrichPost(post, currentUserId);

        model.addAttribute("post", post);
        model.addAttribute("isFollowing", post.get("isFollowing"));
        model.addAttribute("isMyPost", post.get("isMyPost"));
        model.addAttribute("loginUserNickname", session.getAttribute(LOGIN_NICKNAME_SESSION_KEY));
        model.addAttribute("loginUserId", session.getAttribute(LOGIN_SESSION_KEY));
        return "post-detail";
    }

    @GetMapping("/movies/{movieCode}/posts")
    public String moviePostsPage(@PathVariable String movieCode, Model model, HttpSession session) {
        initializeSocialTables();

        Long movieId = findMovieIdByCode(movieCode);
        if (movieId == null) {
            throw new ResponseStatusException(NOT_FOUND);
        }

        Map<String, Object> movie = jdbcTemplate.query("""
                SELECT movie_cd, COALESCE(title, movie_name) AS movieTitle, poster_image_url AS posterUrl
                FROM movie
                WHERE id = ?
                """, rs -> {
            if (!rs.next()) {
                return null;
            }
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("movieCode", rs.getString("movie_cd"));
            row.put("movieTitle", rs.getString("movieTitle"));
            row.put("posterUrl", rs.getString("posterUrl"));
            return row;
        }, movieId);

        if (movie == null) {
            throw new ResponseStatusException(NOT_FOUND);
        }

        List<Map<String, Object>> posts = jdbcTemplate.query("""
                SELECT
                    sp.id AS postId,
                    sp.content,
                    sp.post_type AS postType,
                    sp.created_at AS createdAt,
                    sp.user_id AS userId,
                    u.nickname,
                    u.login_id AS loginId,
                    u.profile_image_url AS profileImageUrl
                FROM social_post sp
                JOIN "USER" u ON u.id = sp.user_id
                WHERE sp.movie_id = ?
                  AND sp.is_deleted = FALSE
                ORDER BY sp.created_at DESC
                """, (rs, rowNum) -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("postId", rs.getLong("postId"));
            row.put("content", rs.getString("content"));
            row.put("postType", rs.getString("postType"));
            row.put("createdAt", rs.getTimestamp("createdAt"));
            row.put("userId", rs.getLong("userId"));
            row.put("nickname", rs.getString("nickname"));
            row.put("loginId", rs.getString("loginId"));
            row.put("profileImageUrl", rs.getString("profileImageUrl"));
            return row;
        }, movieId);

        Long currentUserId = findOptionalCurrentUserId(session);
        for (Map<String, Object> post : posts) {
            enrichPost(post, currentUserId);
        }

        model.addAttribute("movie", movie);
        model.addAttribute("posts", posts);
        model.addAttribute("loginUserNickname", session.getAttribute(LOGIN_NICKNAME_SESSION_KEY));
        model.addAttribute("loginUserId", session.getAttribute(LOGIN_SESSION_KEY));
        model.addAttribute("currentUserId", currentUserId);
        return "movie-posts";
    }

    // ── Post creation ──────────────────────────────────────────────────────────

    @PostMapping("/posts")
    @Transactional
    public String createPost(
            @RequestParam(defaultValue = "photo") String postType,
            @RequestParam(required = false) List<MultipartFile> images,
            @RequestParam(required = false) MultipartFile videoFile,
            @RequestParam(required = false) MultipartFile docFile,
            @RequestParam(required = false) String content,
            @RequestParam Long movieId,
            @RequestParam(required = false) String pollQuestion,
            @RequestParam(required = false) List<String> pollOption,
            @RequestParam(required = false, defaultValue = "false") boolean pollMultiSelect,
            @RequestParam(required = false) String quizQuestion,
            @RequestParam(required = false) List<String> quizOption,
            @RequestParam(required = false, defaultValue = "0") int quizCorrectIndex,
            HttpSession session) {

        if (!isLoggedIn(session)) {
            return "redirect:/charts";
        }
        initializeSocialTables();

        try {
            Long userId = requireCurrentUserId(session);
            String movieCode = findMovieCode(movieId);

            Long postId;
            if ("video".equals(postType)) {
                postId = createVideoPost(userId, movieId, content, videoFile);
            } else if ("file".equals(postType)) {
                postId = createFilePost(userId, movieId, content, docFile);
            } else if ("poll".equals(postType)) {
                postId = createPollPost(userId, movieId, content, pollQuestion, pollOption, pollMultiSelect);
            } else if ("quiz".equals(postType)) {
                postId = createQuizPost(userId, movieId, content, quizQuestion, quizOption, quizCorrectIndex);
            } else {
                postId = createPhotoPost(userId, movieId, content, images);
            }

            try {
                notificationService.createNewPostNotifications(userId, postId, movieCode);
            } catch (Exception ex) {
                log.warn("New post notification failed: {}", ex.getMessage());
            }
            return "redirect:/movies/" + movieCode;
        } catch (ResponseStatusException e) {
            throw e;
        } catch (Exception e) {
            log.error("게시물 저장 실패: {}", e.getMessage(), e);
            return "redirect:/posts/create?error=true";
        }
    }

    private Long createPhotoPost(Long userId, Long movieId, String content, List<MultipartFile> images)
            throws IOException {
        List<MultipartFile> validImages = filterValidImages(images);
        if (validImages.isEmpty()) {
            throw new ResponseStatusException(BAD_REQUEST, "최소 한 장의 이미지를 첨부해주세요.");
        }
        if (validImages.size() > 5) {
            throw new ResponseStatusException(BAD_REQUEST, "이미지는 최대 5장까지 업로드할 수 있습니다.");
        }
        Long postId = insertPost(userId, movieId, normalizeContent(content), "photo");
        int order = 0;
        for (MultipartFile image : validImages) {
            String imageUrl = storeUploadedFile(userId, image, "posts", ALLOWED_IMAGE_EXTENSIONS);
            insertPostImage(postId, imageUrl, order++);
        }
        return postId;
    }

    private Long createVideoPost(Long userId, Long movieId, String content, MultipartFile videoFile)
            throws IOException {
        if (videoFile == null || videoFile.isEmpty()) {
            throw new ResponseStatusException(BAD_REQUEST, "동영상 파일을 첨부해주세요.");
        }
        String fileUrl = storeUploadedFile(userId, videoFile, "videos", ALLOWED_VIDEO_EXTENSIONS);
        Long postId = insertPost(userId, movieId, normalizeContent(content), "video");
        insertPostAttachment(postId, fileUrl, videoFile.getOriginalFilename(), "video", videoFile.getSize());
        return postId;
    }

    private Long createFilePost(Long userId, Long movieId, String content, MultipartFile docFile)
            throws IOException {
        if (docFile == null || docFile.isEmpty()) {
            throw new ResponseStatusException(BAD_REQUEST, "파일을 첨부해주세요.");
        }
        String fileUrl = storeUploadedFile(userId, docFile, "files", ALLOWED_ATTACH_EXTENSIONS);
        Long postId = insertPost(userId, movieId, normalizeContent(content), "file");
        insertPostAttachment(postId, fileUrl, docFile.getOriginalFilename(), "file", docFile.getSize());
        return postId;
    }

    private Long createPollPost(Long userId, Long movieId, String content,
            String pollQuestion, List<String> pollOptions, boolean multiSelect) {
        if (pollQuestion == null || pollQuestion.isBlank()) {
            throw new ResponseStatusException(BAD_REQUEST, "투표 질문을 입력해주세요.");
        }
        List<String> validOptions = pollOptions == null ? List.of()
                : pollOptions.stream().map(String::trim).filter(s -> !s.isEmpty()).toList();
        if (validOptions.size() < 2) {
            throw new ResponseStatusException(BAD_REQUEST, "투표 선택지를 2개 이상 입력해주세요.");
        }
        Long postId = insertPost(userId, movieId, normalizeContent(content), "poll");
        Long pollId = insertPoll(postId, pollQuestion.trim(), multiSelect);
        for (int i = 0; i < validOptions.size(); i++) {
            insertPollOption(pollId, validOptions.get(i), i);
        }
        return postId;
    }

    private Long createQuizPost(Long userId, Long movieId, String content,
            String quizQuestion, List<String> quizOptions, int correctIndex) {
        if (quizQuestion == null || quizQuestion.isBlank()) {
            throw new ResponseStatusException(BAD_REQUEST, "퀴즈 질문을 입력해주세요.");
        }
        List<String> validOptions = quizOptions == null ? List.of()
                : quizOptions.stream().map(String::trim).filter(s -> !s.isEmpty()).toList();
        if (validOptions.size() < 2) {
            throw new ResponseStatusException(BAD_REQUEST, "퀴즈 선택지를 2개 이상 입력해주세요.");
        }
        if (correctIndex < 0 || correctIndex >= validOptions.size()) {
            throw new ResponseStatusException(BAD_REQUEST, "정답 선택지를 지정해주세요.");
        }
        Long postId = insertPost(userId, movieId, normalizeContent(content), "quiz");
        Long quizId = insertQuiz(postId, quizQuestion.trim());
        for (int i = 0; i < validOptions.size(); i++) {
            insertQuizOption(quizId, validOptions.get(i), i == correctIndex, i);
        }
        return postId;
    }

    // ── Feed & search API ──────────────────────────────────────────────────────

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

    @GetMapping("/api/feed/posts")
    @ResponseBody
    public Map<String, Object> feedPosts(@RequestParam(required = false) String cursor,
                                         @RequestParam(required = false, defaultValue = "10") int limit,
                                         HttpSession session) {
        initializeSocialTables();
        if (!isLoggedIn(session)) {
            throw new ResponseStatusException(UNAUTHORIZED);
        }

        Long currentUserId = findOptionalCurrentUserId(session);
        FeedSlice feedSlice = loadFeedSlice(cursor, limit, currentUserId);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("posts", feedSlice.posts());
        result.put("hasMore", feedSlice.hasMore());
        result.put("nextCursor", feedSlice.nextCursor());
        return result;
    }

    // ── Like API ───────────────────────────────────────────────────────────────

    @PostMapping("/api/posts/{postId}/like")
    @ResponseBody
    public Map<String, Object> togglePostLike(@PathVariable Long postId, HttpSession session) {
        initializeSocialTables();
        if (!isLoggedIn(session)) {
            throw new ResponseStatusException(UNAUTHORIZED);
        }

        Long userId = requireCurrentUserId(session);
        Integer postExists = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM social_post
                WHERE id = ?
                  AND is_deleted = FALSE
                """, Integer.class, postId);
        if (postExists == null || postExists < 1) {
            throw new ResponseStatusException(NOT_FOUND);
        }

        Integer existing = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM post_like
                WHERE user_id = ?
                  AND post_id = ?
                """, Integer.class, userId, postId);

        boolean liked;
        if (existing != null && existing > 0) {
            jdbcTemplate.update("""
                    DELETE FROM post_like
                    WHERE user_id = ?
                      AND post_id = ?
                    """, userId, postId);
            liked = false;
        } else {
            jdbcTemplate.update("""
                    INSERT INTO post_like (user_id, post_id)
                    VALUES (?, ?)
                    """, userId, postId);
            liked = true;
            try {
                Long postOwnerId = jdbcTemplate.query("""
                        SELECT user_id
                        FROM social_post
                        WHERE id = ?
                        """, rs -> rs.next() ? rs.getLong("user_id") : null, postId);
                String postMovieCode = jdbcTemplate.query("""
                        SELECT m.movie_cd
                        FROM social_post sp
                        JOIN movie m ON m.id = sp.movie_id
                        WHERE sp.id = ?
                        """, rs -> rs.next() ? rs.getString("movie_cd") : null, postId);
                if (postOwnerId != null) {
                    notificationService.createPostLikeNotification(userId, postId, postOwnerId, postMovieCode);
                }
            } catch (Exception ex) {
                log.warn("Post like notification failed: {}", ex.getMessage());
            }
        }

        Integer likeCount = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM post_like
                WHERE post_id = ?
                """, Integer.class, postId);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("liked", liked);
        result.put("likeCount", likeCount != null ? likeCount : 0);
        return result;
    }

    // ── Comment API ────────────────────────────────────────────────────────────

    @GetMapping("/api/posts/{postId}/comments")
    @ResponseBody
    public List<Map<String, Object>> listPostComments(@PathVariable Long postId, HttpSession session) {
        initializeSocialTables();
        requireActivePost(postId);
        Long currentUserId = findOptionalCurrentUserId(session);
        return fetchPostComments(postId, currentUserId);
    }

    @PostMapping("/api/posts/{postId}/comments")
    @ResponseBody
    @Transactional
    public Map<String, Object> createPostComment(@PathVariable Long postId,
                                                 @RequestParam String content,
                                                 HttpSession session) {
        initializeSocialTables();
        if (!isLoggedIn(session)) {
            throw new ResponseStatusException(UNAUTHORIZED);
        }

        Long userId = requireCurrentUserId(session);
        String normalizedContent = normalizeCommentContent(content);
        Map<String, Object> postInfo = findActivePostInfo(postId);
        if (postInfo == null) {
            throw new ResponseStatusException(NOT_FOUND);
        }

        Long commentId = insertPostComment(postId, userId, normalizedContent);
        Map<String, Object> comment = fetchPostComment(commentId, userId);
        Integer commentCount = countPostComments(postId);

        try {
            Long postOwnerId = ((Number) postInfo.get("ownerId")).longValue();
            String movieCode = (String) postInfo.get("movieCode");
            notificationService.createPostCommentNotification(userId, postId, postOwnerId, movieCode);
        } catch (Exception ex) {
            log.warn("Post comment notification failed: {}", ex.getMessage());
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("comment", comment);
        result.put("commentCount", commentCount);
        return result;
    }

    // ── Poll API ───────────────────────────────────────────────────────────────

    @PostMapping("/api/polls/{pollId}/vote")
    @ResponseBody
    @Transactional
    public Map<String, Object> votePoll(@PathVariable Long pollId,
                                        @RequestParam Long optionId,
                                        HttpSession session) {
        initializeSocialTables();
        if (!isLoggedIn(session)) {
            throw new ResponseStatusException(UNAUTHORIZED);
        }
        Long userId = requireCurrentUserId(session);

        Integer optionExists = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM post_poll_option WHERE id = ? AND poll_id = ?",
                Integer.class, optionId, pollId);
        if (optionExists == null || optionExists == 0) {
            throw new ResponseStatusException(NOT_FOUND);
        }

        jdbcTemplate.update("DELETE FROM post_poll_vote WHERE poll_id = ? AND user_id = ?", pollId, userId);
        jdbcTemplate.update(
                "INSERT INTO post_poll_vote (poll_id, option_id, user_id) VALUES (?, ?, ?)",
                pollId, optionId, userId);

        Integer totalVotes = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM post_poll_vote WHERE poll_id = ?", Integer.class, pollId);

        List<Map<String, Object>> options = jdbcTemplate.query("""
                SELECT ppo.id, COUNT(ppv.id) AS voteCount
                FROM post_poll_option ppo
                LEFT JOIN post_poll_vote ppv ON ppv.option_id = ppo.id
                WHERE ppo.poll_id = ?
                GROUP BY ppo.id
                ORDER BY ppo.display_order
                """, (rs, rowNum) -> {
            Map<String, Object> opt = new LinkedHashMap<>();
            opt.put("optionId", rs.getLong("id"));
            opt.put("voteCount", rs.getInt("voteCount"));
            return opt;
        }, pollId);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("totalVotes", totalVotes != null ? totalVotes : 0);
        result.put("votedOptionId", optionId);
        result.put("options", options);
        return result;
    }

    // ── Quiz API ───────────────────────────────────────────────────────────────

    @PostMapping("/api/quizzes/{quizId}/answer")
    @ResponseBody
    @Transactional
    public Map<String, Object> answerQuiz(@PathVariable Long quizId,
                                          @RequestParam Long optionId,
                                          HttpSession session) {
        initializeSocialTables();
        if (!isLoggedIn(session)) {
            throw new ResponseStatusException(UNAUTHORIZED);
        }
        Long userId = requireCurrentUserId(session);

        Integer existingAnswer = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM post_quiz_answer WHERE quiz_id = ? AND user_id = ?",
                Integer.class, quizId, userId);
        if (existingAnswer != null && existingAnswer > 0) {
            throw new ResponseStatusException(BAD_REQUEST, "이미 답변했습니다.");
        }

        Boolean isCorrect = jdbcTemplate.query(
                "SELECT is_correct FROM post_quiz_option WHERE id = ? AND quiz_id = ?",
                rs -> rs.next() ? rs.getBoolean("is_correct") : null,
                optionId, quizId);
        if (isCorrect == null) {
            throw new ResponseStatusException(NOT_FOUND);
        }

        jdbcTemplate.update(
                "INSERT INTO post_quiz_answer (quiz_id, option_id, user_id, is_correct) VALUES (?, ?, ?, ?)",
                quizId, optionId, userId, isCorrect);

        List<Map<String, Object>> options = jdbcTemplate.query("""
                SELECT id, option_text, is_correct
                FROM post_quiz_option
                WHERE quiz_id = ?
                ORDER BY display_order
                """, (rs, rowNum) -> {
            Map<String, Object> opt = new LinkedHashMap<>();
            opt.put("optionId", rs.getLong("id"));
            opt.put("optionText", rs.getString("option_text"));
            opt.put("isCorrect", rs.getBoolean("is_correct"));
            return opt;
        }, quizId);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("isCorrect", isCorrect);
        result.put("answeredOptionId", optionId);
        result.put("options", options);
        return result;
    }

    // ── Private helpers: auth ──────────────────────────────────────────────────

    private boolean isLoggedIn(HttpSession session) {
        return session.getAttribute(LOGIN_SESSION_KEY) != null;
    }

    private Long requireCurrentUserId(HttpSession session) {
        String loginId = (String) session.getAttribute(LOGIN_SESSION_KEY);
        if (loginId == null || loginId.isBlank()) {
            throw new ResponseStatusException(UNAUTHORIZED);
        }
        Long userId = findUserIdByLoginId(loginId);
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
        return findUserIdByLoginId(loginId);
    }

    private Long findUserIdByLoginId(String loginId) {
        return jdbcTemplate.query("""
                SELECT id
                FROM "USER"
                WHERE login_id = ?
                """, rs -> rs.next() ? rs.getLong("id") : null, loginId);
    }

    // ── Private helpers: lookups ───────────────────────────────────────────────

    private Long findMovieIdByCode(String movieCode) {
        return jdbcTemplate.query("""
                SELECT id
                FROM movie
                WHERE movie_cd = ?
                """, rs -> rs.next() ? rs.getLong("id") : null, movieCode);
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

    private void requireActivePost(Long postId) {
        if (findActivePostInfo(postId) == null) {
            throw new ResponseStatusException(NOT_FOUND);
        }
    }

    private Map<String, Object> findActivePostInfo(Long postId) {
        return jdbcTemplate.query("""
                SELECT
                    sp.user_id AS ownerId,
                    m.movie_cd AS movieCode
                FROM social_post sp
                JOIN movie m ON m.id = sp.movie_id
                WHERE sp.id = ?
                  AND sp.is_deleted = FALSE
                """, rs -> {
            if (!rs.next()) {
                return null;
            }
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("ownerId", rs.getLong("ownerId"));
            info.put("movieCode", rs.getString("movieCode"));
            return info;
        }, postId);
    }

    // ── Private helpers: file storage ──────────────────────────────────────────

    private List<MultipartFile> filterValidImages(List<MultipartFile> images) {
        if (images == null) {
            return List.of();
        }
        return images.stream()
                .filter(image -> image != null && !image.isEmpty())
                .toList();
    }

    private String storeUploadedFile(Long userId, MultipartFile file, String subDir,
            Set<String> allowedExtensions) throws IOException {
        if (file == null || file.isEmpty()) {
            throw new ResponseStatusException(BAD_REQUEST);
        }
        String originalFilename = file.getOriginalFilename();
        String extension = extractExtension(originalFilename);
        if (!allowedExtensions.contains(extension)) {
            throw new ResponseStatusException(BAD_REQUEST, "지원하지 않는 파일 형식입니다.");
        }
        String filename = UUID.randomUUID() + "." + extension;
        Path userDir = Paths.get(uploadDir, subDir, String.valueOf(userId)).toAbsolutePath().normalize();
        Files.createDirectories(userDir);
        Path targetFile = userDir.resolve(filename).normalize();
        file.transferTo(targetFile);
        return "/uploads/" + subDir + "/" + userId + "/" + filename;
    }

    // ── Private helpers: DB inserts ────────────────────────────────────────────

    private Long insertPost(Long userId, Long movieId, String content, String postType) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(connection -> {
            PreparedStatement ps = connection.prepareStatement("""
                    INSERT INTO social_post (user_id, movie_id, content, post_type)
                    VALUES (?, ?, ?, ?)
                    """, new String[]{"id"});
            ps.setLong(1, userId);
            ps.setLong(2, movieId);
            ps.setString(3, content);
            ps.setString(4, postType != null ? postType : "photo");
            return ps;
        }, keyHolder);
        Number key = keyHolder.getKey();
        if (key == null) {
            throw new IllegalStateException("Post insert failed");
        }
        return key.longValue();
    }

    private void insertPostImage(Long postId, String imageUrl, int displayOrder) {
        jdbcTemplate.update("""
                INSERT INTO social_post_image (post_id, image_url, display_order)
                VALUES (?, ?, ?)
                """, postId, imageUrl, displayOrder);
    }

    private void insertPostAttachment(Long postId, String fileUrl, String fileName,
            String attachmentType, long fileSize) {
        jdbcTemplate.update("""
                INSERT INTO post_attachment (post_id, file_url, file_name, attachment_type, file_size)
                VALUES (?, ?, ?, ?, ?)
                """, postId, fileUrl, fileName, attachmentType, fileSize);
    }

    private Long insertPoll(Long postId, String question, boolean multiSelect) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(conn -> {
            PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO post_poll (post_id, question, is_multi_select) VALUES (?, ?, ?)",
                    new String[]{"id"});
            ps.setLong(1, postId);
            ps.setString(2, question);
            ps.setBoolean(3, multiSelect);
            return ps;
        }, keyHolder);
        Number key = keyHolder.getKey();
        if (key == null) throw new IllegalStateException("Poll insert failed");
        return key.longValue();
    }

    private void insertPollOption(Long pollId, String optionText, int displayOrder) {
        jdbcTemplate.update(
                "INSERT INTO post_poll_option (poll_id, option_text, display_order) VALUES (?, ?, ?)",
                pollId, optionText, displayOrder);
    }

    private Long insertQuiz(Long postId, String question) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(conn -> {
            PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO post_quiz (post_id, question) VALUES (?, ?)",
                    new String[]{"id"});
            ps.setLong(1, postId);
            ps.setString(2, question);
            return ps;
        }, keyHolder);
        Number key = keyHolder.getKey();
        if (key == null) throw new IllegalStateException("Quiz insert failed");
        return key.longValue();
    }

    private void insertQuizOption(Long quizId, String optionText, boolean isCorrect, int displayOrder) {
        jdbcTemplate.update(
                "INSERT INTO post_quiz_option (quiz_id, option_text, is_correct, display_order) VALUES (?, ?, ?, ?)",
                quizId, optionText, isCorrect, displayOrder);
    }

    // ── Private helpers: enrichment ────────────────────────────────────────────

    private void enrichPost(Map<String, Object> post, Long currentUserId) {
        Long postId = ((Number) post.get("postId")).longValue();
        String postType = (String) post.getOrDefault("postType", "photo");
        if (postType == null) postType = "photo";

        if ("photo".equals(postType)) {
            List<String> images = jdbcTemplate.queryForList("""
                    SELECT image_url
                    FROM social_post_image
                    WHERE post_id = ?
                    ORDER BY display_order ASC
                    """, String.class, postId);
            post.put("images", images);
            post.put("imageUrl", images.isEmpty() ? null : images.get(0));
        } else {
            post.put("images", List.of());
            post.put("imageUrl", null);
        }

        if ("poll".equals(postType)) {
            post.put("poll", fetchPollForPost(postId, currentUserId));
        }
        if ("quiz".equals(postType)) {
            post.put("quiz", fetchQuizForPost(postId, currentUserId));
        }
        if ("video".equals(postType) || "file".equals(postType)) {
            post.put("attachment", fetchAttachmentForPost(postId));
        }

        Integer likeCount = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM post_like
                WHERE post_id = ?
                """, Integer.class, postId);
        post.put("likeCount", likeCount != null ? likeCount : 0);

        post.put("commentCount", countPostComments(postId));

        boolean likedByMe = false;
        if (currentUserId != null) {
            Integer cnt = jdbcTemplate.queryForObject("""
                    SELECT COUNT(*)
                    FROM post_like
                    WHERE user_id = ?
                      AND post_id = ?
                    """, Integer.class, currentUserId, postId);
            likedByMe = cnt != null && cnt > 0;
        }
        post.put("likedByMe", likedByMe);

        String friendLikedSignal = null;
        if (currentUserId != null) {
            List<String> friends = jdbcTemplate.queryForList("""
                    SELECT u.nickname
                    FROM user_follow f1
                    JOIN user_follow f2
                        ON f1.following_user_id = f2.follower_user_id
                        AND f2.following_user_id = f1.follower_user_id
                    JOIN post_like pl ON pl.user_id = f1.following_user_id
                    JOIN "USER" u ON u.id = f1.following_user_id
                    WHERE f1.follower_user_id = ?
                      AND pl.post_id = ?
                    LIMIT 3
                    """, String.class, currentUserId, postId);
            friendLikedSignal = formatFriendSignal(friends, "좋아요를 남겼어요.");
        }
        post.put("friendLikedSignal", friendLikedSignal);

        Long postUserId = ((Number) post.get("userId")).longValue();
        boolean isMyPost = currentUserId != null && currentUserId.equals(postUserId);
        boolean isFollowing = false;
        if (currentUserId != null && !isMyPost) {
            Integer cnt = jdbcTemplate.queryForObject("""
                    SELECT COUNT(*)
                    FROM user_follow
                    WHERE follower_user_id = ?
                      AND following_user_id = ?
                    """, Integer.class, currentUserId, postUserId);
            isFollowing = cnt != null && cnt > 0;
        }
        post.put("isMyPost", isMyPost);
        post.put("isFollowing", isFollowing);

        post.put("badgeCode", null);
        String selectedBadgeCode = badgeService.getSelectedBadgeCode(postUserId);
        if (selectedBadgeCode != null) {
            BadgeService.BadgeDef def = BadgeService.getBadgeDef(selectedBadgeCode);
            if (def != null) {
                post.put("badgeCode", selectedBadgeCode);
                post.put("badgeTier", def.tier());
                post.put("badgeName", def.name());
                post.put("badgeIcon", def.icon());
            }
        }

        Object createdAt = post.get("createdAt");
        if (createdAt instanceof Timestamp timestamp) {
            post.put("createdAtLabel", timestamp.toLocalDateTime().format(POST_DATE_FORMAT));
            post.put("createdAtMillis", timestamp.getTime());
            post.put("cursor", buildFeedCursor(timestamp, postId));
        } else if (createdAt != null) {
            post.put("createdAtLabel", createdAt.toString());
            post.put("createdAtMillis", null);
            post.put("cursor", null);
        } else {
            post.put("createdAtLabel", "");
            post.put("createdAtMillis", null);
            post.put("cursor", null);
        }
    }

    private Map<String, Object> fetchPollForPost(Long postId, Long currentUserId) {
        Map<String, Object> poll = jdbcTemplate.query(
                "SELECT id, question, is_multi_select FROM post_poll WHERE post_id = ?",
                rs -> {
                    if (!rs.next()) return null;
                    Map<String, Object> p = new LinkedHashMap<>();
                    p.put("pollId", rs.getLong("id"));
                    p.put("question", rs.getString("question"));
                    p.put("isMultiSelect", rs.getBoolean("is_multi_select"));
                    return p;
                }, postId);
        if (poll == null) return null;

        Long pollId = ((Number) poll.get("pollId")).longValue();
        Integer totalVotes = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM post_poll_vote WHERE poll_id = ?", Integer.class, pollId);
        poll.put("totalVotes", totalVotes != null ? totalVotes : 0);

        Long myVotedOptionId = null;
        if (currentUserId != null) {
            myVotedOptionId = jdbcTemplate.query(
                    "SELECT option_id FROM post_poll_vote WHERE poll_id = ? AND user_id = ?",
                    rs -> rs.next() ? rs.getLong("option_id") : null,
                    pollId, currentUserId);
        }
        poll.put("myVotedOptionId", myVotedOptionId);
        poll.put("hasVoted", myVotedOptionId != null);

        final Long finalMyVote = myVotedOptionId;
        List<Map<String, Object>> options = jdbcTemplate.query("""
                SELECT ppo.id, ppo.option_text, ppo.display_order, COUNT(ppv.id) AS voteCount
                FROM post_poll_option ppo
                LEFT JOIN post_poll_vote ppv ON ppv.option_id = ppo.id
                WHERE ppo.poll_id = ?
                GROUP BY ppo.id, ppo.option_text, ppo.display_order
                ORDER BY ppo.display_order
                """, (rs, rowNum) -> {
            Map<String, Object> opt = new LinkedHashMap<>();
            opt.put("optionId", rs.getLong("id"));
            opt.put("optionText", rs.getString("option_text"));
            opt.put("voteCount", rs.getInt("voteCount"));
            opt.put("isMyVote", finalMyVote != null && rs.getLong("id") == finalMyVote);
            return opt;
        }, pollId);

        poll.put("options", options);
        return poll;
    }

    private Map<String, Object> fetchQuizForPost(Long postId, Long currentUserId) {
        Map<String, Object> quiz = jdbcTemplate.query(
                "SELECT id, question FROM post_quiz WHERE post_id = ?",
                rs -> {
                    if (!rs.next()) return null;
                    Map<String, Object> q = new LinkedHashMap<>();
                    q.put("quizId", rs.getLong("id"));
                    q.put("question", rs.getString("question"));
                    return q;
                }, postId);
        if (quiz == null) return null;

        Long quizId = ((Number) quiz.get("quizId")).longValue();

        Long myAnswerOptionId = null;
        Boolean myAnswerCorrect = null;
        if (currentUserId != null) {
            Map<String, Object> answer = jdbcTemplate.query(
                    "SELECT option_id, is_correct FROM post_quiz_answer WHERE quiz_id = ? AND user_id = ?",
                    rs -> {
                        if (!rs.next()) return null;
                        Map<String, Object> a = new LinkedHashMap<>();
                        a.put("optionId", rs.getLong("option_id"));
                        a.put("isCorrect", rs.getBoolean("is_correct"));
                        return a;
                    }, quizId, currentUserId);
            if (answer != null) {
                myAnswerOptionId = ((Number) answer.get("optionId")).longValue();
                myAnswerCorrect = (Boolean) answer.get("isCorrect");
            }
        }
        quiz.put("myAnswerOptionId", myAnswerOptionId);
        quiz.put("myAnswerCorrect", myAnswerCorrect);
        quiz.put("hasAnswered", myAnswerOptionId != null);

        final Long finalMyAnswer = myAnswerOptionId;
        final boolean answered = myAnswerOptionId != null;
        List<Map<String, Object>> options = jdbcTemplate.query("""
                SELECT id, option_text, is_correct, display_order
                FROM post_quiz_option
                WHERE quiz_id = ?
                ORDER BY display_order
                """, (rs, rowNum) -> {
            Map<String, Object> opt = new LinkedHashMap<>();
            opt.put("optionId", rs.getLong("id"));
            opt.put("optionText", rs.getString("option_text"));
            opt.put("isMyAnswer", finalMyAnswer != null && rs.getLong("id") == finalMyAnswer);
            if (answered) {
                opt.put("isCorrect", rs.getBoolean("is_correct"));
            }
            return opt;
        }, quizId);

        quiz.put("options", options);
        return quiz;
    }

    private Map<String, Object> fetchAttachmentForPost(Long postId) {
        return jdbcTemplate.query("""
                SELECT file_url, file_name, file_type, file_size, attachment_type
                FROM post_attachment
                WHERE post_id = ?
                ORDER BY display_order
                LIMIT 1
                """, rs -> {
            if (!rs.next()) return null;
            Map<String, Object> att = new LinkedHashMap<>();
            att.put("fileUrl", rs.getString("file_url"));
            att.put("fileName", rs.getString("file_name"));
            att.put("fileType", rs.getString("file_type"));
            att.put("fileSize", rs.getLong("file_size"));
            att.put("attachmentType", rs.getString("attachment_type"));
            return att;
        }, postId);
    }

    private String formatFriendSignal(List<String> nicknames, String verb) {
        if (nicknames == null || nicknames.isEmpty()) return null;
        if (nicknames.size() == 1) return nicknames.get(0) + "님이 " + verb;
        if (nicknames.size() == 2) return nicknames.get(0) + "님, " + nicknames.get(1) + "님이 " + verb;
        return nicknames.get(0) + "님 외 " + (nicknames.size() - 1) + "명이 " + verb;
    }

    // ── Private helpers: feed ─────────────────────────────────────────────────

    private FeedSlice loadFeedSlice(String cursor, int limit, Long currentUserId) {
        int normalizedLimit = Math.max(1, Math.min(limit, FEED_MAX_PAGE_SIZE));
        FeedCursor feedCursor = parseFeedCursor(cursor);
        List<Object> params = new ArrayList<>();
        String cursorClause = "";
        if (feedCursor != null) {
            cursorClause = """
                      AND (
                            sp.created_at < ?
                         OR (sp.created_at = ? AND sp.id < ?)
                      )
                    """;
            Timestamp cursorTimestamp = new Timestamp(feedCursor.createdAtMillis());
            if (feedCursor.createdAtNanos() >= 0) {
                cursorTimestamp.setNanos(feedCursor.createdAtNanos());
            }
            params.add(cursorTimestamp);
            params.add(cursorTimestamp);
            params.add(feedCursor.postId());
        }
        params.add(normalizedLimit + 1);

        List<Map<String, Object>> rows = jdbcTemplate.query("""
                SELECT
                    sp.id AS postId,
                    sp.content,
                    sp.post_type AS postType,
                    sp.created_at AS createdAt,
                    sp.user_id AS userId,
                    u.nickname,
                    u.login_id AS loginId,
                    u.profile_image_url AS profileImageUrl,
                    m.movie_cd AS movieCode,
                    COALESCE(m.title, m.movie_name) AS movieTitle,
                    m.poster_image_url AS moviePosterUrl
                FROM social_post sp
                JOIN "USER" u ON u.id = sp.user_id
                JOIN movie m ON m.id = sp.movie_id
                WHERE sp.is_deleted = FALSE
                """ + cursorClause + """
                ORDER BY sp.created_at DESC, sp.id DESC
                LIMIT ?
                """, (rs, rowNum) -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("postId", rs.getLong("postId"));
            row.put("content", rs.getString("content"));
            row.put("postType", rs.getString("postType"));
            row.put("createdAt", rs.getTimestamp("createdAt"));
            row.put("userId", rs.getLong("userId"));
            row.put("nickname", rs.getString("nickname"));
            row.put("loginId", rs.getString("loginId"));
            row.put("profileImageUrl", rs.getString("profileImageUrl"));
            row.put("movieCode", rs.getString("movieCode"));
            row.put("movieTitle", rs.getString("movieTitle"));
            row.put("moviePosterUrl", rs.getString("moviePosterUrl"));
            return row;
        }, params.toArray());

        boolean hasMore = rows.size() > normalizedLimit;
        List<Map<String, Object>> posts = hasMore
                ? new ArrayList<>(rows.subList(0, normalizedLimit))
                : rows;
        for (Map<String, Object> post : posts) {
            try {
                enrichPost(post, currentUserId);
            } catch (Exception e) {
                log.warn("enrichPost failed for postId={}: {}", post.get("postId"), e.getMessage());
                post.putIfAbsent("images", List.of());
                post.putIfAbsent("likeCount", 0);
                post.putIfAbsent("commentCount", 0);
                post.putIfAbsent("likedByMe", false);
                post.putIfAbsent("isMyPost", false);
                post.putIfAbsent("isFollowing", false);
                post.putIfAbsent("badgeCode", null);
                post.putIfAbsent("createdAtLabel", "");
                post.putIfAbsent("cursor", null);
            }
        }

        String nextCursor = posts.isEmpty() ? null : (String) posts.getLast().getOrDefault("cursor", null);
        return new FeedSlice(posts, hasMore, nextCursor);
    }

    private List<Map<String, Object>> fetchFeedSuggestedUsers(Long currentUserId, int limit) {
        if (currentUserId == null) return List.of();
        return jdbcTemplate.queryForList("""
                SELECT
                    u.login_id          AS loginId,
                    u.nickname,
                    u.profile_image_url AS profileImageUrl,
                    EXISTS(
                        SELECT 1 FROM user_follow uf
                        WHERE uf.follower_user_id = ?
                          AND uf.following_user_id = u.id
                    ) AS followingByCurrentUser
                FROM "USER" u
                WHERE u.id <> ?
                ORDER BY (SELECT COUNT(*) FROM user_follow uf WHERE uf.following_user_id = u.id) DESC, u.id DESC
                LIMIT ?
                """, currentUserId, currentUserId, limit);
    }

    private List<Map<String, Object>> fetchStoryUsers(Long currentUserId, int limit) {
        if (currentUserId == null) return List.of();
        return jdbcTemplate.queryForList("""
                SELECT u.login_id          AS loginId,
                       u.nickname,
                       u.profile_image_url AS profileImageUrl,
                       EXISTS(
                           SELECT 1 FROM social_post sp
                           WHERE sp.user_id = u.id
                             AND sp.created_at >= DATEADD('DAY', -3, CURRENT_TIMESTAMP)
                       ) AS hasRecentPost
                FROM "USER" u
                JOIN user_follow uf ON uf.following_user_id = u.id
                WHERE uf.follower_user_id = ?
                ORDER BY hasRecentPost DESC, u.nickname
                LIMIT ?
                """, currentUserId, limit);
    }

    private FeedCursor parseFeedCursor(String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return null;
        }
        String[] parts = cursor.trim().split(":");
        if (parts.length != 2 && parts.length != 3) {
            return null;
        }
        try {
            if (parts.length == 3) {
                return new FeedCursor(Long.parseLong(parts[0]), Integer.parseInt(parts[1]), Long.parseLong(parts[2]));
            }
            return new FeedCursor(Long.parseLong(parts[0]), -1, Long.parseLong(parts[1]));
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private String buildFeedCursor(Timestamp createdAt, Long postId) {
        if (createdAt == null || postId == null) {
            return null;
        }
        return createdAt.getTime() + ":" + createdAt.getNanos() + ":" + postId;
    }

    // ── Private helpers: comments ─────────────────────────────────────────────

    private List<Map<String, Object>> fetchPostComments(Long postId, Long currentUserId) {
        long viewerId = currentUserId == null ? -1L : currentUserId;
        return jdbcTemplate.query("""
                SELECT
                    pc.id AS commentId,
                    pc.content,
                    pc.created_at AS createdAt,
                    pc.user_id AS userId,
                    u.nickname,
                    u.login_id AS loginId,
                    u.profile_image_url AS profileImageUrl
                FROM post_comment pc
                JOIN "USER" u ON u.id = pc.user_id
                WHERE pc.post_id = ?
                  AND pc.is_deleted = FALSE
                ORDER BY pc.created_at ASC, pc.id ASC
                """, (rs, rowNum) -> mapCommentRow(rs.getLong("commentId"),
                rs.getString("content"),
                rs.getTimestamp("createdAt"),
                rs.getLong("userId"),
                rs.getString("nickname"),
                rs.getString("loginId"),
                rs.getString("profileImageUrl"),
                viewerId), postId);
    }

    private Map<String, Object> fetchPostComment(Long commentId, Long currentUserId) {
        Map<String, Object> comment = jdbcTemplate.query("""
                SELECT
                    pc.id AS commentId,
                    pc.content,
                    pc.created_at AS createdAt,
                    pc.user_id AS userId,
                    u.nickname,
                    u.login_id AS loginId,
                    u.profile_image_url AS profileImageUrl
                FROM post_comment pc
                JOIN "USER" u ON u.id = pc.user_id
                WHERE pc.id = ?
                  AND pc.is_deleted = FALSE
                """, rs -> {
            if (!rs.next()) {
                return null;
            }
            return mapCommentRow(rs.getLong("commentId"),
                    rs.getString("content"),
                    rs.getTimestamp("createdAt"),
                    rs.getLong("userId"),
                    rs.getString("nickname"),
                    rs.getString("loginId"),
                    rs.getString("profileImageUrl"),
                    currentUserId == null ? -1L : currentUserId);
        }, commentId);
        if (comment == null) {
            throw new ResponseStatusException(NOT_FOUND);
        }
        return comment;
    }

    private Map<String, Object> mapCommentRow(Long commentId, String content, Timestamp createdAt,
            Long userId, String nickname, String loginId, String profileImageUrl, long currentUserId) {
        Map<String, Object> comment = new LinkedHashMap<>();
        comment.put("commentId", commentId);
        comment.put("content", content);
        comment.put("createdAt", createdAt == null ? "" : createdAt.toLocalDateTime().toString());
        comment.put("userId", userId);
        comment.put("nickname", nickname);
        comment.put("loginId", loginId);
        comment.put("profileImageUrl", profileImageUrl);
        comment.put("isMyComment", userId != null && userId == currentUserId);
        return comment;
    }

    private Long insertPostComment(Long postId, Long userId, String content) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(connection -> {
            PreparedStatement ps = connection.prepareStatement("""
                    INSERT INTO post_comment (post_id, user_id, content)
                    VALUES (?, ?, ?)
                    """, new String[]{"id"});
            ps.setLong(1, postId);
            ps.setLong(2, userId);
            ps.setString(3, content);
            return ps;
        }, keyHolder);
        Number key = keyHolder.getKey();
        if (key == null) {
            throw new IllegalStateException("Comment insert failed");
        }
        return key.longValue();
    }

    private Integer countPostComments(Long postId) {
        Integer count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM post_comment
                WHERE post_id = ?
                  AND is_deleted = FALSE
                """, Integer.class, postId);
        return count == null ? 0 : count;
    }

    // ── Private helpers: normalization ────────────────────────────────────────

    private String normalizeContent(String content) {
        String normalized = content == null ? "" : content.trim();
        return normalized.isEmpty() ? null : normalized;
    }

    private String normalizeCommentContent(String content) {
        String normalized = content == null ? "" : content.trim();
        if (normalized.isBlank()) {
            throw new ResponseStatusException(BAD_REQUEST, "댓글 내용을 입력해주세요.");
        }
        if (normalized.length() > 1000) {
            throw new ResponseStatusException(BAD_REQUEST, "댓글은 최대 1000자까지 입력할 수 있습니다.");
        }
        return normalized;
    }

    private String extractExtension(String originalFilename) {
        if (originalFilename == null) {
            throw new ResponseStatusException(BAD_REQUEST);
        }
        String sanitized = originalFilename.replace("\\", "").replace("/", "").trim();
        int dotIndex = sanitized.lastIndexOf('.');
        if (dotIndex < 0 || dotIndex == sanitized.length() - 1) {
            throw new ResponseStatusException(BAD_REQUEST, "Missing file extension");
        }
        return sanitized.substring(dotIndex + 1).toLowerCase(Locale.ROOT);
    }

    // ── Table initialization ──────────────────────────────────────────────────

    private void initializeSocialTables() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS social_post (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    movie_id BIGINT NOT NULL,
                    content TEXT,
                    post_type VARCHAR(20) NOT NULL DEFAULT 'photo',
                    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """);
        try {
            jdbcTemplate.execute("ALTER TABLE social_post ADD COLUMN post_type VARCHAR(20) NOT NULL DEFAULT 'photo'");
        } catch (Exception ignored) {}

        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS social_post_image (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    post_id BIGINT NOT NULL,
                    image_url VARCHAR(500) NOT NULL,
                    display_order INT NOT NULL DEFAULT 0,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """);
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS post_like (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    post_id BIGINT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT uk_post_like UNIQUE (user_id, post_id)
                )
                """);
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS post_comment (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    post_id BIGINT NOT NULL,
                    user_id BIGINT NOT NULL,
                    content TEXT NOT NULL,
                    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """);
        jdbcTemplate.execute("""
                CREATE INDEX IF NOT EXISTS idx_post_comment_post_created
                ON post_comment (post_id, created_at)
                """);
        jdbcTemplate.execute("""
                CREATE INDEX IF NOT EXISTS idx_post_comment_user_created
                ON post_comment (user_id, created_at)
                """);
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS post_attachment (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    post_id BIGINT NOT NULL,
                    file_url VARCHAR(500) NOT NULL,
                    file_name VARCHAR(500),
                    file_type VARCHAR(100),
                    file_size BIGINT,
                    attachment_type VARCHAR(20) NOT NULL DEFAULT 'file',
                    display_order INT NOT NULL DEFAULT 0,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """);
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS post_poll (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    post_id BIGINT NOT NULL,
                    question TEXT NOT NULL,
                    is_multi_select BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """);
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS post_poll_option (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    poll_id BIGINT NOT NULL,
                    option_text VARCHAR(500) NOT NULL,
                    display_order INT NOT NULL DEFAULT 0
                )
                """);
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS post_poll_vote (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    poll_id BIGINT NOT NULL,
                    option_id BIGINT NOT NULL,
                    user_id BIGINT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT uk_poll_vote UNIQUE (poll_id, user_id)
                )
                """);
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS post_quiz (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    post_id BIGINT NOT NULL,
                    question TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """);
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS post_quiz_option (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    quiz_id BIGINT NOT NULL,
                    option_text VARCHAR(500) NOT NULL,
                    is_correct BOOLEAN NOT NULL DEFAULT FALSE,
                    display_order INT NOT NULL DEFAULT 0
                )
                """);
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS post_quiz_answer (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    quiz_id BIGINT NOT NULL,
                    option_id BIGINT NOT NULL,
                    user_id BIGINT NOT NULL,
                    is_correct BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT uk_quiz_answer UNIQUE (quiz_id, user_id)
                )
                """);
    }

    // ── Records ───────────────────────────────────────────────────────────────

    private record FeedSlice(List<Map<String, Object>> posts, boolean hasMore, String nextCursor) {}

    private record FeedCursor(long createdAtMillis, int createdAtNanos, long postId) {}
}
