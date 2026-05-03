package com.cinematch;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.server.ResponseStatusException;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.HttpStatus.UNAUTHORIZED;

import com.cinematch.chart.ChartEntry;
import com.cinematch.chart.ChartMovieRow;
import com.cinematch.chart.ChartRegistry;
import com.cinematch.recommendation.CollaborativeLifeMovieRecommendationService;
import com.cinematch.recommendation.RecommendationRefreshStateService;
import com.cinematch.recommendation.RecommendationBlockService;
import com.cinematch.recommendation.RecommendationMaintenanceService;
import com.cinematch.tag.RecommendationTag;
import com.cinematch.tag.RecommendationTagType;
import com.cinematch.kobis.KobisBoxOfficeService;
import com.cinematch.notification.NotificationService;

@Controller
public class MovieController {

    private static final String LOGIN_SESSION_KEY = "loginUserId";
    private static final String LOGIN_NICKNAME_SESSION_KEY = "loginUserNickname";
    private static final int PAGE_SIZE = 10;
    private static final int HOME_BLOCK_ITEM_LIMIT = 10;
    private static final int LIFE_MOVIE_LIMIT = 10;
    private static final int SOCIAL_PAGE_LIMIT = 24;
    private static final Set<String> SEARCH_EXCLUDED_GENRES = Set.of("공연", "공연실황", "콘서트", "라이브");
    private static final Map<String, String> TAG_TYPE_LABELS = Map.of(
            "MOOD", "분위기",
            "CONTEXT", "추천 상황",
            "CAUTION", "주의 요소",
            "THEME", "주제"
    );
    private static final Map<String, String> TAG_LABELS = Map.ofEntries(
            Map.entry("funny", "유쾌한"),
            Map.entry("tense", "긴장감 있는"),
            Map.entry("dark", "어두운"),
            Map.entry("emotional", "감정적인"),
            Map.entry("romantic", "로맨틱한"),
            Map.entry("hopeful", "희망적인"),
            Map.entry("healing", "힐링되는"),
            Map.entry("spectacle", "볼거리 있는"),
            Map.entry("creepy", "기괴한"),
            Map.entry("with_family", "가족과 보기 좋은"),
            Map.entry("with_partner", "연인과 보기 좋은"),
            Map.entry("late_night", "늦은 밤 보기 좋은"),
            Map.entry("violent", "폭력적인"),
            Map.entry("sad", "슬픈"),
            Map.entry("slow_burn", "잔잔하게 쌓이는"),
            Map.entry("long_running", "러닝타임 긴"),
            Map.entry("investigation", "수사"),
            Map.entry("mystery", "미스터리"),
            Map.entry("zombie", "좀비"),
            Map.entry("disaster", "재난"),
            Map.entry("true_story", "실화 기반"),
            Map.entry("coming_of_age", "성장"),
            Map.entry("friendship", "우정"),
            Map.entry("survival", "생존"),
            Map.entry("revenge", "복수")
    );

    private static final List<String> HOME_CHART_CODES = List.of("top-sales", "million-club", "flash-hit");

    private final JdbcTemplate jdbcTemplate;
    private final RecommendationRefreshStateService recommendationRefreshStateService;
    private final RecommendationMaintenanceService recommendationMaintenanceService;
    private final RecommendationBlockService recommendationBlockService;
    private final CollaborativeLifeMovieRecommendationService collaborativeLifeMovieRecommendationService;
    private final ChartRegistry chartRegistry;
    private final KobisBoxOfficeService kobisBoxOfficeService;
    private final NotificationService notificationService;

    public MovieController(JdbcTemplate jdbcTemplate,
                           RecommendationRefreshStateService recommendationRefreshStateService,
                           RecommendationMaintenanceService recommendationMaintenanceService,
                           RecommendationBlockService recommendationBlockService,
                           CollaborativeLifeMovieRecommendationService collaborativeLifeMovieRecommendationService,
                           ChartRegistry chartRegistry,
                           KobisBoxOfficeService kobisBoxOfficeService,
                           NotificationService notificationService) {
        this.jdbcTemplate = jdbcTemplate;
        this.recommendationRefreshStateService = recommendationRefreshStateService;
        this.recommendationMaintenanceService = recommendationMaintenanceService;
        this.recommendationBlockService = recommendationBlockService;
        this.collaborativeLifeMovieRecommendationService = collaborativeLifeMovieRecommendationService;
        this.chartRegistry = chartRegistry;
        this.kobisBoxOfficeService = kobisBoxOfficeService;
        this.notificationService = notificationService;
    }

    @GetMapping("/")
    public String root() {
        return "redirect:/login";
    }

    @GetMapping("/login")
    public String loginPage(@RequestParam(required = false) String error, Model model) {
        if (error != null) {
            model.addAttribute("loginError", "등록된 사용자가 아닙니다. ID와 PW를 다시 확인해주세요.");
        }
        return "login-page";
    }

    @GetMapping("/signup")
    public String signupPage(@RequestParam(required = false) String error, Model model) {
        if (error != null) {
            model.addAttribute("signupError", "이미 사용 중인 ID이거나 입력값이 올바르지 않습니다.");
        }
        return "signup-page";
    }

    @PostMapping("/login")
    public String login(@RequestParam String id, @RequestParam String pw, HttpSession session, Model model) {
        LoginUser loginUser = jdbcTemplate.query("""
                SELECT login_id, nickname
                FROM "USER"
                WHERE login_id = ? AND login_pw = ?
                """, rs -> rs.next() ? new LoginUser(rs.getString("login_id"), rs.getString("nickname")) : null, id, pw);

        if (loginUser == null) {
            return "redirect:/login?error=1";
        }

        session.setAttribute(LOGIN_SESSION_KEY, loginUser.loginId());
        session.setAttribute(LOGIN_NICKNAME_SESSION_KEY, loginUser.nickname());
        return "redirect:/charts";
    }

    @PostMapping("/signup")
    public String signup(@RequestParam String id, @RequestParam String pw, @RequestParam String nickname,
                         @RequestParam String gender, @RequestParam Integer age) {
        if (nickname == null || nickname.isBlank()
                || (!gender.equals("MALE") && !gender.equals("FEMALE"))
                || age == null || age < 1 || age > 120) {
            return "redirect:/signup?error=1";
        }

        Integer existingUserCount = jdbcTemplate.query("""
                SELECT COUNT(*)
                FROM "USER"
                WHERE login_id = ?
                """, rs -> rs.next() ? rs.getInt(1) : 0, id);

        if (existingUserCount != null && existingUserCount > 0) {
            return "redirect:/signup?error=1";
        }

        jdbcTemplate.update("""
                INSERT INTO "USER" (login_id, login_pw, nickname, gender, age)
                VALUES (?, ?, ?, ?, ?)
                """, id, pw, nickname.trim(), gender, age);

        return "redirect:/login";
    }

    @GetMapping("/logout")
    public String logout(HttpSession session) {
        session.invalidate();
        return "redirect:/login";
    }

    @GetMapping("/charts")
    public String home(@RequestParam(defaultValue = "1") int page,
                       @RequestParam(required = false) String query,
                       @RequestParam(required = false) String genre,
                       @RequestParam(required = false) String tagType,
                       @RequestParam(required = false) String tagName,
                       @RequestParam(required = false, defaultValue = "false") boolean showAdvanced,
                       Model model, HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/login";
        }

        String normalizedQuery = query == null ? "" : query.trim();
        String normalizedGenre = genre == null ? "" : genre.trim();
        if (isSearchExcludedGenre(normalizedGenre)) {
            normalizedGenre = "";
        }
        String normalizedTagType = tagType == null ? "" : tagType.trim().toUpperCase();
        String normalizedTagName = tagName == null ? "" : tagName.trim();
        if (normalizedTagType.isEmpty() || normalizedTagName.isEmpty()) {
            normalizedTagType = "";
            normalizedTagName = "";
        }
        boolean hasQuery = !normalizedQuery.isEmpty();
        boolean hasGenre = !normalizedGenre.isEmpty();
        boolean hasTag = !normalizedTagType.isEmpty();
        boolean hasActiveSearch = hasQuery || hasGenre || hasTag;

        addCurrentUserAttributes(model, session);
        model.addAttribute("query", normalizedQuery);
        model.addAttribute("selectedGenre", normalizedGenre);
        model.addAttribute("selectedTagType", normalizedTagType);
        model.addAttribute("selectedTagName", normalizedTagName);
        model.addAttribute("selectedTagLabel", labelForTag(normalizedTagName));
        model.addAttribute("showAdvanced", showAdvanced || hasGenre || hasTag);
        model.addAttribute("genres", fetchChartGenres());
        model.addAttribute("tagGroups", fetchChartTagGroups());
        model.addAttribute("hasActiveSearch", hasActiveSearch);

        if (hasActiveSearch) {
            String countSql = """
                    SELECT COUNT(DISTINCT m.id)
                    FROM movie m
                    LEFT JOIN movie_genre mg ON mg.movie_id = m.id
                    LEFT JOIN genre g ON g.id = mg.genre_id
                    WHERE (? = '' OR UPPER(COALESCE(m.title, m.movie_name, '')) LIKE UPPER(?)
                           OR UPPER(COALESCE(m.movie_name, '')) LIKE UPPER(?)
                           OR UPPER(COALESCE(m.movie_name_en, m.original_title, m.movie_name_original, '')) LIKE UPPER(?)
                           OR UPPER(COALESCE(m.movie_name_original, m.original_title, '')) LIKE UPPER(?)
                           OR UPPER(m.movie_cd) LIKE UPPER(?))
                      AND (? = '' OR g.name = ?)
                      AND (? = '' OR EXISTS (
                            SELECT 1
                            FROM movie_tag mt
                            JOIN tag t ON t.id = mt.tag_id
                            WHERE mt.movie_id = m.id
                              AND t.tag_type = ?
                              AND t.tag_name = ?
                      ))
                    """ + searchExcludedGenreClause();
            String queryPattern = "%" + normalizedQuery + "%";
            java.util.ArrayList<Object> countParams = new java.util.ArrayList<>(List.of(
                    normalizedQuery, queryPattern, queryPattern, queryPattern, queryPattern, queryPattern,
                    normalizedGenre, normalizedGenre,
                    normalizedTagName, normalizedTagType, normalizedTagName
            ));
            countParams.addAll(SEARCH_EXCLUDED_GENRES);
            int totalMovies = jdbcTemplate.queryForObject(countSql, Integer.class, countParams.toArray());
            int totalPages = Math.max(1, (int) Math.ceil((double) totalMovies / PAGE_SIZE));
            int currentPage = Math.min(Math.max(page, 1), totalPages);
            int offset = (currentPage - 1) * PAGE_SIZE;

            String movieSearchSql = """
                    SELECT DISTINCT
                        m.ranking,
                        m.movie_cd,
                        COALESCE(m.title, m.movie_name) AS movie_name,
                        COALESCE(m.movie_name_en, m.original_title, m.movie_name_original) AS movie_name_en,
                        m.poster_image_url
                    FROM movie m
                    LEFT JOIN movie_genre mg ON mg.movie_id = m.id
                    LEFT JOIN genre g ON g.id = mg.genre_id
                    WHERE (? = '' OR UPPER(COALESCE(m.title, m.movie_name, '')) LIKE UPPER(?)
                           OR UPPER(COALESCE(m.movie_name, '')) LIKE UPPER(?)
                           OR UPPER(COALESCE(m.movie_name_en, m.original_title, m.movie_name_original, '')) LIKE UPPER(?)
                           OR UPPER(COALESCE(m.movie_name_original, m.original_title, '')) LIKE UPPER(?)
                           OR UPPER(m.movie_cd) LIKE UPPER(?))
                      AND (? = '' OR g.name = ?)
                      AND (? = '' OR EXISTS (
                            SELECT 1
                            FROM movie_tag mt
                            JOIN tag t ON t.id = mt.tag_id
                            WHERE mt.movie_id = m.id
                              AND t.tag_type = ?
                              AND t.tag_name = ?
                      ))
                    ORDER BY
                        CASE WHEN m.ranking IS NULL THEN 1 ELSE 0 END,
                        m.ranking ASC,
                        COALESCE(m.title, m.movie_name) ASC
                    LIMIT ? OFFSET ?
                    """ + searchExcludedGenreClause();

            java.util.ArrayList<Object> movieSearchParams = new java.util.ArrayList<>(List.of(
                    normalizedQuery, queryPattern, queryPattern, queryPattern, queryPattern, queryPattern,
                    normalizedGenre, normalizedGenre,
                    normalizedTagName, normalizedTagType, normalizedTagName
            ));
            movieSearchParams.addAll(SEARCH_EXCLUDED_GENRES);
            movieSearchParams.add(PAGE_SIZE);
            movieSearchParams.add(offset);

            List<MoviePosterView> movies = jdbcTemplate.query(movieSearchSql, (rs, rowNum) -> new MoviePosterView(
                    rs.getInt("ranking"),
                    rs.getString("movie_cd"),
                    rs.getString("movie_name"),
                    rs.getString("movie_name_en"),
                    rs.getString("poster_image_url")
            ), movieSearchParams.toArray());

            model.addAttribute("movies", movies);
            model.addAttribute("searchResultCount", totalMovies);
            model.addAttribute("currentPage", currentPage);
            model.addAttribute("totalPages", totalPages);
            model.addAttribute("hasPrevious", currentPage > 1);
            model.addAttribute("hasNext", currentPage < totalPages);
            model.addAttribute("previousPage", currentPage - 1);
            model.addAttribute("nextPage", currentPage + 1);
            model.addAttribute("pages", java.util.stream.IntStream.rangeClosed(1, totalPages).boxed().toList());
            model.addAttribute("recommendationBlocks", List.of());
            model.addAttribute("primaryRecommendationBlock", null);
            model.addAttribute("collaborativeLifeBlock", null);
            model.addAttribute("secondaryRecommendationBlocks", List.of());
            model.addAttribute("collaborativeLifeRepresentative", null);
            model.addAttribute("fallbackMovies", List.of());
            model.addAttribute("homeChartSections", List.of());
            model.addAttribute("trendingMovies", List.of());
        } else {
            Long userId = getCurrentUserId(session);
            recommendationMaintenanceService.ensureRecommendations(userId, 200);
            List<RecommendationBlockService.RecommendationBlock> recommendationBlocks =
                    recommendationBlockService.buildBlocks(userId, 120, HOME_BLOCK_ITEM_LIMIT).blocks();
            RecommendationBlockService.RecommendationBlock primaryRecommendationBlock = recommendationBlocks.stream()
                    .filter(block -> "PERSONALIZED".equals(block.key()))
                    .findFirst()
                    .orElse(null);
            List<RecommendationBlockService.RecommendationBlock> secondaryRecommendationBlocks = recommendationBlocks.stream()
                    .filter(block -> !"PERSONALIZED".equals(block.key()))
                    .toList();
            CollaborativeLifeMovieRecommendationService.CollaborativeLifeSection collaborativeLifeSection =
                    collaborativeLifeMovieRecommendationService.buildSection(userId).orElse(null);
            RecommendationBlockService.RecommendationBlock collaborativeLifeBlock =
                    collaborativeLifeSection == null ? null : collaborativeLifeSection.block();

            model.addAttribute("movies", List.of());
            model.addAttribute("searchResultCount", 0);
            model.addAttribute("currentPage", 1);
            model.addAttribute("totalPages", 1);
            model.addAttribute("hasPrevious", false);
            model.addAttribute("hasNext", false);
            model.addAttribute("previousPage", 1);
            model.addAttribute("nextPage", 1);
            model.addAttribute("pages", List.of(1));
            model.addAttribute("recommendationBlocks", recommendationBlocks);
            model.addAttribute("primaryRecommendationBlock", primaryRecommendationBlock);
            model.addAttribute("collaborativeLifeBlock", collaborativeLifeBlock);
            model.addAttribute("collaborativeLifeRepresentative",
                    collaborativeLifeSection == null ? null : collaborativeLifeSection.representativeUser());
            model.addAttribute("secondaryRecommendationBlocks", secondaryRecommendationBlocks);
            model.addAttribute("fallbackMovies", recommendationBlocks.isEmpty() ? fetchPopularMovies(12) : List.of());
            model.addAttribute("trendingMovies", kobisBoxOfficeService.fetchBoxOffice(10).stream()
                    .filter(movie -> movie.posterImageUrl() != null && !movie.posterImageUrl().isBlank())
                    .toList());

            List<HomeChartSectionView> homeChartSections = HOME_CHART_CODES.stream()
                    .flatMap(code -> chartRegistry.find(code).stream())
                    .map(a -> new HomeChartSectionView(ChartEntry.of(a), a.fetch(10)))
                    .toList();
            model.addAttribute("homeChartSections", homeChartSections);
        }
        return "index";
    }

    @GetMapping("/users/{loginId}")
    public String userProfilePage(@PathVariable String loginId, Model model, HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/login";
        }

        initializeActivityTables();
        Long currentUserId = getCurrentUserId(session);
        String normalizedLoginId = loginId == null ? "" : loginId.trim();
        if (normalizedLoginId.isBlank()) {
            throw new ResponseStatusException(NOT_FOUND);
        }

        String currentLoginId = (String) session.getAttribute(LOGIN_SESSION_KEY);
        if (normalizedLoginId.equalsIgnoreCase(currentLoginId)) {
            return "redirect:/mypage";
        }

        SocialProfileView targetUser = fetchSocialProfile(normalizedLoginId, currentUserId);
        if (targetUser == null) {
            throw new ResponseStatusException(NOT_FOUND);
        }

        addCurrentUserAttributes(model, session);
        model.addAttribute("targetUser", targetUser);
        model.addAttribute("lifeMovies", fetchLifeMovies(targetUser.userId(), LIFE_MOVIE_LIMIT));
        model.addAttribute("lifeMovieCount", countLifeMovies(targetUser.userId()));
        model.addAttribute("watchingMovies", fetchWatchedMovies(targetUser.userId(), "WATCHING", LIFE_MOVIE_LIMIT));
        model.addAttribute("watchingMovieCount", countWatchedMovies(targetUser.userId(), "WATCHING"));
        model.addAttribute("watchedMovies", fetchWatchedMovies(targetUser.userId(), "WATCHED", LIFE_MOVIE_LIMIT));
        model.addAttribute("watchedMovieCount", countWatchedMovies(targetUser.userId(), "WATCHED"));
        model.addAttribute("likedMovies", fetchLikedMovies(targetUser.userId(), LIFE_MOVIE_LIMIT));
        model.addAttribute("likedMovieCount", countLikedMovies(targetUser.userId()));
        model.addAttribute("profileRedirectPath", "/users/" + targetUser.loginId());
        return "user-profile";
    }

    @GetMapping("/mypage")
    public String myPage(@RequestParam(required = false, defaultValue = "false") boolean showLifePicker,
                         Model model, HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/login";
        }

        initializeActivityTables();
        Long userId = getCurrentUserId(session);
        UserProfileView userProfile = jdbcTemplate.query("""
                SELECT login_id, nickname, gender, age, profile_image_url
                FROM "USER"
                WHERE login_id = ?
                """, rs -> rs.next()
                ? new UserProfileView(
                        rs.getString("login_id"),
                        rs.getString("nickname"),
                        rs.getString("gender"),
                        rs.getInt("age"),
                        rs.getString("profile_image_url"))
                : null, session.getAttribute(LOGIN_SESSION_KEY));

        if (userProfile == null) {
            session.invalidate();
            return "redirect:/login";
        }

        int lifeMovieCount = countLifeMovies(userId);
        long followerCount = countFollowers(userId);
        long followingCount = countFollowing(userId);
        addCurrentUserAttributes(model, session);
        model.addAttribute("userProfile", userProfile);
        model.addAttribute("followerCount", followerCount);
        model.addAttribute("followingCount", followingCount);
        model.addAttribute("socialFollowerUsers", fetchFollowerUsers(userId, userId, SOCIAL_PAGE_LIMIT));
        model.addAttribute("socialFollowingUsers", fetchFollowingUsers(userId, userId, SOCIAL_PAGE_LIMIT));
        model.addAttribute("lifeMovies", fetchLifeMovies(userId, LIFE_MOVIE_LIMIT));
        model.addAttribute("lifeMovieCount", lifeMovieCount);
        model.addAttribute("likedMovies", fetchLikedMovies(userId, 12));
        model.addAttribute("likedMovieCount", countLikedMovies(userId));
        model.addAttribute("collectionMovies", fetchCollectionMovies(userId, 12));
        model.addAttribute("collectionMovieCount", countCollectionMovies(userId));
        model.addAttribute("laterMovies", fetchStoredMovies(userId, 12));
        model.addAttribute("laterMovieCount", countStoredMovies(userId));
        model.addAttribute("dislikedMovies", fetchDislikedMovies(userId, 12));
        model.addAttribute("dislikedMovieCount", countDislikedMovies(userId));
        model.addAttribute("watchingMovies", fetchWatchedMovies(userId, "WATCHING", 12));
        model.addAttribute("watchingMovieCount", countWatchedMovies(userId, "WATCHING"));
        model.addAttribute("watchedMovies", fetchWatchedMovies(userId, "WATCHED", 12));
        model.addAttribute("watchedMovieCount", countWatchedMovies(userId, "WATCHED"));
        model.addAttribute("showLifePicker", showLifePicker);
        model.addAttribute("lifeLimitReached", lifeMovieCount >= LIFE_MOVIE_LIMIT);
        model.addAttribute("lifeSelectionSlots", Math.max(0, LIFE_MOVIE_LIMIT - lifeMovieCount));
        model.addAttribute("lifePickerCandidates", fetchLifeMovieSearchResults(userId));
        return "my-page";
    }

    @GetMapping("/people")
    public String peoplePage(@RequestParam(required = false) String user,
                             @RequestParam(required = false) String query,
                             @RequestParam(required = false, defaultValue = "followers") String view,
                             Model model, HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/login";
        }

        initializeActivityTables();
        Long currentUserId = getCurrentUserId(session);
        String currentLoginId = (String) session.getAttribute(LOGIN_SESSION_KEY);
        String targetLoginId = (user == null || user.isBlank()) ? currentLoginId : user.trim();
        String normalizedView = normalizePeopleView(view);
        String normalizedQuery = query == null ? "" : query.trim();

        SocialProfileView targetUser = fetchSocialProfile(targetLoginId, currentUserId);
        if (targetUser == null) {
            throw new ResponseStatusException(NOT_FOUND);
        }

        addCurrentUserAttributes(model, session);
        model.addAttribute("targetUser", targetUser);
        model.addAttribute("selectedView", normalizedView);
        model.addAttribute("searchQuery", normalizedQuery);
        model.addAttribute("searchResults", normalizedQuery.isBlank()
                ? fetchSuggestedUsers(currentUserId, targetUser.userId(), SOCIAL_PAGE_LIMIT)
                : searchUsers(currentUserId, normalizedQuery, SOCIAL_PAGE_LIMIT));
        model.addAttribute("relationshipUsers", "following".equals(normalizedView)
                ? fetchFollowingUsers(targetUser.userId(), currentUserId, SOCIAL_PAGE_LIMIT)
                : fetchFollowerUsers(targetUser.userId(), currentUserId, SOCIAL_PAGE_LIMIT));
        model.addAttribute("relationshipTitle", "following".equals(normalizedView) ? "팔로잉" : "팔로워");
        model.addAttribute("searchSectionTitle", normalizedQuery.isBlank() ? "추천할 만한 사용자" : "검색 결과");
        return "people-page";
    }

    @GetMapping("/stored")
    public String storedMovies(Model model, HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/login";
        }

        initializeActivityTables();
        Long userId = getCurrentUserId(session);
        addCurrentUserAttributes(model, session);
        model.addAttribute("laterMovies", fetchStoredMovies(userId, null));
        return "stored-page";
    }

    @GetMapping("/movies/{movieCode}")
    public String detail(@PathVariable String movieCode, Model model, HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/login";
        }

        initializeActivityTables();
        Long userId = getCurrentUserId(session);
        Long movieId = findMovieId(movieCode);
        MovieActionState actionState = fetchMovieActionState(userId, movieId);
        MovieDetailView movie = jdbcTemplate.query("""
                SELECT
                    m.ranking,
                    m.movie_cd,
                    COALESCE(m.title, m.movie_name) AS movie_name,
                    COALESCE(m.movie_name_en, m.original_title, m.movie_name_original) AS movie_name_en,
                    COALESCE(m.movie_name_original, m.original_title) AS movie_name_original,
                    m.poster_image_url,
                    (
                        SELECT COUNT(*)
                        FROM user_movie_like uml
                        WHERE uml.movie_id = m.id AND uml.liked = TRUE
                    ) AS like_count,
                    m.box_office_open_date,
                    m.movie_info_open_date,
                    m.production_year,
                    m.show_time,
                    m.box_office_sales_acc,
                    m.box_office_audi_acc,
                    m.box_office_scrn_cnt,
                    m.box_office_show_cnt,
                    mt.name AS type_name,
                    ps.name AS production_status_name,
                    n.name AS nation_name
                FROM movie m
                LEFT JOIN movie_type mt ON mt.id = m.movie_type_id
                LEFT JOIN production_status ps ON ps.id = m.production_status_id
                LEFT JOIN nation n ON n.id = m.nation_id
                WHERE m.movie_cd = ?
                """, rs -> {
            if (!rs.next()) {
                return null;
            }
            return new MovieDetailView(
                    rs.getInt("ranking"),
                    rs.getString("movie_cd"),
                    rs.getString("movie_name"),
                    rs.getString("movie_name_en"),
                    rs.getString("movie_name_original"),
                    rs.getString("poster_image_url"),
                    rs.getInt("like_count"),
                    actionState.liked(),
                    actionState.disliked(),
                    actionState.stored(),
                    actionState.collected(),
                    actionState.watchStatus(),
                    actionState.watchedRating(),
                    rs.getObject("box_office_open_date", LocalDate.class),
                    rs.getObject("movie_info_open_date", LocalDate.class),
                    rs.getObject("production_year", Integer.class),
                    rs.getObject("show_time", Integer.class),
                    rs.getString("type_name"),
                    rs.getString("production_status_name"),
                    rs.getString("nation_name"),
                    rs.getObject("box_office_sales_acc", Long.class),
                    rs.getObject("box_office_audi_acc", Long.class),
                    rs.getObject("box_office_scrn_cnt", Integer.class),
                    rs.getObject("box_office_show_cnt", Integer.class)
            );
        }, movieCode);

        if (movie == null) {
            throw new ResponseStatusException(NOT_FOUND);
        }

        model.addAttribute("movie", movie);
        addCurrentUserAttributes(model, session);
        model.addAttribute("genres", fetchOrderedNames("""
                SELECT g.name
                FROM movie_genre mg
                JOIN genre g ON g.id = mg.genre_id
                JOIN movie m ON m.id = mg.movie_id
                WHERE m.movie_cd = ?
                ORDER BY mg.display_order
                """, movieCode));
        model.addAttribute("recommendationTags", jdbcTemplate.query("""
                SELECT t.tag_type, t.tag_name
                FROM movie_tag mt
                JOIN tag t ON t.id = mt.tag_id
                JOIN movie m ON m.id = mt.movie_id
                WHERE m.movie_cd = ?
                ORDER BY
                    CASE t.tag_type
                        WHEN 'MOOD' THEN 1
                        WHEN 'CONTEXT' THEN 2
                        WHEN 'CAUTION' THEN 3
                        WHEN 'THEME' THEN 4
                        ELSE 5
                    END,
                    t.tag_name
                """, (rs, rowNum) -> new MovieTagView(
                rs.getString("tag_type"),
                labelForTagType(rs.getString("tag_type")),
                rs.getString("tag_name"),
                labelForTag(rs.getString("tag_name"))
        ), movieCode));
        model.addAttribute("directors", fetchOrderedNames("""
                SELECT p.name
                FROM movie_director md
                JOIN person p ON p.id = md.person_id
                JOIN movie m ON m.id = md.movie_id
                WHERE m.movie_cd = ?
                ORDER BY md.display_order
                """, movieCode));
        model.addAttribute("actors", fetchOrderedNames("""
                SELECT p.name
                FROM movie_actor ma
                JOIN person p ON p.id = ma.person_id
                JOIN movie m ON m.id = ma.movie_id
                WHERE m.movie_cd = ?
                ORDER BY ma.display_order
                """, movieCode));
        model.addAttribute("companies", jdbcTemplate.query("""
                SELECT c.name, mc.company_role
                FROM movie_company mc
                JOIN company c ON c.id = mc.company_id
                JOIN movie m ON m.id = mc.movie_id
                WHERE m.movie_cd = ?
                ORDER BY mc.display_order
                """, (rs, rowNum) -> new CompanyView(
                rs.getString("name"),
                rs.getString("company_role")
        ), movieCode));
        model.addAttribute("providers", jdbcTemplate.query("""
                SELECT
                    p.provider_name,
                    CASE mp.provider_type
                        WHEN 'FLATRATE' THEN '구독'
                        WHEN 'RENT' THEN '대여'
                        WHEN 'BUY' THEN '구매'
                        ELSE mp.provider_type
                    END AS provider_type
                FROM movie_provider mp
                JOIN provider p ON p.id = mp.provider_id
                JOIN movie m ON m.id = mp.movie_id
                WHERE m.movie_cd = ?
                ORDER BY
                    CASE mp.provider_type
                        WHEN 'FLATRATE' THEN 1
                        WHEN 'RENT' THEN 2
                        WHEN 'BUY' THEN 3
                        ELSE 4
                    END,
                    mp.display_order
                """, (rs, rowNum) -> new ProviderView(
                rs.getString("provider_name"),
                rs.getString("provider_type")
        ), movieCode));
        model.addAttribute("audits", jdbcTemplate.query("""
                SELECT a.audit_no, wg.name AS watch_grade_name
                FROM movie_audit ma
                LEFT JOIN audit a ON a.id = ma.audit_id
                LEFT JOIN watch_grade wg ON wg.id = ma.watch_grade_id
                JOIN movie m ON m.id = ma.movie_id
                WHERE m.movie_cd = ?
                ORDER BY ma.display_order
                """, (rs, rowNum) -> new AuditView(
                rs.getString("audit_no"),
                rs.getString("watch_grade_name")
        ), movieCode));
        return "movie-detail";
    }

    @PostMapping("/movies/{movieCode}/like")
    @Transactional
    public String toggleLike(@PathVariable String movieCode, HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/login";
        }
        applyLikeState(getCurrentUserId(session), findMovieId(movieCode), true);
        return "redirect:/movies/" + movieCode;
    }

    @PostMapping("/movies/{movieCode}/dislike")
    @Transactional
    public String toggleDislike(@PathVariable String movieCode, HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/login";
        }
        applyLikeState(getCurrentUserId(session), findMovieId(movieCode), false);
        return "redirect:/movies/" + movieCode;
    }

    @PostMapping("/movies/{movieCode}/store")
    @Transactional
    public String toggleStore(@PathVariable String movieCode, HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/login";
        }
        toggleSimpleCollection(getCurrentUserId(session), findMovieId(movieCode), "user_movie_store");
        return "redirect:/movies/" + movieCode;
    }

    @PostMapping("/movies/{movieCode}/collection")
    @Transactional
    public String toggleCollection(@PathVariable String movieCode, HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/login";
        }
        toggleCollectionMembership(getCurrentUserId(session), findMovieId(movieCode));
        return "redirect:/movies/" + movieCode;
    }

    @PostMapping("/movies/{movieCode}/watched")
    @Transactional
    public String toggleWatched(@PathVariable String movieCode, HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/login";
        }
        cycleWatchState(getCurrentUserId(session), findMovieId(movieCode));
        return "redirect:/movies/" + movieCode;
    }

    @PostMapping("/movies/{movieCode}/watched/rating")
    @Transactional
    public String saveWatchedRating(@PathVariable String movieCode,
                                    @RequestParam int rating,
                                    HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/login";
        }
        saveWatchRating(getCurrentUserId(session), findMovieId(movieCode), rating);
        return "redirect:/movies/" + movieCode;
    }

    @PostMapping("/api/movies/{movieCode}/like")
    @ResponseBody
    @Transactional
    public Map<String, Object> toggleLikeApi(@PathVariable String movieCode, HttpSession session) {
        Long userId = requireCurrentUserId(session);
        Long movieId = findMovieId(movieCode);
        applyLikeState(userId, movieId, true);
        return buildMovieActionPayload(userId, movieId);
    }

    @PostMapping("/api/movies/{movieCode}/dislike")
    @ResponseBody
    @Transactional
    public Map<String, Object> toggleDislikeApi(@PathVariable String movieCode, HttpSession session) {
        Long userId = requireCurrentUserId(session);
        Long movieId = findMovieId(movieCode);
        applyLikeState(userId, movieId, false);
        return buildMovieActionPayload(userId, movieId);
    }

    @PostMapping("/api/movies/{movieCode}/later")
    @ResponseBody
    @Transactional
    public Map<String, Object> toggleLaterApi(@PathVariable String movieCode, HttpSession session) {
        Long userId = requireCurrentUserId(session);
        Long movieId = findMovieId(movieCode);
        toggleSimpleCollection(userId, movieId, "user_movie_store");
        return buildMovieActionPayload(userId, movieId);
    }

    @PostMapping("/api/movies/{movieCode}/collection")
    @ResponseBody
    @Transactional
    public Map<String, Object> toggleCollectionApi(@PathVariable String movieCode, HttpSession session) {
        Long userId = requireCurrentUserId(session);
        Long movieId = findMovieId(movieCode);
        toggleCollectionMembership(userId, movieId);
        return buildMovieActionPayload(userId, movieId);
    }

    @PostMapping("/api/movies/{movieCode}/watch-state")
    @ResponseBody
    @Transactional
    public Map<String, Object> cycleWatchStateApi(@PathVariable String movieCode, HttpSession session) {
        Long userId = requireCurrentUserId(session);
        Long movieId = findMovieId(movieCode);
        cycleWatchState(userId, movieId);
        return buildMovieActionPayload(userId, movieId);
    }

    @PostMapping("/api/movies/{movieCode}/watched/rating")
    @ResponseBody
    @Transactional
    public Map<String, Object> saveWatchedRatingApi(@PathVariable String movieCode,
                                                    @RequestParam int rating,
                                                    HttpSession session) {
        Long userId = requireCurrentUserId(session);
        Long movieId = findMovieId(movieCode);
        saveWatchRating(userId, movieId, rating);
        return buildMovieActionPayload(userId, movieId);
    }

    @PostMapping("/api/people/follow")
    @ResponseBody
    @Transactional
    public Map<String, Object> followUserApi(@RequestParam String loginId, HttpSession session) {
        Long currentUserId = requireCurrentUserId(session);
        initializeSocialTables();
        Long targetUserId = findUserIdByLoginId(loginId);
        if (targetUserId == null) {
            throw new ResponseStatusException(NOT_FOUND);
        }
        if (currentUserId.equals(targetUserId)) {
            throw new ResponseStatusException(BAD_REQUEST, "자기 자신은 팔로우할 수 없습니다.");
        }

        Integer count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM user_follow
                WHERE follower_user_id = ? AND following_user_id = ?
                """, Integer.class, currentUserId, targetUserId);
        if (count == null || count == 0) {
            jdbcTemplate.update("""
                    INSERT INTO user_follow (follower_user_id, following_user_id)
                    VALUES (?, ?)
                    """, currentUserId, targetUserId);
            notificationService.createFollowNotification(currentUserId, targetUserId);
        }
        return buildSocialActionPayload(currentUserId, targetUserId, loginId);
    }

    @PostMapping("/api/people/unfollow")
    @ResponseBody
    @Transactional
    public Map<String, Object> unfollowUserApi(@RequestParam String loginId, HttpSession session) {
        Long currentUserId = requireCurrentUserId(session);
        initializeSocialTables();
        Long targetUserId = findUserIdByLoginId(loginId);
        if (targetUserId == null) {
            throw new ResponseStatusException(NOT_FOUND);
        }
        jdbcTemplate.update("""
                DELETE FROM user_follow
                WHERE follower_user_id = ? AND following_user_id = ?
                """, currentUserId, targetUserId);
        return buildSocialActionPayload(currentUserId, targetUserId, loginId);
    }

    @PostMapping("/mypage/life")
    @Transactional
    public String addLifeMovie(@RequestParam String movieCode, HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/login";
        }

        addMovieToLifeCollection(getCurrentUserId(session), findMovieId(movieCode));
        return "redirect:/mypage";
    }

    @PostMapping("/mypage/life/remove")
    @Transactional
    public String removeLifeMovie(@RequestParam String movieCode, HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/login";
        }

        jdbcTemplate.update("""
                DELETE FROM user_movie_life
                WHERE user_id = ? AND movie_id = ?
                """, getCurrentUserId(session), findMovieId(movieCode));
        recommendationRefreshStateService.markDirty(getCurrentUserId(session));
        return "redirect:/mypage";
    }

    @PostMapping("/people/follow")
    @Transactional
    public String followUser(@RequestParam String loginId,
                             @RequestParam(required = false) String redirectTo,
                             HttpSession session) {
        Long currentUserId = requireCurrentUserId(session);
        initializeSocialTables();
        Long targetUserId = findUserIdByLoginId(loginId);
        if (targetUserId == null) {
            throw new ResponseStatusException(NOT_FOUND);
        }
        if (!currentUserId.equals(targetUserId)) {
            Integer count = jdbcTemplate.queryForObject("""
                    SELECT COUNT(*)
                    FROM user_follow
                    WHERE follower_user_id = ? AND following_user_id = ?
                    """, Integer.class, currentUserId, targetUserId);
            if (count == null || count == 0) {
                jdbcTemplate.update("""
                        INSERT INTO user_follow (follower_user_id, following_user_id)
                        VALUES (?, ?)
                        """, currentUserId, targetUserId);
                notificationService.createFollowNotification(currentUserId, targetUserId);
            }
        }
        return "redirect:" + sanitizeRedirectPath(redirectTo, "/people?user=" + encodeQueryParam(loginId));
    }

    @PostMapping("/people/unfollow")
    @Transactional
    public String unfollowUser(@RequestParam String loginId,
                               @RequestParam(required = false) String redirectTo,
                               HttpSession session) {
        Long currentUserId = requireCurrentUserId(session);
        initializeSocialTables();
        Long targetUserId = findUserIdByLoginId(loginId);
        if (targetUserId == null) {
            throw new ResponseStatusException(NOT_FOUND);
        }
        jdbcTemplate.update("""
                DELETE FROM user_follow
                WHERE follower_user_id = ? AND following_user_id = ?
                """, currentUserId, targetUserId);
        return "redirect:" + sanitizeRedirectPath(redirectTo, "/people?user=" + encodeQueryParam(loginId));
    }

    @PostMapping("/mypage/profile-image")
    @Transactional
    public String updateProfileImage(@RequestParam(required = false) String profileImageUrl,
                                     HttpSession session) {
        Long userId = requireCurrentUserId(session);
        initializeSocialTables();
        String normalizedUrl = profileImageUrl == null ? null : profileImageUrl.trim();
        if (normalizedUrl != null && normalizedUrl.isBlank()) {
            normalizedUrl = null;
        }
        if (normalizedUrl != null && normalizedUrl.length() > 500) {
            throw new ResponseStatusException(BAD_REQUEST, "프로필 이미지 주소가 너무 깁니다.");
        }
        jdbcTemplate.update("""
                UPDATE "USER"
                SET profile_image_url = ?
                WHERE id = ?
                """, normalizedUrl, userId);
        return "redirect:/mypage";
    }

    private boolean isLoggedIn(HttpSession session) {
        return session.getAttribute(LOGIN_SESSION_KEY) != null;
    }

    private Long getCurrentUserId(HttpSession session) {
        String loginId = (String) session.getAttribute(LOGIN_SESSION_KEY);
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

    private Long findMovieId(String movieCode) {
        Long movieId = jdbcTemplate.query("""
                SELECT id
                FROM movie
                WHERE movie_cd = ?
                """, rs -> rs.next() ? rs.getLong("id") : null, movieCode);
        if (movieId == null) {
            throw new ResponseStatusException(NOT_FOUND);
        }
        return movieId;
    }

    private void addCurrentUserAttributes(Model model, HttpSession session) {
        initializeSocialTables();
        model.addAttribute("loginUserId", session.getAttribute(LOGIN_SESSION_KEY));
        model.addAttribute("loginUserNickname", session.getAttribute(LOGIN_NICKNAME_SESSION_KEY));
        model.addAttribute("loginUserProfileImageUrl", fetchCurrentUserProfileImageUrl(session));
        if (isLoggedIn(session)) {
            Long currentUserId = getCurrentUserId(session);
            model.addAttribute("socialSuggestedUsers", fetchSuggestedUsers(currentUserId, currentUserId, SOCIAL_PAGE_LIMIT));
        }
    }

    private Long requireCurrentUserId(HttpSession session) {
        if (!isLoggedIn(session)) {
            throw new ResponseStatusException(UNAUTHORIZED);
        }
        return getCurrentUserId(session);
    }

    private void initializeActivityTables() {
        initializeWatchedTable();
        initializeCollectionTable();
        initializeSocialTables();
    }

    private void initializeWatchedTable() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS user_movie_watched (
                    user_id BIGINT NOT NULL,
                    movie_id BIGINT NOT NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'WATCHED',
                    rating INT,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, movie_id),
                    CONSTRAINT fk_user_movie_watched_user FOREIGN KEY (user_id) REFERENCES "USER"(id),
                    CONSTRAINT fk_user_movie_watched_movie FOREIGN KEY (movie_id) REFERENCES movie(id)
                )
                """);
        jdbcTemplate.execute("""
                ALTER TABLE user_movie_watched
                ADD COLUMN IF NOT EXISTS rating INT
                """);
        jdbcTemplate.execute("""
                ALTER TABLE user_movie_watched
                ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'WATCHED'
                """);
        jdbcTemplate.update("""
                UPDATE user_movie_watched
                SET status = 'WATCHED'
                WHERE status IS NULL
                """);
    }

    private void initializeCollectionTable() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS user_movie_collection (
                    user_id BIGINT NOT NULL,
                    movie_id BIGINT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, movie_id),
                    CONSTRAINT fk_user_movie_collection_user FOREIGN KEY (user_id) REFERENCES "USER"(id),
                    CONSTRAINT fk_user_movie_collection_movie FOREIGN KEY (movie_id) REFERENCES movie(id)
                )
                """);
    }

    private void initializeSocialTables() {
        jdbcTemplate.execute("""
                ALTER TABLE "USER"
                ADD COLUMN IF NOT EXISTS profile_image_url VARCHAR(500)
                """);
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS user_follow (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    follower_user_id BIGINT NOT NULL,
                    following_user_id BIGINT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT fk_user_follow_follower FOREIGN KEY (follower_user_id) REFERENCES "USER"(id),
                    CONSTRAINT fk_user_follow_following FOREIGN KEY (following_user_id) REFERENCES "USER"(id),
                    CONSTRAINT uk_user_follow_pair UNIQUE (follower_user_id, following_user_id),
                    CONSTRAINT ck_user_follow_self CHECK (follower_user_id <> following_user_id)
                )
                """);
        jdbcTemplate.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_follow_follower
                ON user_follow (follower_user_id, created_at)
                """);
        jdbcTemplate.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_follow_following
                ON user_follow (following_user_id, created_at)
                """);
    }

    private String fetchCurrentUserProfileImageUrl(HttpSession session) {
        Object loginId = session.getAttribute(LOGIN_SESSION_KEY);
        if (!(loginId instanceof String currentLoginId) || currentLoginId.isBlank()) {
            return null;
        }
        return jdbcTemplate.query("""
                SELECT profile_image_url
                FROM "USER"
                WHERE login_id = ?
                """, rs -> rs.next() ? rs.getString("profile_image_url") : null, currentLoginId);
    }

    private Long findUserIdByLoginId(String loginId) {
        return jdbcTemplate.query("""
                SELECT id
                FROM "USER"
                WHERE login_id = ?
                """, rs -> rs.next() ? rs.getLong("id") : null, loginId);
    }

    private long countFollowers(Long userId) {
        Long count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM user_follow
                WHERE following_user_id = ?
                """, Long.class, userId);
        return count == null ? 0L : count;
    }

    private long countFollowing(Long userId) {
        Long count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM user_follow
                WHERE follower_user_id = ?
                """, Long.class, userId);
        return count == null ? 0L : count;
    }

    private String normalizePeopleView(String view) {
        if ("following".equalsIgnoreCase(view)) {
            return "following";
        }
        return "followers";
    }

    private SocialProfileView fetchSocialProfile(String loginId, Long currentUserId) {
        return jdbcTemplate.query("""
                SELECT
                    u.id,
                    u.login_id,
                    u.nickname,
                    u.profile_image_url,
                    (SELECT COUNT(*) FROM user_follow uf WHERE uf.following_user_id = u.id) AS follower_count,
                    (SELECT COUNT(*) FROM user_follow uf WHERE uf.follower_user_id = u.id) AS following_count,
                    EXISTS(
                        SELECT 1
                        FROM user_follow uf
                        WHERE uf.follower_user_id = ?
                          AND uf.following_user_id = u.id
                    ) AS followed_by_current_user
                FROM "USER" u
                WHERE u.login_id = ?
                """, rs -> rs.next()
                ? new SocialProfileView(
                        rs.getLong("id"),
                        rs.getString("login_id"),
                        rs.getString("nickname"),
                        rs.getString("profile_image_url"),
                        rs.getLong("follower_count"),
                        rs.getLong("following_count"),
                        rs.getBoolean("followed_by_current_user"),
                        rs.getLong("id") == currentUserId
                )
                : null, currentUserId, loginId);
    }

    private List<SocialUserCardView> searchUsers(Long currentUserId, String query, int limit) {
        String pattern = "%" + query + "%";
        return jdbcTemplate.query("""
                SELECT
                    u.id,
                    u.login_id,
                    u.nickname,
                    u.profile_image_url,
                    (SELECT COUNT(*) FROM user_follow uf WHERE uf.following_user_id = u.id) AS follower_count,
                    (SELECT COUNT(*) FROM user_follow uf WHERE uf.follower_user_id = u.id) AS following_count,
                    EXISTS(
                        SELECT 1
                        FROM user_follow uf
                        WHERE uf.follower_user_id = ?
                          AND uf.following_user_id = u.id
                    ) AS followed_by_current_user
                FROM "USER" u
                WHERE u.id <> ?
                  AND (
                        UPPER(u.login_id) LIKE UPPER(?)
                        OR UPPER(u.nickname) LIKE UPPER(?)
                  )
                ORDER BY
                    CASE
                        WHEN UPPER(u.login_id) = UPPER(?) THEN 0
                        WHEN UPPER(u.login_id) LIKE UPPER(?) THEN 1
                        ELSE 2
                    END,
                    u.login_id ASC
                LIMIT ?
                """, this::mapSocialUserCard,
                currentUserId,
                currentUserId,
                pattern,
                pattern,
                query,
                query + "%",
                limit);
    }

    private List<SocialUserCardView> fetchSuggestedUsers(Long currentUserId, Long targetUserId, int limit) {
        return jdbcTemplate.query("""
                SELECT
                    u.id,
                    u.login_id,
                    u.nickname,
                    u.profile_image_url,
                    (SELECT COUNT(*) FROM user_follow uf WHERE uf.following_user_id = u.id) AS follower_count,
                    (SELECT COUNT(*) FROM user_follow uf WHERE uf.follower_user_id = u.id) AS following_count,
                    EXISTS(
                        SELECT 1
                        FROM user_follow uf
                        WHERE uf.follower_user_id = ?
                          AND uf.following_user_id = u.id
                    ) AS followed_by_current_user
                FROM "USER" u
                WHERE u.id <> ?
                ORDER BY
                    CASE WHEN u.id = ? THEN 0 ELSE 1 END,
                    u.id DESC
                LIMIT ?
                """, this::mapSocialUserCard,
                currentUserId,
                currentUserId,
                targetUserId,
                limit);
    }

    private List<SocialUserCardView> fetchFollowerUsers(Long targetUserId, Long currentUserId, int limit) {
        return jdbcTemplate.query("""
                SELECT
                    u.id,
                    u.login_id,
                    u.nickname,
                    u.profile_image_url,
                    (SELECT COUNT(*) FROM user_follow inner_uf WHERE inner_uf.following_user_id = u.id) AS follower_count,
                    (SELECT COUNT(*) FROM user_follow inner_uf WHERE inner_uf.follower_user_id = u.id) AS following_count,
                    EXISTS(
                        SELECT 1
                        FROM user_follow current_rel
                        WHERE current_rel.follower_user_id = ?
                          AND current_rel.following_user_id = u.id
                    ) AS followed_by_current_user
                FROM user_follow uf
                JOIN "USER" u ON u.id = uf.follower_user_id
                WHERE uf.following_user_id = ?
                ORDER BY uf.created_at DESC, u.login_id ASC
                LIMIT ?
                """, this::mapSocialUserCard, currentUserId, targetUserId, limit);
    }

    private List<SocialUserCardView> fetchFollowingUsers(Long targetUserId, Long currentUserId, int limit) {
        return jdbcTemplate.query("""
                SELECT
                    u.id,
                    u.login_id,
                    u.nickname,
                    u.profile_image_url,
                    (SELECT COUNT(*) FROM user_follow inner_uf WHERE inner_uf.following_user_id = u.id) AS follower_count,
                    (SELECT COUNT(*) FROM user_follow inner_uf WHERE inner_uf.follower_user_id = u.id) AS following_count,
                    EXISTS(
                        SELECT 1
                        FROM user_follow current_rel
                        WHERE current_rel.follower_user_id = ?
                          AND current_rel.following_user_id = u.id
                    ) AS followed_by_current_user
                FROM user_follow uf
                JOIN "USER" u ON u.id = uf.following_user_id
                WHERE uf.follower_user_id = ?
                ORDER BY uf.created_at DESC, u.login_id ASC
                LIMIT ?
                """, this::mapSocialUserCard, currentUserId, targetUserId, limit);
    }

    private SocialUserCardView mapSocialUserCard(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
        long userId = rs.getLong("id");
        return new SocialUserCardView(
                userId,
                rs.getString("login_id"),
                rs.getString("nickname"),
                rs.getString("profile_image_url"),
                rs.getLong("follower_count"),
                rs.getLong("following_count"),
                rs.getBoolean("followed_by_current_user"),
                false
        );
    }

    private String sanitizeRedirectPath(String redirectTo, String fallback) {
        if (redirectTo == null || redirectTo.isBlank()) {
            return fallback;
        }
        String normalized = redirectTo.trim();
        if (!normalized.startsWith("/") || normalized.startsWith("//")) {
            return fallback;
        }
        return normalized;
    }

    private String encodeQueryParam(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private Map<String, Object> buildSocialActionPayload(Long currentUserId, Long targetUserId, String targetLoginId) {
        Integer followingCount = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM user_follow
                WHERE follower_user_id = ? AND following_user_id = ?
                """, Integer.class, currentUserId, targetUserId);
        boolean following = followingCount != null && followingCount > 0;
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("loginId", targetLoginId);
        payload.put("followingByCurrentUser", following);
        payload.put("currentUserFollowerCount", countFollowers(currentUserId));
        payload.put("currentUserFollowingCount", countFollowing(currentUserId));
        payload.put("targetFollowerCount", countFollowers(targetUserId));
        payload.put("targetFollowingCount", countFollowing(targetUserId));
        return payload;
    }

    private void applyLikeState(Long userId, Long movieId, boolean liked) {
        Boolean existing = jdbcTemplate.query("""
                SELECT liked
                FROM user_movie_like
                WHERE user_id = ? AND movie_id = ?
                """, rs -> rs.next() ? rs.getBoolean("liked") : null, userId, movieId);

        if (existing != null && existing == liked) {
            jdbcTemplate.update("""
                    DELETE FROM user_movie_like
                    WHERE user_id = ? AND movie_id = ?
                    """, userId, movieId);
        } else if (existing != null) {
            jdbcTemplate.update("""
                    UPDATE user_movie_like
                    SET liked = ?, created_at = CURRENT_TIMESTAMP
                    WHERE user_id = ? AND movie_id = ?
                    """, liked, userId, movieId);
        } else {
            jdbcTemplate.update("""
                    INSERT INTO user_movie_like (user_id, movie_id, liked)
                    VALUES (?, ?, ?)
                    """, userId, movieId, liked);
        }
        recommendationRefreshStateService.markDirty(userId);
    }

    private void toggleSimpleCollection(Long userId, Long movieId, String tableName) {
        Integer count = jdbcTemplate.query("""
                SELECT COUNT(*)
                FROM %s
                WHERE user_id = ? AND movie_id = ?
                """.formatted(tableName), rs -> rs.next() ? rs.getInt(1) : 0, userId, movieId);

        if (count != null && count > 0) {
            jdbcTemplate.update("""
                    DELETE FROM %s
                    WHERE user_id = ? AND movie_id = ?
                    """.formatted(tableName), userId, movieId);
        } else {
            jdbcTemplate.update("""
                    INSERT INTO %s (user_id, movie_id)
                    VALUES (?, ?)
                    """.formatted(tableName), userId, movieId);
        }
        recommendationRefreshStateService.markDirty(userId);
    }

    private void toggleCollectionMembership(Long userId, Long movieId) {
        initializeCollectionTable();
        Integer count = jdbcTemplate.query("""
                SELECT COUNT(*)
                FROM user_movie_collection
                WHERE user_id = ? AND movie_id = ?
                """, rs -> rs.next() ? rs.getInt(1) : 0, userId, movieId);

        if (count != null && count > 0) {
            jdbcTemplate.update("""
                    DELETE FROM user_movie_collection
                    WHERE user_id = ? AND movie_id = ?
                    """, userId, movieId);
            jdbcTemplate.update("""
                    DELETE FROM user_movie_life
                    WHERE user_id = ? AND movie_id = ?
                    """, userId, movieId);
        } else {
            jdbcTemplate.update("""
                    INSERT INTO user_movie_collection (user_id, movie_id)
                    VALUES (?, ?)
                    """, userId, movieId);
        }
        recommendationRefreshStateService.markDirty(userId);
    }

    private String cycleWatchState(Long userId, Long movieId) {
        initializeWatchedTable();
        String currentStatus = jdbcTemplate.query("""
                SELECT status
                FROM user_movie_watched
                WHERE user_id = ? AND movie_id = ?
                """, rs -> rs.next() ? normalizeWatchStatus(rs.getString("status")) : null, userId, movieId);

        if (currentStatus == null) {
            jdbcTemplate.update("""
                    INSERT INTO user_movie_watched (user_id, movie_id, status, rating)
                    VALUES (?, ?, 'WATCHING', NULL)
                    """, userId, movieId);
            recommendationRefreshStateService.markDirty(userId);
            return "WATCHING";
        }

        if ("WATCHING".equals(currentStatus)) {
            jdbcTemplate.update("""
                    UPDATE user_movie_watched
                    SET status = 'WATCHED', rating = NULL, created_at = CURRENT_TIMESTAMP
                    WHERE user_id = ? AND movie_id = ?
                    """, userId, movieId);
            recommendationRefreshStateService.markDirty(userId);
            return "WATCHED";
        }

        jdbcTemplate.update("""
                DELETE FROM user_movie_watched
                WHERE user_id = ? AND movie_id = ?
                """, userId, movieId);
        recommendationRefreshStateService.markDirty(userId);
        return null;
    }

    private void saveWatchRating(Long userId, Long movieId, int rating) {
        if (rating < 1 || rating > 5) {
            throw new ResponseStatusException(BAD_REQUEST);
        }

        int updated = jdbcTemplate.update("""
                UPDATE user_movie_watched
                SET rating = ?
                WHERE user_id = ?
                  AND movie_id = ?
                  AND COALESCE(status, 'WATCHED') = 'WATCHED'
                """, rating, userId, movieId);
        if (updated == 0) {
            throw new ResponseStatusException(BAD_REQUEST, "별점은 봤어요 상태에서만 저장할 수 있습니다.");
        }
        recommendationRefreshStateService.markDirty(userId);
    }

    private void addMovieToLifeCollection(Long userId, Long movieId) {
        initializeCollectionTable();
        Integer inCollection = jdbcTemplate.query("""
                SELECT COUNT(*)
                FROM user_movie_collection
                WHERE user_id = ? AND movie_id = ?
                """, rs -> rs.next() ? rs.getInt(1) : 0, userId, movieId);
        if (inCollection == null || inCollection == 0) {
            throw new ResponseStatusException(BAD_REQUEST, "컬렉션에 담긴 영화만 인생영화로 추가할 수 있습니다.");
        }

        if (countLifeMovies(userId) >= LIFE_MOVIE_LIMIT) {
            throw new ResponseStatusException(BAD_REQUEST, "인생영화는 최대 10편까지 추가할 수 있습니다.");
        }

        Integer existingCount = jdbcTemplate.query("""
                SELECT COUNT(*)
                FROM user_movie_life
                WHERE user_id = ? AND movie_id = ?
                """, rs -> rs.next() ? rs.getInt(1) : 0, userId, movieId);
        if (existingCount == null || existingCount == 0) {
            jdbcTemplate.update("""
                    INSERT INTO user_movie_life (user_id, movie_id)
                    VALUES (?, ?)
                    """, userId, movieId);
            recommendationRefreshStateService.markDirty(userId);
            notificationService.createLifeMovieNotifications(userId, movieId);
        }
    }

    private MovieActionState fetchMovieActionState(Long userId, Long movieId) {
        initializeActivityTables();

        Boolean likedValue = jdbcTemplate.query("""
                SELECT liked
                FROM user_movie_like
                WHERE user_id = ? AND movie_id = ?
                """, rs -> rs.next() ? rs.getBoolean("liked") : null, userId, movieId);
        Boolean stored = jdbcTemplate.query("""
                SELECT COUNT(*) > 0
                FROM user_movie_store
                WHERE user_id = ? AND movie_id = ?
                """, rs -> rs.next() && rs.getBoolean(1), userId, movieId);
        Boolean collected = jdbcTemplate.query("""
                SELECT COUNT(*) > 0
                FROM user_movie_collection
                WHERE user_id = ? AND movie_id = ?
                """, rs -> rs.next() && rs.getBoolean(1), userId, movieId);
        WatchState watchState = jdbcTemplate.query("""
                SELECT status, rating
                FROM user_movie_watched
                WHERE user_id = ? AND movie_id = ?
                """, rs -> rs.next()
                ? new WatchState(normalizeWatchStatus(rs.getString("status")), rs.getObject("rating", Integer.class))
                : new WatchState(null, null), userId, movieId);

        return new MovieActionState(
                Boolean.TRUE.equals(likedValue),
                Boolean.FALSE.equals(likedValue),
                Boolean.TRUE.equals(stored),
                Boolean.TRUE.equals(collected),
                watchState.status(),
                watchState.rating()
        );
    }

    private Map<String, Object> buildMovieActionPayload(Long userId, Long movieId) {
        MovieActionState state = fetchMovieActionState(userId, movieId);
        Integer likeCount = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM user_movie_like
                WHERE movie_id = ?
                  AND liked = TRUE
                """, Integer.class, movieId);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("liked", state.liked());
        payload.put("disliked", state.disliked());
        payload.put("later", state.stored());
        payload.put("collected", state.collected());
        payload.put("watchStatus", state.watchStatus());
        payload.put("watching", "WATCHING".equals(state.watchStatus()));
        payload.put("watched", "WATCHED".equals(state.watchStatus()));
        payload.put("canRate", "WATCHED".equals(state.watchStatus()));
        payload.put("rating", state.watchedRating());
        payload.put("likeCount", likeCount == null ? 0 : likeCount);
        return payload;
    }

    private String normalizeWatchStatus(String status) {
        if ("WATCHING".equalsIgnoreCase(status)) {
            return "WATCHING";
        }
        if ("WATCHED".equalsIgnoreCase(status)) {
            return "WATCHED";
        }
        return null;
    }

    private List<MoviePosterView> fetchPopularMovies(int limit) {
        return jdbcTemplate.query("""
                SELECT
                    m.ranking,
                    m.movie_cd,
                    COALESCE(m.title, m.movie_name) AS movie_name,
                    COALESCE(m.movie_name_en, m.original_title, m.movie_name_original) AS movie_name_en,
                    m.poster_image_url
                FROM movie m
                ORDER BY
                    CASE WHEN m.ranking IS NULL THEN 1 ELSE 0 END,
                    m.ranking ASC,
                    COALESCE(m.title, m.movie_name) ASC
                LIMIT ?
                """, (rs, rowNum) -> new MoviePosterView(
                rs.getInt("ranking"),
                rs.getString("movie_cd"),
                rs.getString("movie_name"),
                rs.getString("movie_name_en"),
                rs.getString("poster_image_url")
        ), limit);
    }

    private List<ActivityMovieCardView> fetchLikedMovies(Long userId, Integer limit) {
        return fetchActivityMovies("""
                SELECT
                    m.ranking,
                    m.movie_cd,
                    COALESCE(m.title, m.movie_name) AS movie_name,
                    COALESCE(m.movie_name_en, m.original_title, m.movie_name_original) AS movie_name_en,
                    m.poster_image_url,
                    NULL AS watch_status,
                    NULL AS rating
                FROM user_movie_like uml
                JOIN movie m ON m.id = uml.movie_id
                WHERE uml.user_id = ?
                  AND uml.liked = TRUE
                ORDER BY uml.created_at DESC, m.ranking ASC
                """, limit, userId);
    }

    private List<ActivityMovieCardView> fetchDislikedMovies(Long userId, Integer limit) {
        return fetchActivityMovies("""
                SELECT
                    m.ranking,
                    m.movie_cd,
                    COALESCE(m.title, m.movie_name) AS movie_name,
                    COALESCE(m.movie_name_en, m.original_title, m.movie_name_original) AS movie_name_en,
                    m.poster_image_url,
                    NULL AS watch_status,
                    NULL AS rating
                FROM user_movie_like uml
                JOIN movie m ON m.id = uml.movie_id
                WHERE uml.user_id = ?
                  AND uml.liked = FALSE
                ORDER BY uml.created_at DESC, m.ranking ASC
                """, limit, userId);
    }

    private List<ActivityMovieCardView> fetchCollectionMovies(Long userId, Integer limit) {
        initializeCollectionTable();
        return fetchActivityMovies("""
                SELECT
                    m.ranking,
                    m.movie_cd,
                    COALESCE(m.title, m.movie_name) AS movie_name,
                    COALESCE(m.movie_name_en, m.original_title, m.movie_name_original) AS movie_name_en,
                    m.poster_image_url,
                    NULL AS watch_status,
                    NULL AS rating
                FROM user_movie_collection umc
                JOIN movie m ON m.id = umc.movie_id
                WHERE umc.user_id = ?
                ORDER BY umc.created_at DESC, m.ranking ASC
                """, limit, userId);
    }

    private List<ActivityMovieCardView> fetchStoredMovies(Long userId, Integer limit) {
        return fetchActivityMovies("""
                SELECT
                    m.ranking,
                    m.movie_cd,
                    COALESCE(m.title, m.movie_name) AS movie_name,
                    COALESCE(m.movie_name_en, m.original_title, m.movie_name_original) AS movie_name_en,
                    m.poster_image_url,
                    NULL AS watch_status,
                    NULL AS rating
                FROM user_movie_store ums
                JOIN movie m ON m.id = ums.movie_id
                WHERE ums.user_id = ?
                ORDER BY ums.created_at DESC, m.ranking ASC
                """, limit, userId);
    }

    private List<ActivityMovieCardView> fetchWatchedMovies(Long userId, String status, Integer limit) {
        initializeWatchedTable();
        return fetchActivityMovies("""
                SELECT
                    m.ranking,
                    m.movie_cd,
                    COALESCE(m.title, m.movie_name) AS movie_name,
                    COALESCE(m.movie_name_en, m.original_title, m.movie_name_original) AS movie_name_en,
                    m.poster_image_url,
                    COALESCE(umw.status, 'WATCHED') AS watch_status,
                    umw.rating
                FROM user_movie_watched umw
                JOIN movie m ON m.id = umw.movie_id
                WHERE umw.user_id = ?
                  AND COALESCE(umw.status, 'WATCHED') = ?
                ORDER BY umw.created_at DESC, m.ranking ASC
                """, limit, userId, status);
    }

    private int countLikedMovies(Long userId) {
        Integer count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM user_movie_like
                WHERE user_id = ?
                  AND liked = TRUE
                """, Integer.class, userId);
        return count == null ? 0 : count;
    }

    private int countDislikedMovies(Long userId) {
        Integer count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM user_movie_like
                WHERE user_id = ?
                  AND liked = FALSE
                """, Integer.class, userId);
        return count == null ? 0 : count;
    }

    private int countCollectionMovies(Long userId) {
        initializeCollectionTable();
        Integer count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM user_movie_collection
                WHERE user_id = ?
                """, Integer.class, userId);
        return count == null ? 0 : count;
    }

    private int countStoredMovies(Long userId) {
        Integer count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM user_movie_store
                WHERE user_id = ?
                """, Integer.class, userId);
        return count == null ? 0 : count;
    }

    private int countWatchedMovies(Long userId, String status) {
        initializeWatchedTable();
        Integer count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM user_movie_watched
                WHERE user_id = ?
                  AND COALESCE(status, 'WATCHED') = ?
                """, Integer.class, userId, status);
        return count == null ? 0 : count;
    }

    private int countLifeMovies(Long userId) {
        Integer count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM user_movie_life
                WHERE user_id = ?
                """, Integer.class, userId);
        return count == null ? 0 : count;
    }

    private List<ActivityMovieCardView> fetchLifeMovies(Long userId, Integer limit) {
        return fetchActivityMovies("""
                SELECT
                    m.ranking,
                    m.movie_cd,
                    COALESCE(m.title, m.movie_name) AS movie_name,
                    COALESCE(m.movie_name_en, m.original_title, m.movie_name_original) AS movie_name_en,
                    m.poster_image_url,
                    NULL AS watch_status,
                    NULL AS rating
                FROM user_movie_life uml
                JOIN movie m ON m.id = uml.movie_id
                WHERE uml.user_id = ?
                ORDER BY uml.created_at DESC, m.ranking ASC
                """, limit, userId);
    }

    private List<LifeMovieSearchResultView> fetchLifeMovieSearchResults(Long userId) {
        initializeCollectionTable();
        return jdbcTemplate.query("""
                SELECT
                    m.movie_cd,
                    COALESCE(m.title, m.movie_name) AS movie_name,
                    COALESCE(m.movie_name_en, m.original_title, m.movie_name_original) AS movie_name_en,
                    m.poster_image_url,
                    EXISTS(
                        SELECT 1
                        FROM user_movie_life uml
                        WHERE uml.user_id = ? AND uml.movie_id = m.id
                    ) AS already_added
                FROM user_movie_collection umc
                JOIN movie m ON m.id = umc.movie_id
                WHERE umc.user_id = ?
                ORDER BY umc.created_at DESC, m.ranking ASC
                LIMIT 24
                """, (rs, rowNum) -> new LifeMovieSearchResultView(
                rs.getString("movie_cd"),
                rs.getString("movie_name"),
                rs.getString("movie_name_en"),
                rs.getString("poster_image_url"),
                rs.getBoolean("already_added")
        ), userId, userId);
    }

    private List<ActivityMovieCardView> fetchActivityMovies(String sql, Integer limit, Object... params) {
        java.util.ArrayList<Object> arguments = new java.util.ArrayList<>(Arrays.asList(params));
        String finalSql = sql;
        if (limit != null) {
            finalSql += " LIMIT ?";
            arguments.add(limit);
        }
        return jdbcTemplate.query(finalSql, (rs, rowNum) -> new ActivityMovieCardView(
                rs.getInt("ranking"),
                rs.getString("movie_cd"),
                rs.getString("movie_name"),
                rs.getString("movie_name_en"),
                rs.getString("poster_image_url"),
                normalizeWatchStatus(rs.getString("watch_status")),
                rs.getObject("rating", Integer.class)
        ), arguments.toArray());
    }

    private List<String> fetchOrderedNames(String sql, String movieCode) {
        return jdbcTemplate.query(sql, (rs, rowNum) -> rs.getString(1), movieCode);
    }

    private List<String> fetchChartGenres() {
        return jdbcTemplate.query("""
                SELECT name
                FROM genre
                WHERE name NOT IN (%s)
                ORDER BY name ASC
                """.formatted(placeholders(SEARCH_EXCLUDED_GENRES.size())),
                (rs, rowNum) -> rs.getString("name"),
                SEARCH_EXCLUDED_GENRES.toArray());
    }

    private boolean isSearchExcludedGenre(String genreName) {
        return genreName != null && SEARCH_EXCLUDED_GENRES.contains(genreName.trim());
    }

    private String searchExcludedGenreClause() {
        return """
                  AND NOT EXISTS (
                        SELECT 1
                        FROM movie_genre mg_excluded
                        JOIN genre g_excluded ON g_excluded.id = mg_excluded.genre_id
                        WHERE mg_excluded.movie_id = m.id
                          AND g_excluded.name IN (%s)
                  )
                """.formatted(placeholders(SEARCH_EXCLUDED_GENRES.size()));
    }

    private String placeholders(int count) {
        return java.util.stream.IntStream.range(0, count)
                .mapToObj(index -> "?")
                .collect(java.util.stream.Collectors.joining(", "));
    }

    private List<TagFilterGroupView> fetchChartTagGroups() {
        return Arrays.stream(RecommendationTagType.values())
                .map(type -> new TagFilterGroupView(
                        type.name(),
                        labelForTagType(type.name()),
                        Arrays.stream(RecommendationTag.values())
                                .filter(tag -> tag.type() == type)
                                .map(tag -> new TagChipView(
                                        type.name(),
                                        tag.code(),
                                        labelForTag(tag.code())
                                ))
                                .toList()
                ))
                .toList();
    }

    private String labelForTagType(String tagType) {
        return TAG_TYPE_LABELS.getOrDefault(tagType, tagType);
    }

    private String labelForTag(String tagName) {
        if (tagName == null || tagName.isBlank()) {
            return "";
        }
        return TAG_LABELS.getOrDefault(tagName, tagName);
    }

    public record MoviePosterView(int ranking, String movieCode, String movieName, String movieNameEn, String posterImageUrl) {
    }

    public record MovieDetailView(
            int ranking,
            String movieCode,
            String movieName,
            String movieNameEn,
            String movieNameOriginal,
            String posterImageUrl,
            int likeCount,
            boolean likedByCurrentUser,
            boolean dislikedByCurrentUser,
            boolean storedByCurrentUser,
            boolean collectedByCurrentUser,
            String watchStatus,
            Integer watchedRating,
            LocalDate boxOfficeOpenDate,
            LocalDate movieInfoOpenDate,
            Integer productionYear,
            Integer showTime,
            String typeName,
            String productionStatusName,
            String nationName,
            Long boxOfficeSalesAcc,
            Long boxOfficeAudiAcc,
            Integer boxOfficeScrnCnt,
            Integer boxOfficeShowCnt
    ) {
        public boolean watchingByCurrentUser() {
            return "WATCHING".equals(watchStatus);
        }

        public boolean watchedByCurrentUser() {
            return "WATCHED".equals(watchStatus);
        }
    }

    public record ActivityMovieCardView(
            int ranking,
            String movieCode,
            String movieName,
            String movieNameEn,
            String posterImageUrl,
            String watchStatus,
            Integer watchedRating
    ) {
        public boolean watching() {
            return "WATCHING".equals(watchStatus);
        }

        public boolean watched() {
            return "WATCHED".equals(watchStatus);
        }
    }

    public record CompanyView(String name, String role) {
    }

    public record ProviderView(String name, String type) {
    }

    public record AuditView(String auditNo, String watchGradeName) {
    }

    public record MovieTagView(String tagType, String tagTypeLabel, String tagName, String tagLabel) {
    }

    public record TagChipView(String tagType, String tagName, String displayName) {
    }

    public record TagFilterGroupView(String key, String label, List<TagChipView> tags) {
    }

    public record HomeChartSectionView(ChartEntry entry, List<ChartMovieRow> rows) {
    }

    public record UserProfileView(String loginId, String nickname, String gender, int age, String profileImageUrl) {
    }

    public record SocialProfileView(
            Long userId,
            String loginId,
            String nickname,
            String profileImageUrl,
            long followerCount,
            long followingCount,
            boolean followedByCurrentUser,
            boolean self
    ) {
    }

    public record SocialUserCardView(
            Long userId,
            String loginId,
            String nickname,
            String profileImageUrl,
            long followerCount,
            long followingCount,
            boolean followingByCurrentUser,
            boolean self
    ) {
    }

    public record LifeMovieSearchResultView(
            String movieCode,
            String movieName,
            String movieNameEn,
            String posterImageUrl,
            boolean alreadyAdded
    ) {
    }

    private record MovieActionState(
            boolean liked,
            boolean disliked,
            boolean stored,
            boolean collected,
            String watchStatus,
            Integer watchedRating
    ) {
    }

    private record WatchState(String status, Integer rating) {
    }

    private record LoginUser(String loginId, String nickname) {
    }
}
