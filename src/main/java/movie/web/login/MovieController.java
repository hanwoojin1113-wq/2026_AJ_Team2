package movie.web.login;

import java.time.LocalDate;
import java.util.List;

import jakarta.servlet.http.HttpSession;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.server.ResponseStatusException;

import static org.springframework.http.HttpStatus.NOT_FOUND;

@Controller
public class MovieController {

    private static final String LOGIN_SESSION_KEY = "loginUserId";
    private static final String LOGIN_NICKNAME_SESSION_KEY = "loginUserNickname";
    private static final int PAGE_SIZE = 10;

    private final JdbcTemplate jdbcTemplate;

    public MovieController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
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
                       @RequestParam(required = false, defaultValue = "false") boolean showAdvanced,
                       Model model, HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/login";
        }

        String normalizedQuery = query == null ? "" : query.trim();
        String normalizedGenre = genre == null ? "" : genre.trim();
        boolean hasQuery = !normalizedQuery.isEmpty();
        boolean hasGenre = !normalizedGenre.isEmpty();

        String countSql = """
                SELECT COUNT(DISTINCT m.id)
                FROM movie m
                LEFT JOIN movie_genre mg ON mg.movie_id = m.id
                LEFT JOIN genre g ON g.id = mg.genre_id
                WHERE (? = '' OR UPPER(m.movie_name) LIKE UPPER(?)
                       OR UPPER(COALESCE(m.movie_name_en, '')) LIKE UPPER(?)
                       OR UPPER(m.movie_cd) LIKE UPPER(?))
                  AND (? = '' OR g.name = ?)
                """;
        String queryPattern = "%" + normalizedQuery + "%";
        int totalMovies = jdbcTemplate.queryForObject(countSql, Integer.class,
                normalizedQuery, queryPattern, queryPattern, queryPattern,
                normalizedGenre, normalizedGenre);
        int totalPages = Math.max(1, (int) Math.ceil((double) totalMovies / PAGE_SIZE));
        int currentPage = Math.min(Math.max(page, 1), totalPages);
        int offset = (currentPage - 1) * PAGE_SIZE;

        List<MoviePosterView> movies = jdbcTemplate.query("""
                SELECT DISTINCT m.ranking, m.movie_cd, m.movie_name, m.movie_name_en, m.poster_image_url
                FROM movie m
                LEFT JOIN movie_genre mg ON mg.movie_id = m.id
                LEFT JOIN genre g ON g.id = mg.genre_id
                WHERE (? = '' OR UPPER(m.movie_name) LIKE UPPER(?)
                       OR UPPER(COALESCE(m.movie_name_en, '')) LIKE UPPER(?)
                       OR UPPER(m.movie_cd) LIKE UPPER(?))
                  AND (? = '' OR g.name = ?)
                ORDER BY m.ranking ASC
                LIMIT ? OFFSET ?
                """, (rs, rowNum) -> new MoviePosterView(
                rs.getInt("ranking"),
                rs.getString("movie_cd"),
                rs.getString("movie_name"),
                rs.getString("movie_name_en"),
                rs.getString("poster_image_url")
        ), normalizedQuery, queryPattern, queryPattern, queryPattern,
                normalizedGenre, normalizedGenre, PAGE_SIZE, offset);

        model.addAttribute("movies", movies);
        addCurrentUserAttributes(model, session);
        model.addAttribute("currentPage", currentPage);
        model.addAttribute("totalPages", totalPages);
        model.addAttribute("hasPrevious", currentPage > 1);
        model.addAttribute("hasNext", currentPage < totalPages);
        model.addAttribute("previousPage", currentPage - 1);
        model.addAttribute("nextPage", currentPage + 1);
        model.addAttribute("pages", java.util.stream.IntStream.rangeClosed(1, totalPages).boxed().toList());
        model.addAttribute("query", normalizedQuery);
        model.addAttribute("selectedGenre", normalizedGenre);
        model.addAttribute("showAdvanced", showAdvanced || hasGenre);
        model.addAttribute("genres", fetchChartGenres());
        return "index";
    }

    @GetMapping("/mypage")
    public String myPage(@RequestParam(required = false, defaultValue = "false") boolean showLifeSearch,
                         @RequestParam(required = false) String lifeQuery,
                         Model model, HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/login";
        }

        Long userId = getCurrentUserId(session);
        UserProfileView userProfile = jdbcTemplate.query("""
                SELECT login_id, nickname, gender, age
                FROM "USER"
                WHERE login_id = ?
                """, rs -> rs.next()
                ? new UserProfileView(
                        rs.getString("login_id"),
                        rs.getString("nickname"),
                        rs.getString("gender"),
                        rs.getInt("age"))
                : null, session.getAttribute(LOGIN_SESSION_KEY));

        if (userProfile == null) {
            session.invalidate();
            return "redirect:/login";
        }

        addCurrentUserAttributes(model, session);
        model.addAttribute("userProfile", userProfile);
        model.addAttribute("storedMovies", fetchStoredMovies(userId, 5));
        model.addAttribute("storedMovieCount", countStoredMovies(userId));
        model.addAttribute("lifeMovies", fetchLifeMovies(userId, 5));
        model.addAttribute("showLifeSearch", showLifeSearch);
        model.addAttribute("lifeQuery", lifeQuery == null ? "" : lifeQuery);
        model.addAttribute("lifeSearchResults", fetchLifeMovieSearchResults(userId, lifeQuery, showLifeSearch));
        return "my-page";
    }

    @GetMapping("/stored")
    public String storedMovies(Model model, HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/login";
        }

        Long userId = getCurrentUserId(session);
        addCurrentUserAttributes(model, session);
        model.addAttribute("storedMovies", fetchStoredMovies(userId, null));
        return "stored-page";
    }

    @GetMapping("/movies/{movieCode}")
    public String detail(@PathVariable String movieCode, Model model, HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/login";
        }

        String loginId = (String) session.getAttribute(LOGIN_SESSION_KEY);
        MovieDetailView movie = jdbcTemplate.query("""
                SELECT
                    m.ranking,
                    m.movie_cd,
                    m.movie_name,
                    m.movie_name_en,
                    m.movie_name_original,
                    m.poster_image_url,
                    (
                        SELECT COUNT(*)
                        FROM user_movie_like uml
                        WHERE uml.movie_id = m.id AND uml.liked = TRUE
                    ) AS like_count,
                    EXISTS(
                        SELECT 1
                        FROM user_movie_like uml
                        JOIN "USER" u ON u.id = uml.user_id
                        WHERE uml.movie_id = m.id
                          AND uml.liked = TRUE
                          AND u.login_id = ?
                    ) AS liked_by_current_user,
                    EXISTS(
                        SELECT 1
                        FROM user_movie_store ums
                        JOIN "USER" u ON u.id = ums.user_id
                        WHERE ums.movie_id = m.id
                          AND u.login_id = ?
                    ) AS stored_by_current_user,
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
                    rs.getBoolean("liked_by_current_user"),
                    rs.getBoolean("stored_by_current_user"),
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
        }, loginId, loginId, movieCode);

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
    public String increaseLike(@PathVariable String movieCode, HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/login";
        }

        String loginId = (String) session.getAttribute(LOGIN_SESSION_KEY);
        Long userId = jdbcTemplate.query("""
                SELECT id
                FROM "USER"
                WHERE login_id = ?
                """, rs -> rs.next() ? rs.getLong("id") : null, loginId);

        Long movieId = jdbcTemplate.query("""
                SELECT id
                FROM movie
                WHERE movie_cd = ?
                """, rs -> rs.next() ? rs.getLong("id") : null, movieCode);

        if (userId == null || movieId == null) {
            throw new ResponseStatusException(NOT_FOUND);
        }

        Boolean alreadyLiked = jdbcTemplate.query("""
                SELECT liked
                FROM user_movie_like
                WHERE user_id = ? AND movie_id = ?
                """, rs -> rs.next() ? rs.getBoolean("liked") : null, userId, movieId);

        if (Boolean.TRUE.equals(alreadyLiked)) {
            jdbcTemplate.update("""
                    DELETE FROM user_movie_like
                    WHERE user_id = ? AND movie_id = ?
                    """, userId, movieId);
        } else {
            jdbcTemplate.update("""
                    INSERT INTO user_movie_like (user_id, movie_id, liked)
                    VALUES (?, ?, TRUE)
                    """, userId, movieId);
        }

        return "redirect:/movies/" + movieCode;
    }

    @PostMapping("/movies/{movieCode}/store")
    @Transactional
    public String toggleStore(@PathVariable String movieCode, HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/login";
        }

        Long userId = getCurrentUserId(session);
        Long movieId = findMovieId(movieCode);

        Integer storedCount = jdbcTemplate.query("""
                SELECT COUNT(*)
                FROM user_movie_store
                WHERE user_id = ? AND movie_id = ?
                """, rs -> rs.next() ? rs.getInt(1) : 0, userId, movieId);

        if (storedCount != null && storedCount > 0) {
            jdbcTemplate.update("""
                    DELETE FROM user_movie_store
                    WHERE user_id = ? AND movie_id = ?
                    """, userId, movieId);
        } else {
            jdbcTemplate.update("""
                    INSERT INTO user_movie_store (user_id, movie_id)
                    VALUES (?, ?)
                    """, userId, movieId);
        }

        return "redirect:/movies/" + movieCode;
    }

    @PostMapping("/mypage/life")
    @Transactional
    public String addLifeMovie(@RequestParam String movieCode, HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/login";
        }

        Long userId = getCurrentUserId(session);
        Long movieId = findMovieId(movieCode);

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
        }

        return "redirect:/mypage?showLifeSearch=true";
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
        model.addAttribute("loginUserId", session.getAttribute(LOGIN_SESSION_KEY));
        model.addAttribute("loginUserNickname", session.getAttribute(LOGIN_NICKNAME_SESSION_KEY));
    }

    private List<MoviePosterView> fetchStoredMovies(Long userId, Integer limit) {
        String sql = """
                SELECT m.ranking, m.movie_cd, m.movie_name, m.movie_name_en, m.poster_image_url
                FROM user_movie_store ums
                JOIN movie m ON m.id = ums.movie_id
                WHERE ums.user_id = ?
                ORDER BY ums.created_at DESC, m.ranking ASC
                """;

        if (limit != null) {
            sql += " LIMIT ?";
            return jdbcTemplate.query(sql, (rs, rowNum) -> new MoviePosterView(
                    rs.getInt("ranking"),
                    rs.getString("movie_cd"),
                    rs.getString("movie_name"),
                    rs.getString("movie_name_en"),
                    rs.getString("poster_image_url")
            ), userId, limit);
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> new MoviePosterView(
                rs.getInt("ranking"),
                rs.getString("movie_cd"),
                rs.getString("movie_name"),
                rs.getString("movie_name_en"),
                rs.getString("poster_image_url")
        ), userId);
    }

    private int countStoredMovies(Long userId) {
        Integer count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM user_movie_store
                WHERE user_id = ?
                """, Integer.class, userId);
        return count == null ? 0 : count;
    }

    private List<MoviePosterView> fetchLifeMovies(Long userId, Integer limit) {
        String sql = """
                SELECT m.ranking, m.movie_cd, m.movie_name, m.movie_name_en, m.poster_image_url
                FROM user_movie_life uml
                JOIN movie m ON m.id = uml.movie_id
                WHERE uml.user_id = ?
                ORDER BY uml.created_at DESC, m.ranking ASC
                """;

        if (limit != null) {
            sql += " LIMIT ?";
            return jdbcTemplate.query(sql, (rs, rowNum) -> new MoviePosterView(
                    rs.getInt("ranking"),
                    rs.getString("movie_cd"),
                    rs.getString("movie_name"),
                    rs.getString("movie_name_en"),
                    rs.getString("poster_image_url")
            ), userId, limit);
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> new MoviePosterView(
                rs.getInt("ranking"),
                rs.getString("movie_cd"),
                rs.getString("movie_name"),
                rs.getString("movie_name_en"),
                rs.getString("poster_image_url")
        ), userId);
    }

    private List<LifeMovieSearchResultView> fetchLifeMovieSearchResults(Long userId, String lifeQuery, boolean showLifeSearch) {
        if (!showLifeSearch || lifeQuery == null || lifeQuery.isBlank()) {
            return List.of();
        }

        String keyword = "%" + lifeQuery.trim() + "%";
        return jdbcTemplate.query("""
                SELECT
                    m.movie_cd,
                    m.movie_name,
                    m.movie_name_en,
                    m.poster_image_url,
                    EXISTS(
                        SELECT 1
                        FROM user_movie_life uml
                        WHERE uml.user_id = ? AND uml.movie_id = m.id
                    ) AS already_added
                FROM movie m
                WHERE UPPER(m.movie_name) LIKE UPPER(?)
                   OR UPPER(COALESCE(m.movie_name_en, '')) LIKE UPPER(?)
                ORDER BY m.ranking ASC, m.movie_name ASC
                LIMIT 12
                """, (rs, rowNum) -> new LifeMovieSearchResultView(
                rs.getString("movie_cd"),
                rs.getString("movie_name"),
                rs.getString("movie_name_en"),
                rs.getString("poster_image_url"),
                rs.getBoolean("already_added")
        ), userId, keyword, keyword);
    }

    private List<String> fetchOrderedNames(String sql, String movieCode) {
        return jdbcTemplate.query(sql, (rs, rowNum) -> rs.getString(1), movieCode);
    }

    private List<String> fetchChartGenres() {
        return jdbcTemplate.query("""
                SELECT name
                FROM genre
                ORDER BY name ASC
                """, (rs, rowNum) -> rs.getString("name"));
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
            boolean storedByCurrentUser,
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
    }

    public record CompanyView(String name, String role) {
    }

    public record AuditView(String auditNo, String watchGradeName) {
    }

    public record UserProfileView(String loginId, String nickname, String gender, int age) {
    }

    public record LifeMovieSearchResultView(
            String movieCode,
            String movieName,
            String movieNameEn,
            String posterImageUrl,
            boolean alreadyAdded
    ) {
    }

    private record LoginUser(String loginId, String nickname) {
    }
}
