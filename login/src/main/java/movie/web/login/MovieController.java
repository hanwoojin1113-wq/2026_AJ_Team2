package movie.web.login;

import java.time.LocalDate;
import java.util.List;

import jakarta.servlet.http.HttpSession;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.server.ResponseStatusException;

import static org.springframework.http.HttpStatus.NOT_FOUND;

@Controller
public class MovieController {

    private static final String LOGIN_SESSION_KEY = "loginUserId";
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
        String loginId = jdbcTemplate.query("""
                SELECT login_id
                FROM "USER"
                WHERE login_id = ? AND login_pw = ?
                """, rs -> rs.next() ? rs.getString("login_id") : null, id, pw);

        if (loginId == null) {
            return "redirect:/login?error=1";
        }

        session.setAttribute(LOGIN_SESSION_KEY, loginId);
        return "redirect:/charts";
    }

    @PostMapping("/signup")
    public String signup(@RequestParam String id, @RequestParam String pw, @RequestParam String gender,
                         @RequestParam Integer age) {
        if ((!gender.equals("MALE") && !gender.equals("FEMALE")) || age == null || age < 1 || age > 120) {
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
                INSERT INTO "USER" (login_id, login_pw, gender, age)
                VALUES (?, ?, ?, ?)
                """, id, pw, gender, age);

        return "redirect:/login";
    }

    @GetMapping("/logout")
    public String logout(HttpSession session) {
        session.invalidate();
        return "redirect:/login";
    }

    @GetMapping("/charts")
    public String home(@RequestParam(defaultValue = "1") int page, Model model, HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/login";
        }

        int totalMovies = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM movie", Integer.class);
        int totalPages = Math.max(1, (int) Math.ceil((double) totalMovies / PAGE_SIZE));
        int currentPage = Math.min(Math.max(page, 1), totalPages);
        int offset = (currentPage - 1) * PAGE_SIZE;

        List<MoviePosterView> movies = jdbcTemplate.query("""
                SELECT ranking, movie_cd, movie_name, movie_name_en, poster_image_url
                FROM movie
                ORDER BY ranking ASC
                LIMIT ? OFFSET ?
                """, (rs, rowNum) -> new MoviePosterView(
                rs.getInt("ranking"),
                rs.getString("movie_cd"),
                rs.getString("movie_name"),
                rs.getString("movie_name_en"),
                rs.getString("poster_image_url")
        ), PAGE_SIZE, offset);

        model.addAttribute("movies", movies);
        model.addAttribute("loginUserId", session.getAttribute(LOGIN_SESSION_KEY));
        model.addAttribute("currentPage", currentPage);
        model.addAttribute("totalPages", totalPages);
        model.addAttribute("hasPrevious", currentPage > 1);
        model.addAttribute("hasNext", currentPage < totalPages);
        model.addAttribute("previousPage", currentPage - 1);
        model.addAttribute("nextPage", currentPage + 1);
        model.addAttribute("pages", java.util.stream.IntStream.rangeClosed(1, totalPages).boxed().toList());
        return "index";
    }

    @GetMapping("/movies/{movieCode}")
    public String detail(@PathVariable String movieCode, Model model, HttpSession session) {
        if (!isLoggedIn(session)) {
            return "redirect:/login";
        }

        MovieDetailView movie = jdbcTemplate.query("""
                SELECT
                    m.ranking,
                    m.movie_cd,
                    m.movie_name,
                    m.movie_name_en,
                    m.movie_name_original,
                    m.poster_image_url,
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
        model.addAttribute("loginUserId", session.getAttribute(LOGIN_SESSION_KEY));
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

    private boolean isLoggedIn(HttpSession session) {
        return session.getAttribute(LOGIN_SESSION_KEY) != null;
    }

    private List<String> fetchOrderedNames(String sql, String movieCode) {
        return jdbcTemplate.query(sql, (rs, rowNum) -> rs.getString(1), movieCode);
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
}
