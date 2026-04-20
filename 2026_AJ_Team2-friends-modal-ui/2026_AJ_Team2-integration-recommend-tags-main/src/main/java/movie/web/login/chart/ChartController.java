package movie.web.login.chart;

import jakarta.servlet.http.HttpSession;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.Arrays;
import java.util.List;

/**
 * 랭킹 차트 페이지 컨트롤러.
 *
 * GET /ranking          → 차트 목록 페이지 (ranking.html)
 * GET /ranking/{code}   → 특정 차트 순위 페이지 (ranking-detail.html)
 *
 * 세션에서 로그인 유저 닉네임을 꺼내 Navbar 표시용으로 Model에 추가한다.
 * 세션 키는 기존 프로젝트의 LoginController 규칙을 따른다.
 */
@Controller
public class ChartController {

    /** 기존 프로젝트의 세션 닉네임 키 (LoginController와 동일하게 맞출 것) */
    private static final String SESSION_NICKNAME_KEY = "loginUserNickname";

    /** 차트 1페이지 기본 표시 개수 */
    private static final int DEFAULT_LIMIT = 30;

    private final ChartRegistry chartRegistry;

    public ChartController(ChartRegistry chartRegistry) {
        this.chartRegistry = chartRegistry;
    }

    /**
     * 랭킹 차트 목록 페이지.
     * 카테고리 파라미터가 있으면 해당 카테고리 차트만 필터링해 보여준다.
     *
     * @param category 카테고리 코드 (선택, 없으면 전체 표시)
     * @param model    Thymeleaf 모델
     * @param session  로그인 세션
     */
    @GetMapping("/ranking")
    public String rankingIndex(
            @RequestParam(required = false) String category,
            Model model,
            HttpSession session
    ) {
        if (session.getAttribute("loginUserId") == null) return "redirect:/login";
        ChartCategory selectedCategory = parseCategory(category);

        List<ChartSection> sections = (selectedCategory == null
                ? chartRegistry.allAlgorithms()
                : chartRegistry.algorithmsByCategory(selectedCategory))
                .stream()
                .map(a -> new ChartSection(ChartEntry.of(a), a.fetch(10)))
                .toList();

        model.addAttribute("loginUserNickname", session.getAttribute(SESSION_NICKNAME_KEY));
        model.addAttribute("sections", sections);
        model.addAttribute("categories", ChartCategory.values());
        model.addAttribute("selectedCategory", selectedCategory);
        model.addAttribute("totalCount", chartRegistry.size());

        return "ranking";
    }

    /**
     * 특정 차트 상세 순위 페이지.
     * code에 해당하는 알고리즘을 Registry에서 찾아 fetch()를 호출한 뒤 결과를 모델에 넣는다.
     *
     * @param code   알고리즘 코드 (예: "top-sales")
     * @param limit  표시할 최대 순위 수 (기본 30, 최대 100)
     * @param model  Thymeleaf 모델
     * @param session 로그인 세션
     */
    @GetMapping("/ranking/{code}")
    public String rankingDetail(
            @PathVariable String code,
            @RequestParam(defaultValue = "30") int limit,
            Model model,
            HttpSession session
    ) {
        if (session.getAttribute("loginUserId") == null) return "redirect:/login";
        // 알고리즘이 없으면 목록 페이지로 리다이렉트
        ChartAlgorithm algorithm = chartRegistry.find(code).orElse(null);
        if (algorithm == null) {
            return "redirect:/ranking";
        }

        // limit 범위 보정 (1~100)
        int normalizedLimit = Math.max(1, Math.min(limit, 100));

        // 알고리즘 실행 — DB 조회 및 정렬
        List<ChartMovieRow> rows = algorithm.fetch(normalizedLimit);

        // 같은 카테고리의 다른 차트 목록 (사이드 또는 하단 연관 차트용)
        List<ChartEntry> relatedCharts = chartRegistry.entriesByCategory(algorithm.category())
                .stream()
                .filter(e -> !e.code().equals(code))   // 현재 차트 제외
                .limit(4)
                .toList();

        model.addAttribute("loginUserNickname", session.getAttribute(SESSION_NICKNAME_KEY));
        model.addAttribute("algorithm", algorithm);       // 차트 메타정보 (제목·설명·카테고리)
        model.addAttribute("rows", rows);                 // 순위 결과 목록
        model.addAttribute("limit", normalizedLimit);
        model.addAttribute("relatedCharts", relatedCharts); // 연관 차트

        return "ranking-detail";
    }

    /**
     * 카테고리 문자열을 ChartCategory enum으로 변환한다.
     * 매핑 실패 시 null 반환 (= 전체 표시).
     */
    private ChartCategory parseCategory(String category) {
        if (category == null || category.isBlank()) {
            return null;
        }
        return Arrays.stream(ChartCategory.values())
                .filter(c -> c.name().equalsIgnoreCase(category.trim()))
                .findFirst()
                .orElse(null);
    }
}
