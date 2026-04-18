package movie.web.login.chart;

import java.util.List;

/**
 * 랭킹 차트 알고리즘의 공통 인터페이스.
 * 모든 차트 알고리즘은 이 인터페이스를 구현하여 일관된 방식으로 조회된다.
 */
public interface ChartAlgorithm {

    /**
     * 알고리즘 고유 코드 (URL 파라미터 및 식별자로 사용).
     * 예: "top-sales", "million-club"
     */
    String code();

    /**
     * 화면에 표시할 차트 이름.
     */
    String title();

    /**
     * 차트에 대한 짧은 설명 (카드 하단 등에 표시).
     */
    String description();

    /**
     * 차트 카테고리 (BOXOFFICE, AUDIENCE, EFFICIENCY, TIME, GENRE, PEOPLE).
     */
    ChartCategory category();

    /**
     * 차트 아이콘 식별자 (HTML 템플릿에서 SVG 분기 처리).
     */
    String icon();

    /**
     * 상위 N개 영화 결과를 조회하여 반환.
     * @param limit 최대 조회 개수
     */
    List<ChartMovieRow> fetch(int limit);
}
