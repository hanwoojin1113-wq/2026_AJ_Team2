package com.cinematch.chart;

/**
 * 차트 알고리즘을 분류하는 카테고리 열거형.
 * 랭킹 페이지에서 카테고리별 탭 필터링에 사용된다.
 */
public enum ChartCategory {

    /** 매출액 기반 순위 */
    BOXOFFICE("박스오피스"),

    /** 관객수 기반 순위 */
    AUDIENCE("관객"),

    /** 효율성·가성비 기반 순위 */
    EFFICIENCY("효율"),

    /** 시간·시대 기반 순위 */
    TIME("시대"),

    /** 장르 기반 순위 */
    GENRE("장르"),

    /** 인물(감독·제작사) 기반 순위 */
    PEOPLE("인물");

    private final String label;

    ChartCategory(String label) {
        this.label = label;
    }

    public String label() {
        return label;
    }
}
