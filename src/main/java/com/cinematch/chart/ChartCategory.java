package com.cinematch.chart;

/**
 * 차트 알고리즘을 분류하는 카테고리 구분값.
 * 랭킹 페이지에서 카테고리 탭과 섹션 배지 렌더링에 사용된다.
 */
public enum ChartCategory {

    /** TMDB movie/day 기준 실시간 인기작 */
    TRENDING("실시간 인기"),

    /** 누적 매출 기준 순위 */
    BOXOFFICE("박스오피스"),

    /** 관객수 기준 순위 */
    AUDIENCE("관객"),

    /** 효율/가성비 기반 순위 */
    EFFICIENCY("효율"),

    /** 시간/연대 기반 순위 */
    TIME("시대"),

    /** 장르 기반 순위 */
    GENRE("장르"),

    /** 감독/배우 기반 순위 */
    PEOPLE("인물"),

    /** TMDB 평점 기반 순위 */
    RATING("평점"),

    /** 서비스 내 유저 활동 기반 순위 */
    COMMUNITY("커뮤니티");

    private final String label;

    ChartCategory(String label) {
        this.label = label;
    }

    public String label() {
        return label;
    }
}
