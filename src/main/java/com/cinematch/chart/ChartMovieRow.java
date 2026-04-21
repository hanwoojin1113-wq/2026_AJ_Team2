package com.cinematch.chart;

import java.time.LocalDate;

/**
 * 차트 조회 결과 한 행(영화 1개)을 담는 읽기 전용 DTO.
 * 각 알고리즘이 공통으로 반환하는 형태로, 템플릿에서 일관되게 렌더링된다.
 *
 * @param rankNo          차트 내 순위 번호 (1부터 시작)
 * @param movieCode       KOBIS 영화 코드 (상세 페이지 URL에 사용)
 * @param movieName       한국어 제목
 * @param movieNameEn     영어 제목 (없을 수 있음)
 * @param posterImageUrl  포스터 이미지 URL (없을 수 있음)
 * @param openDate        개봉일 (없을 수 있음)
 * @param productionYear  제작연도 (없을 수 있음)
 * @param metricLabel     이 차트에서 강조할 지표 이름 (예: "누적 관객", "일평균 매출")
 * @param metricValue     지표 값을 포맷된 문자열로 표현 (예: "1,761만 명", "5,286만원/일")
 * @param badgeLabel      포스터 위에 표시할 뱃지 텍스트 (예: "천만", "장기 흥행", 없으면 null)
 */
public record ChartMovieRow(
        int rankNo,
        String movieCode,
        String movieName,
        String movieNameEn,
        String posterImageUrl,
        LocalDate openDate,
        Integer productionYear,
        String metricLabel,
        String metricValue,
        String badgeLabel
) {
    /**
     * 뱃지 없이 생성할 때 사용하는 편의 팩토리.
     */
    public static ChartMovieRow of(
            int rankNo,
            String movieCode,
            String movieName,
            String movieNameEn,
            String posterImageUrl,
            LocalDate openDate,
            Integer productionYear,
            String metricLabel,
            String metricValue
    ) {
        return new ChartMovieRow(
                rankNo, movieCode, movieName, movieNameEn,
                posterImageUrl, openDate, productionYear,
                metricLabel, metricValue, null
        );
    }
}
