package com.cinematch.chart;

/**
 * 랭킹 목록 페이지(index)에서 카드 형태로 표시되는 차트 메타데이터.
 * 실제 영화 데이터는 포함하지 않으며, 클릭 시 상세 차트 페이지로 이동한다.
 *
 * @param code        알고리즘 고유 코드 (/ranking/{code} URL에 사용)
 * @param title       차트 이름
 * @param description 차트 설명
 * @param category    카테고리
 * @param icon        아이콘 식별자
 * @param isNew       신규 차트 여부 (NEW 뱃지 표시)
 * @param isHot       인기 차트 여부 (HOT 뱃지 표시)
 */
public record ChartEntry(
        String code,
        String title,
        String description,
        ChartCategory category,
        String icon,
        boolean isNew,
        boolean isHot
) {
    public static ChartEntry of(ChartAlgorithm algorithm) {
        return new ChartEntry(
                algorithm.code(),
                algorithm.title(),
                algorithm.description(),
                algorithm.category(),
                algorithm.icon(),
                false,
                false
        );
    }

    public static ChartEntry hot(ChartAlgorithm algorithm) {
        return new ChartEntry(
                algorithm.code(),
                algorithm.title(),
                algorithm.description(),
                algorithm.category(),
                algorithm.icon(),
                false,
                true
        );
    }

    public static ChartEntry newChart(ChartAlgorithm algorithm) {
        return new ChartEntry(
                algorithm.code(),
                algorithm.title(),
                algorithm.description(),
                algorithm.category(),
                algorithm.icon(),
                true,
                false
        );
    }
}
