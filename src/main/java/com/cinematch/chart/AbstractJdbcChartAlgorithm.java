package com.cinematch.chart;

import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDate;
import java.util.List;

/**
 * JDBC 기반 차트 알고리즘의 공통 베이스 클래스.
 * JdbcTemplate 주입과 공통 매핑 로직을 제공하여 하위 알고리즘 클래스의 중복을 줄인다.
 */
public abstract class AbstractJdbcChartAlgorithm implements ChartAlgorithm {

    protected final JdbcTemplate jdbcTemplate;

    protected AbstractJdbcChartAlgorithm(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * 하위 클래스가 ORDER BY와 WHERE 조건을 포함한 완성된 SQL을 반환하면,
     * 이 메서드가 공통 SELECT 컬럼과 LIMIT을 붙여 실행 후 결과를 매핑한다.
     *
     * SQL에는 반드시 다음 컬럼 별칭이 포함되어야 한다:
     *   movie_code, movie_name, movie_name_en, poster_image_url,
     *   open_date, production_year, metric_value
     *
     * @param innerSql 서브쿼리 또는 완성된 SELECT 문
     * @param params   바인딩 파라미터
     * @param limit    최대 행 수
     * @param metricLabel 지표 레이블
     * @param badgeResolver 배지 텍스트 결정 함수 (null이면 배지 없음)
     */
    protected List<ChartMovieRow> runQuery(
            String innerSql,
            Object[] params,
            int limit,
            String metricLabel,
            java.util.function.Function<java.sql.ResultSet, String> badgeResolver
    ) {
        String sql = innerSql + " LIMIT " + Math.max(1, Math.min(limit, 200));
        List<ChartMovieRow> rows = jdbcTemplate.query(sql, (rs, rowNum) -> new ChartMovieRow(
                rowNum + 1,
                rs.getString("movie_code"),
                rs.getString("movie_name"),
                rs.getString("movie_name_en"),
                rs.getString("poster_image_url"),
                rs.getObject("open_date", LocalDate.class),
                rs.getObject("production_year", Integer.class),
                metricLabel,
                rs.getString("metric_value"),
                badgeResolver != null ? safeResolveBadge(badgeResolver, rs) : null
        ), params);

        return rows.stream()
                .filter(row -> row.posterImageUrl() != null && !row.posterImageUrl().isBlank())
                .toList();
    }

    /** badge resolver 실행 중 예외 발생 시 null 반환 */
    private String safeResolveBadge(java.util.function.Function<java.sql.ResultSet, String> resolver, java.sql.ResultSet rs) {
        try {
            return resolver.apply(rs);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 공통 SELECT 컬럼 프래그먼트.
     * 하위 클래스에서 FROM절과 조건을 이어 붙일 때 사용한다.
     */
    protected static final String BASE_SELECT = """
            SELECT
                m.movie_cd                                          AS movie_code,
                COALESCE(m.title, m.movie_name)                    AS movie_name,
                COALESCE(m.movie_name_en, m.original_title)        AS movie_name_en,
                m.poster_image_url,
                COALESCE(m.release_date, m.box_office_open_date)   AS open_date,
                m.production_year
            FROM movie m
            """;

    /**
     * 숫자를 "1,234만" 형태로 포맷 (Java 쪽 포맷이 필요한 경우 사용).
     */
    protected String formatManwon(long value) {
        if (value >= 100_000_000L) {
            return String.format("%.1f억원", value / 100_000_000.0);
        }
        if (value >= 10_000L) {
            return String.format("%,d만원", value / 10_000);
        }
        return String.format("%,d원", value);
    }

    /** 숫자를 "1,234만 명" 형태로 포맷 */
    protected String formatAudi(long value) {
        if (value >= 10_000_000L) {
            return String.format("%.0f만 명", value / 10_000.0);
        }
        return String.format("%,d 명", value);
    }
}
