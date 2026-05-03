package com.cinematch.chart.algorithms;

import com.cinematch.chart.AbstractJdbcChartAlgorithm;
import com.cinematch.chart.ChartCategory;
import com.cinematch.chart.ChartMovieRow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * [차트 14] 세계 흥행 순위
 * TMDB에서 가져온 글로벌 revenue(달러 기준) 내림차순.
 * 국내 박스오피스와 다른 시각으로 세계가 선택한 최고 흥행작을 보여준다.
 */
@Component
public class GlobalHitChart extends AbstractJdbcChartAlgorithm {

    public GlobalHitChart(JdbcTemplate jdbcTemplate) {
        super(jdbcTemplate);
    }

    @Override public String code()        { return "global-hit"; }
    @Override public String title()       { return "세계 흥행 순위"; }
    @Override public String description() { return "전 세계 박스오피스 수익 기준 — 국경을 넘은 최고 흥행작"; }
    @Override public ChartCategory category() { return ChartCategory.BOXOFFICE; }
    @Override public String icon()        { return "globe"; }

    @Override
    public List<ChartMovieRow> fetch(int limit) {
        return runQuery("""
                SELECT
                    m.movie_cd                                          AS movie_code,
                    COALESCE(m.title, m.movie_name)                    AS movie_name,
                    COALESCE(m.movie_name_en, m.original_title)        AS movie_name_en,
                    m.poster_image_url,
                    COALESCE(m.release_date, m.box_office_open_date)   AS open_date,
                    m.production_year,
                    m.revenue                                          AS raw_revenue,
                    CASE
                        WHEN m.revenue >= 1000000000
                            THEN CONCAT(CAST(CAST(m.revenue / 100000000 AS BIGINT) AS VARCHAR), '억$')
                        WHEN m.revenue >= 1000000
                            THEN CONCAT(CAST(CAST(m.revenue / 1000000 AS BIGINT) AS VARCHAR), '백만$')
                        ELSE CONCAT(CAST(m.revenue AS VARCHAR), '$')
                    END                                                AS metric_value
                FROM movie m
                WHERE m.revenue IS NOT NULL
                  AND m.revenue > 0
                ORDER BY m.revenue DESC
                """,
                new Object[]{}, limit, "세계 흥행 수익",
                rs -> {
                    try {
                        long rev = rs.getLong("raw_revenue");
                        return rev >= 1_000_000_000L ? "10억$+" : null;
                    } catch (Exception e) { return null; }
                });
    }
}
