package com.cinematch.chart.algorithms;

import com.cinematch.chart.AbstractJdbcChartAlgorithm;
import com.cinematch.chart.ChartCategory;
import com.cinematch.chart.ChartMovieRow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * [차트 4] 스크린 독점 없이 흥행한 영화
 * 스크린 수가 전체 중앙값 이하임에도 관객 상위권에 든 영화.
 * 대작의 스크린 독과점 없이 입소문만으로 흥행한 진짜 명작을 발굴한다.
 */
@Component
public class UnderscreenChart extends AbstractJdbcChartAlgorithm {

    public UnderscreenChart(JdbcTemplate jdbcTemplate) {
        super(jdbcTemplate);
    }

    @Override public String code()        { return "underscreen"; }
    @Override public String title()       { return "스크린 독점 없이 흥행"; }
    @Override public String description() { return "적은 스크린으로도 많은 관객을 모은 입소문 흥행작"; }
    @Override public ChartCategory category() { return ChartCategory.EFFICIENCY; }
    @Override public String icon()        { return "eye"; }

    @Override
    public List<ChartMovieRow> fetch(int limit) {
        /*
         * 스크린 수가 전체 데이터의 중앙값(PERCENTILE_CONT 사용) 이하인 영화 중
         * 관객 수 내림차순 정렬.
         * H2 DB는 PERCENTILE_CONT를 지원하므로 서브쿼리로 중앙값 계산.
         */
        return runQuery("""
                SELECT
                    m.movie_cd                                          AS movie_code,
                    COALESCE(m.title, m.movie_name)                    AS movie_name,
                    COALESCE(m.movie_name_en, m.original_title)        AS movie_name_en,
                    m.poster_image_url,
                    COALESCE(m.release_date, m.box_office_open_date)   AS open_date,
                    m.production_year,
                    CAST(ROUND(m.box_office_audi_acc / 10000.0) AS VARCHAR) || '만 명' AS metric_value,
                    m.box_office_scrn_cnt                              AS raw_scrn
                FROM movie m
                WHERE m.box_office_audi_acc IS NOT NULL
                  AND m.box_office_scrn_cnt IS NOT NULL
                  AND m.box_office_scrn_cnt <= (
                      SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY m2.box_office_scrn_cnt)
                      FROM movie m2
                      WHERE m2.box_office_scrn_cnt IS NOT NULL
                  )
                ORDER BY m.box_office_audi_acc DESC
                """,
                new Object[]{}, limit, "누적 관객",
                rs -> "소수 스크린");
    }
}
