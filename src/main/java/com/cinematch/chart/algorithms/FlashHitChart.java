package com.cinematch.chart.algorithms;

import com.cinematch.chart.AbstractJdbcChartAlgorithm;
import com.cinematch.chart.ChartCategory;
import com.cinematch.chart.ChartMovieRow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * [차트 7] 반짝 흥행 영화
 * 일평균 매출 = box_office_sales_acc / DATEDIFF(CURRENT_DATE, box_office_open_date) 내림차순.
 * 개봉 초반에 폭발적인 집중 관람이 일어난 영화를 찾는다.
 * 입소문보다 개봉 전 기대감과 초기 충성 팬덤이 강했던 작품이 상위에 오른다.
 */
@Component
public class FlashHitChart extends AbstractJdbcChartAlgorithm {

    public FlashHitChart(JdbcTemplate jdbcTemplate) {
        super(jdbcTemplate);
    }

    @Override public String code()        { return "flash-hit"; }
    @Override public String title()       { return "반짝 흥행 영화"; }
    @Override public String description() { return "개봉 초반에 매출이 집중된 폭발적 흥행작"; }
    @Override public ChartCategory category() { return ChartCategory.TIME; }
    @Override public String icon()        { return "zap"; }

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
                    CAST(ROUND(m.box_office_sales_acc * 1.0
                        / GREATEST(DATEDIFF('DAY', COALESCE(m.release_date, m.box_office_open_date), CURRENT_DATE), 1)
                        / 10000)
                         AS VARCHAR) || '만원/일'                      AS metric_value
                FROM movie m
                WHERE m.box_office_sales_acc IS NOT NULL
                  AND COALESCE(m.release_date, m.box_office_open_date) IS NOT NULL
                ORDER BY (m.box_office_sales_acc * 1.0
                    / GREATEST(DATEDIFF('DAY', COALESCE(m.release_date, m.box_office_open_date), CURRENT_DATE), 1)) DESC
                """,
                new Object[]{}, limit, "일평균 매출",
                rs -> "반짝 흥행");
    }
}