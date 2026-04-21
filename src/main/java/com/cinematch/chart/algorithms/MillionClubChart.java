package com.cinematch.chart.algorithms;

import com.cinematch.chart.AbstractJdbcChartAlgorithm;
import com.cinematch.chart.ChartCategory;
import com.cinematch.chart.ChartMovieRow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * [차트 2] 천만 클럽
 * 누적 관객 1000만 명 이상 달성한 영화들을 관객수 내림차순으로 표시.
 * 한국 영화 문화의 대표적 기준점인 '천만 관객'을 달성한 작품만 선별한다.
 */
@Component
public class MillionClubChart extends AbstractJdbcChartAlgorithm {

    public MillionClubChart(JdbcTemplate jdbcTemplate) {
        super(jdbcTemplate);
    }

    @Override public String code()        { return "million-club"; }
    @Override public String title()       { return "천만 클럽"; }
    @Override public String description() { return "누적 관객 1,000만 명을 돌파한 영화 모음"; }
    @Override public ChartCategory category() { return ChartCategory.AUDIENCE; }
    @Override public String icon()        { return "users"; }

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
                    CAST(ROUND(m.box_office_audi_acc / 10000.0) AS VARCHAR) || '만 명' AS metric_value,
                    m.box_office_audi_acc                              AS raw_audi
                FROM movie m
                WHERE m.box_office_audi_acc >= 10000000
                ORDER BY m.box_office_audi_acc DESC
                """,
                new Object[]{}, limit, "누적 관객",
                rs -> "천만");
    }
}
