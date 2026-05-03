package com.cinematch.chart.algorithms;

import com.cinematch.chart.AbstractJdbcChartAlgorithm;
import com.cinematch.chart.ChartCategory;
import com.cinematch.chart.ChartMovieRow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * [차트 13] 평점 명작
 * TMDB 평균 평점(vote_average) 기준으로 내림차순 정렬.
 * 신뢰성을 위해 투표 수 200개 이상인 영화만 포함한다.
 * 박스오피스 흥행과 무관하게 관객과 평론가에게 검증된 명작을 발굴한다.
 */
@Component
public class HighRatedChart extends AbstractJdbcChartAlgorithm {

    public HighRatedChart(JdbcTemplate jdbcTemplate) {
        super(jdbcTemplate);
    }

    @Override public String code()        { return "high-rated"; }
    @Override public String title()       { return "평점 명작"; }
    @Override public String description() { return "TMDB 평점 상위 — 흥행보다 작품성으로 검증된 영화"; }
    @Override public ChartCategory category() { return ChartCategory.RATING; }
    @Override public String icon()        { return "star"; }

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
                    m.vote_average                                     AS vote_avg_raw,
                    CONCAT(
                        CAST(ROUND(m.vote_average * 10) / 10.0 AS DECIMAL(3, 1)),
                        '점 (', CAST(m.vote_count AS VARCHAR), '명 투표)'
                    )                                                  AS metric_value
                FROM movie m
                WHERE m.vote_average IS NOT NULL
                  AND m.vote_count >= 200
                ORDER BY m.vote_average DESC, m.vote_count DESC
                """,
                new Object[]{}, limit, "TMDB 평점",
                rs -> {
                    try {
                        double avg = rs.getDouble("vote_avg_raw");
                        if (avg >= 8.5) return "★ 명작";
                        if (avg >= 8.0) return "★ 수작";
                        return null;
                    } catch (Exception e) { return null; }
                });
    }
}
