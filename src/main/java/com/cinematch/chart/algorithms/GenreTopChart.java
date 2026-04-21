package com.cinematch.chart.algorithms;

import com.cinematch.chart.AbstractJdbcChartAlgorithm;
import com.cinematch.chart.ChartCategory;
import com.cinematch.chart.ChartMovieRow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * [차트 11] 장르별 역대 1위
 * 각 장르(genre 테이블)마다 관객 수 1위 영화를 하나씩 추출하여 나열한다.
 * 장르당 하나의 대표작을 보여주므로 다양한 취향의 유저에게 입문 영화를 추천하는 역할을 한다.
 * 관객 수 기준으로 장르 내 1위를 선택하고, 결과는 관객 수 내림차순 정렬.
 */
@Component
public class GenreTopChart extends AbstractJdbcChartAlgorithm {

    public GenreTopChart(JdbcTemplate jdbcTemplate) {
        super(jdbcTemplate);
    }

    @Override public String code()        { return "genre-top"; }
    @Override public String title()       { return "장르별 역대 1위"; }
    @Override public String description() { return "액션, 드라마, 코미디… 각 장르를 대표하는 최고 흥행작"; }
    @Override public ChartCategory category() { return ChartCategory.GENRE; }
    @Override public String icon()        { return "tag"; }

    @Override
    public List<ChartMovieRow> fetch(int limit) {
        /*
         * movie_genre 조인 테이블을 통해 장르별로 파티셔닝.
         * 각 장르 내 box_office_audi_acc 최대값 영화를 ROW_NUMBER()로 선택.
         * display_order = 1 인 첫 번째 장르 기준으로만 대표 장르 뱃지를 표시한다.
         */
        return runQuery("""
                SELECT movie_code, movie_name, movie_name_en, poster_image_url,
                       open_date, production_year, genre_name, metric_value
                FROM (
                    SELECT
                        movie_code, movie_name, movie_name_en, poster_image_url,
                        open_date, production_year, genre_name, raw_audi,
                        CAST(ROUND(raw_audi / 10000.0) AS VARCHAR) || '만 명' AS metric_value,
                        ROW_NUMBER() OVER (
                            PARTITION BY movie_code
                            ORDER BY raw_audi DESC
                        ) AS movie_rn
                    FROM (
                        SELECT
                            m.movie_cd                                          AS movie_code,
                            COALESCE(m.title, m.movie_name)                    AS movie_name,
                            COALESCE(m.movie_name_en, m.original_title)        AS movie_name_en,
                            m.poster_image_url,
                            COALESCE(m.release_date, m.box_office_open_date)   AS open_date,
                            m.production_year,
                            g.name                                             AS genre_name,
                            m.box_office_audi_acc                              AS raw_audi,
                            ROW_NUMBER() OVER (
                                PARTITION BY g.id
                                ORDER BY m.box_office_audi_acc DESC
                            ) AS rn
                        FROM movie m
                        JOIN movie_genre mg ON mg.movie_id = m.id
                        JOIN genre g ON g.id = mg.genre_id
                        WHERE m.box_office_audi_acc IS NOT NULL
                    ) ranked
                    WHERE rn = 1
                ) deduped
                WHERE movie_rn = 1
                ORDER BY raw_audi DESC
                """,
                new Object[]{}, limit, "장르 1위 관객",
                rs -> {
                    try {
                        return rs.getString("genre_name") + " 1위";
                    } catch (Exception e) { return null; }
                });
    }
}