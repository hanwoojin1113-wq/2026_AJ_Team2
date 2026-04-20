package movie.web.login.chart.algorithms;

import movie.web.login.chart.AbstractJdbcChartAlgorithm;
import movie.web.login.chart.ChartCategory;
import movie.web.login.chart.ChartMovieRow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * [차트 10] 연대별 흥행왕
 * 10년 단위로 묶어 각 연대의 관객 1위 영화를 표시한다.
 * FLOOR(prdtYear / 10) * 10 으로 연대를 계산하고,
 * 각 연대 내 box_office_audi_acc 최대값을 가진 영화를 하나씩 추출한다.
 * 결과는 최신 연대부터 내림차순으로 정렬된다.
 */
@Component
public class DecadeChart extends AbstractJdbcChartAlgorithm {

    public DecadeChart(JdbcTemplate jdbcTemplate) {
        super(jdbcTemplate);
    }

    @Override public String code()        { return "decade"; }
    @Override public String title()       { return "연대별 흥행왕"; }
    @Override public String description() { return "90년대부터 현재까지, 각 10년을 대표하는 최고 흥행작"; }
    @Override public ChartCategory category() { return ChartCategory.TIME; }
    @Override public String icon()        { return "calendar"; }

    @Override
    public List<ChartMovieRow> fetch(int limit) {
        /*
         * 각 연대(decade)에서 box_office_audi_acc가 가장 높은 영화 한 편을 선택.
         * ROW_NUMBER() OVER (PARTITION BY decade ORDER BY audi DESC) = 1 로 연대 대표작 추출.
         * H2 DB 윈도우 함수 지원.
         */
        return runQuery("""
                SELECT
                    movie_code,
                    movie_name,
                    movie_name_en,
                    poster_image_url,
                    open_date,
                    production_year,
                    CAST(ROUND(raw_audi / 10000.0) AS VARCHAR) || '만 명 (' || decade || '년대)' AS metric_value,
                    decade
                FROM (
                    SELECT
                        m.movie_cd                                          AS movie_code,
                        COALESCE(m.title, m.movie_name)                    AS movie_name,
                        COALESCE(m.movie_name_en, m.original_title)        AS movie_name_en,
                        m.poster_image_url,
                        COALESCE(m.release_date, m.box_office_open_date)   AS open_date,
                        m.production_year,
                        m.box_office_audi_acc                              AS raw_audi,
                        FLOOR(COALESCE(m.production_year,
                              YEAR(COALESCE(m.release_date, m.box_office_open_date))) / 10) * 10 AS decade,
                        ROW_NUMBER() OVER (
                            PARTITION BY FLOOR(COALESCE(m.production_year,
                                              YEAR(COALESCE(m.release_date, m.box_office_open_date))) / 10) * 10
                            ORDER BY m.box_office_audi_acc DESC
                        ) AS rn
                    FROM movie m
                    WHERE m.box_office_audi_acc IS NOT NULL
                      AND COALESCE(m.production_year,
                            YEAR(COALESCE(m.release_date, m.box_office_open_date))) IS NOT NULL
                ) ranked
                WHERE rn = 1
                ORDER BY decade DESC
                """,
                new Object[]{}, limit, "연대 1위",
                rs -> {
                    try {
                        return rs.getInt("decade") + "년대 1위";
                    } catch (Exception e) { return null; }
                });
    }
}
