package movie.web.login.chart.algorithms;

import movie.web.login.chart.AbstractJdbcChartAlgorithm;
import movie.web.login.chart.ChartCategory;
import movie.web.login.chart.ChartMovieRow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * [차트 8] 스크린당 상영 횟수 순위 (장기 상영 지표)
 * box_office_show_cnt / box_office_scrn_cnt 내림차순.
 * 같은 스크린에서 하루에 여러 번 상영 = 좌석 회전율이 높음 = 극장이 계속 틀어준 장기 흥행작.
 * 과속스캔들, 왕의 남자처럼 오래도록 입소문 타며 상영된 영화가 상위권에 오른다.
 */
@Component
public class LongRunnerChart extends AbstractJdbcChartAlgorithm {

    public LongRunnerChart(JdbcTemplate jdbcTemplate) {
        super(jdbcTemplate);
    }

    @Override public String code()        { return "long-runner"; }
    @Override public String title()       { return "오래 사랑받은 영화"; }
    @Override public String description() { return "스크린당 상영 횟수가 많아 극장이 계속 틀어준 장기 흥행작"; }
    @Override public ChartCategory category() { return ChartCategory.TIME; }
    @Override public String icon()        { return "clock"; }

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
                    CAST(ROUND(m.box_office_show_cnt * 1.0 / m.box_office_scrn_cnt, 1)
                         AS VARCHAR) || '회/스크린'                    AS metric_value
                FROM movie m
                WHERE m.box_office_show_cnt IS NOT NULL
                  AND m.box_office_scrn_cnt IS NOT NULL
                  AND m.box_office_scrn_cnt > 0
                ORDER BY (m.box_office_show_cnt * 1.0 / m.box_office_scrn_cnt) DESC
                """,
                new Object[]{}, limit, "스크린당 상영 횟수",
                rs -> "장기 상영");
    }
}
