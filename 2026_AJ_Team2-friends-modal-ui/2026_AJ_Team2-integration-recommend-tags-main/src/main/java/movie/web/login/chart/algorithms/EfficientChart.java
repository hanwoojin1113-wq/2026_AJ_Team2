package movie.web.login.chart.algorithms;

import movie.web.login.chart.AbstractJdbcChartAlgorithm;
import movie.web.login.chart.ChartCategory;
import movie.web.login.chart.ChartMovieRow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * [차트 3] 가성비 영화
 * 스크린 수 대비 관객 수(box_office_audi_acc / box_office_scrn_cnt) 내림차순.
 * 스크린이 많지 않아도 관객을 많이 끌어모은 효율적인 흥행작을 보여준다.
 * 독립·소규모 배급 영화의 저력을 발견하는 데 유용하다.
 */
@Component
public class EfficientChart extends AbstractJdbcChartAlgorithm {

    public EfficientChart(JdbcTemplate jdbcTemplate) {
        super(jdbcTemplate);
    }

    @Override public String code()        { return "efficient"; }
    @Override public String title()       { return "가성비 영화"; }
    @Override public String description() { return "스크린 수 대비 관객이 가장 많은 고효율 흥행작"; }
    @Override public ChartCategory category() { return ChartCategory.EFFICIENCY; }
    @Override public String icon()        { return "trending-up"; }

    @Override
    public List<ChartMovieRow> fetch(int limit) {
        return runQuery("""
                SELECT
                    m.movie_cd                                                          AS movie_code,
                    COALESCE(m.title, m.movie_name)                                    AS movie_name,
                    COALESCE(m.movie_name_en, m.original_title)                        AS movie_name_en,
                    m.poster_image_url,
                    COALESCE(m.release_date, m.box_office_open_date)                   AS open_date,
                    m.production_year,
                    CAST(ROUND(m.box_office_audi_acc * 1.0 / m.box_office_scrn_cnt)
                         AS VARCHAR) || '명/스크린'                                   AS metric_value
                FROM movie m
                WHERE m.box_office_audi_acc IS NOT NULL
                  AND m.box_office_scrn_cnt IS NOT NULL
                  AND m.box_office_scrn_cnt > 0
                ORDER BY (m.box_office_audi_acc * 1.0 / m.box_office_scrn_cnt) DESC
                """,
                new Object[]{}, limit, "스크린당 관객", null);
    }
}
