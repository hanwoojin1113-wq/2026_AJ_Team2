package movie.web.login.chart.algorithms;

import movie.web.login.chart.AbstractJdbcChartAlgorithm;
import movie.web.login.chart.ChartCategory;
import movie.web.login.chart.ChartMovieRow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * [차트 6] 특별관 선호 영화
 * 1인당 매출액(box_office_sales_acc / box_office_audi_acc) 내림차순.
 * 같은 관객 수라도 매출이 높다면 IMAX·4DX·스크린X 등 프리미엄 상영관 비중이 높다는 의미.
 * 관람 경험이 중요한 대형 스펙터클 영화가 상위권에 오르는 경향이 있다.
 */
@Component
public class PremiumChart extends AbstractJdbcChartAlgorithm {

    public PremiumChart(JdbcTemplate jdbcTemplate) {
        super(jdbcTemplate);
    }

    @Override public String code()        { return "premium"; }
    @Override public String title()       { return "특별관 선호 영화"; }
    @Override public String description() { return "1인당 매출이 높아 IMAX·4DX 관람이 많은 스펙터클 영화"; }
    @Override public ChartCategory category() { return ChartCategory.EFFICIENCY; }
    @Override public String icon()        { return "film"; }

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
                    CAST(ROUND(m.box_office_sales_acc * 1.0 / m.box_office_audi_acc)
                         AS VARCHAR) || '원/인'                        AS metric_value
                FROM movie m
                WHERE m.box_office_sales_acc IS NOT NULL
                  AND m.box_office_audi_acc IS NOT NULL
                  AND m.box_office_audi_acc > 0
                ORDER BY (m.box_office_sales_acc * 1.0 / m.box_office_audi_acc) DESC
                """,
                new Object[]{}, limit, "1인당 매출",
                rs -> "프리미엄");
    }
}
