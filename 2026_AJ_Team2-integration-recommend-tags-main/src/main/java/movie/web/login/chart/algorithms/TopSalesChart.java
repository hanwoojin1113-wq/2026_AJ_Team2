package movie.web.login.chart.algorithms;

import movie.web.login.chart.AbstractJdbcChartAlgorithm;
import movie.web.login.chart.ChartCategory;
import movie.web.login.chart.ChartMovieRow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * [차트 1] 역대 매출 순위
 * box_office_sales_acc(누적 매출액) 내림차순.
 * 한국 역대 박스오피스 최고 흥행작을 매출 기준으로 나열한다.
 */
@Component
public class TopSalesChart extends AbstractJdbcChartAlgorithm {

    public TopSalesChart(JdbcTemplate jdbcTemplate) {
        super(jdbcTemplate);
    }

    @Override public String code()        { return "top-sales"; }
    @Override public String title()       { return "역대 매출 순위"; }
    @Override public String description() { return "누적 매출액 기준 역대 최고 흥행작 순위"; }
    @Override public ChartCategory category() { return ChartCategory.BOXOFFICE; }
    @Override public String icon()        { return "chart-bar"; }

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
                    CAST(m.box_office_sales_acc AS VARCHAR)            AS metric_value,
                    m.box_office_sales_acc                             AS raw_sales
                FROM movie m
                WHERE m.box_office_sales_acc IS NOT NULL
                ORDER BY m.box_office_sales_acc DESC
                """,
                new Object[]{}, limit, "누적 매출액",
                rs -> {
                    try {
                        long sales = rs.getLong("raw_sales");
                        return sales >= 100_000_000_000L ? "100억+" : null;
                    } catch (Exception e) { return null; }
                });
    }
}
