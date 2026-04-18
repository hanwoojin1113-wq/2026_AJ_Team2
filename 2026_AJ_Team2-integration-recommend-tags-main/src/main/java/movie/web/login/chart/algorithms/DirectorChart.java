package movie.web.login.chart.algorithms;

import movie.web.login.chart.AbstractJdbcChartAlgorithm;
import movie.web.login.chart.ChartCategory;
import movie.web.login.chart.ChartMovieRow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * [차트 12] 감독 흥행 보증 수표
 * 2편 이상의 흥행작(box_office_audi_acc 데이터가 있는 영화)을 가진 감독의 작품 중
 * 감독 총 누적 관객 순으로 정렬한다.
 * 같은 감독의 여러 작품이 나올 수 있으며, 각 작품 카드에는 해당 감독의 총 관객 수가 표시된다.
 * 흥행 감독의 필모그래피를 한눈에 볼 수 있는 차트다.
 */
@Component
public class DirectorChart extends AbstractJdbcChartAlgorithm {

    public DirectorChart(JdbcTemplate jdbcTemplate) {
        super(jdbcTemplate);
    }

    @Override public String code()        { return "director"; }
    @Override public String title()       { return "감독 흥행 보증 수표"; }
    @Override public String description() { return "2편 이상 흥행작을 만든 감독의 대표작 모음"; }
    @Override public ChartCategory category() { return ChartCategory.PEOPLE; }
    @Override public String icon()        { return "video"; }

    @Override
    public List<ChartMovieRow> fetch(int limit) {
        /*
         * person 테이블의 감독(movie_director 조인)을 기준으로
         * 흥행작 2편 이상인 감독만 필터(HAVING COUNT >= 2).
         * 각 감독별 총 관객 합계로 정렬하고, 감독의 모든 작품을 결과에 포함.
         * 결과 내 rank_no는 전체 결과 순서 기반.
         */
        return runQuery("""
                SELECT
                    m.movie_cd                                          AS movie_code,
                    COALESCE(m.title, m.movie_name)                    AS movie_name,
                    COALESCE(m.movie_name_en, m.original_title)        AS movie_name_en,
                    m.poster_image_url,
                    COALESCE(m.release_date, m.box_office_open_date)   AS open_date,
                    m.production_year,
                    p.name                                             AS director_name,
                    dir_totals.total_audi,
                    CAST(ROUND(m.box_office_audi_acc / 10000.0) AS VARCHAR)
                        || '만 명 (감독 합계 '
                        || CAST(ROUND(dir_totals.total_audi / 10000.0) AS VARCHAR)
                        || '만)'                                       AS metric_value
                FROM movie m
                JOIN movie_director md ON md.movie_id = m.id
                JOIN person p ON p.id = md.person_id
                JOIN (
                    SELECT
                        p2.id                    AS person_id,
                        SUM(m2.box_office_audi_acc) AS total_audi,
                        COUNT(*)                 AS hit_count
                    FROM person p2
                    JOIN movie_director md2 ON md2.person_id = p2.id
                    JOIN movie m2 ON m2.id = md2.movie_id
                    WHERE m2.box_office_audi_acc IS NOT NULL
                    GROUP BY p2.id
                    HAVING COUNT(*) >= 2
                ) dir_totals ON dir_totals.person_id = p.id
                WHERE m.box_office_audi_acc IS NOT NULL
                ORDER BY dir_totals.total_audi DESC, m.box_office_audi_acc DESC
                """,
                new Object[]{}, limit, "작품 관객",
                rs -> {
                    try {
                        return rs.getString("director_name") + " 감독";
                    } catch (Exception e) { return null; }
                });
    }
}
