package com.cinematch.chart.algorithms;

import com.cinematch.chart.AbstractJdbcChartAlgorithm;
import com.cinematch.chart.ChartCategory;
import com.cinematch.chart.ChartMovieRow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * [차트 15] 주연 배우 대표작
 * display_order <= 3 기준 주연 배우가 2편 이상 흥행작에 출연한 경우만 포함.
 * 배우 누적 관객 합계 순으로 정렬하여 믿고 보는 배우의 대표 출연작을 보여준다.
 * DirectorChart와 동일한 패턴을 사용한다.
 */
@Component
public class ActorChart extends AbstractJdbcChartAlgorithm {

    public ActorChart(JdbcTemplate jdbcTemplate) {
        super(jdbcTemplate);
    }

    @Override public String code()        { return "actor"; }
    @Override public String title()       { return "주연 배우 대표작"; }
    @Override public String description() { return "2편 이상 흥행작에 주연한 배우의 대표 출연작 모음"; }
    @Override public ChartCategory category() { return ChartCategory.PEOPLE; }
    @Override public String icon()        { return "user"; }

    @Override
    public List<ChartMovieRow> fetch(int limit) {
        return runQuery("""
                SELECT movie_code, movie_name, movie_name_en, poster_image_url,
                       open_date, production_year, actor_name, total_audi, metric_value
                FROM (
                    SELECT
                        m.movie_cd                                          AS movie_code,
                        COALESCE(m.title, m.movie_name)                    AS movie_name,
                        COALESCE(m.movie_name_en, m.original_title)        AS movie_name_en,
                        m.poster_image_url,
                        COALESCE(m.release_date, m.box_office_open_date)   AS open_date,
                        m.production_year,
                        p.name                                             AS actor_name,
                        act_totals.total_audi,
                        CONCAT(
                            CAST(ROUND(m.box_office_audi_acc / 10000.0) AS VARCHAR),
                            '만 명 (배우 합계 ',
                            CAST(ROUND(act_totals.total_audi / 10000.0) AS VARCHAR),
                            '만)'
                        )                                                  AS metric_value,
                        ROW_NUMBER() OVER (
                            PARTITION BY m.id
                            ORDER BY act_totals.total_audi DESC
                        ) AS movie_rn
                    FROM movie m
                    JOIN movie_actor ma ON ma.movie_id = m.id
                    JOIN person p ON p.id = ma.person_id
                    JOIN (
                        SELECT
                            p2.id                       AS person_id,
                            SUM(m2.box_office_audi_acc) AS total_audi,
                            COUNT(*)                    AS hit_count
                        FROM person p2
                        JOIN movie_actor ma2 ON ma2.person_id = p2.id
                        JOIN movie m2 ON m2.id = ma2.movie_id
                        WHERE m2.box_office_audi_acc IS NOT NULL
                          AND ma2.display_order <= 3
                        GROUP BY p2.id
                        HAVING COUNT(*) >= 2
                    ) act_totals ON act_totals.person_id = p.id
                    WHERE m.box_office_audi_acc IS NOT NULL
                      AND ma.display_order <= 3
                ) deduped
                WHERE movie_rn = 1
                ORDER BY total_audi DESC, movie_code
                """,
                new Object[]{}, limit, "출연작 관객",
                rs -> {
                    try {
                        return rs.getString("actor_name");
                    } catch (Exception e) { return null; }
                });
    }
}
