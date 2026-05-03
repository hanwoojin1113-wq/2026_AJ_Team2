package com.cinematch.chart.algorithms;

import com.cinematch.chart.AbstractJdbcChartAlgorithm;
import com.cinematch.chart.ChartCategory;
import com.cinematch.chart.ChartMovieRow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * [차트 16] 유저 추천 영화
 * 서비스 내 user_movie_like.liked = TRUE 집계 기준 내림차순.
 * 박스오피스나 TMDB가 아닌, 이 서비스 사용자들이 직접 좋아요를 누른 영화를 보여준다.
 * 데이터가 쌓일수록 신뢰도가 높아지는 커뮤니티 기반 차트.
 */
@Component
public class UserLikedChart extends AbstractJdbcChartAlgorithm {

    public UserLikedChart(JdbcTemplate jdbcTemplate) {
        super(jdbcTemplate);
    }

    @Override public String code()        { return "most-liked"; }
    @Override public String title()       { return "유저 추천 영화"; }
    @Override public String description() { return "CineMatch 사용자들이 가장 많이 좋아요를 누른 영화"; }
    @Override public ChartCategory category() { return ChartCategory.COMMUNITY; }
    @Override public String icon()        { return "heart"; }

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
                    CONCAT(CAST(likes.like_count AS VARCHAR), '명')   AS metric_value
                FROM movie m
                JOIN (
                    SELECT movie_id, COUNT(*) AS like_count
                    FROM user_movie_like
                    WHERE liked = TRUE
                    GROUP BY movie_id
                    HAVING COUNT(*) >= 1
                ) likes ON likes.movie_id = m.id
                ORDER BY likes.like_count DESC, COALESCE(m.title, m.movie_name) ASC
                """,
                new Object[]{}, limit, "좋아요",
                null);
    }
}
