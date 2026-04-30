package com.cinematch.chart.algorithms;

import com.cinematch.chart.ChartAlgorithm;
import com.cinematch.chart.ChartCategory;
import com.cinematch.chart.ChartMovieRow;
import com.cinematch.tmdb.TmdbTrendingService;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Order(-100)
public class TmdbTrendingChart implements ChartAlgorithm {

    private final TmdbTrendingService tmdbTrendingService;

    public TmdbTrendingChart(TmdbTrendingService tmdbTrendingService) {
        this.tmdbTrendingService = tmdbTrendingService;
    }

    @Override
    public String code() {
        return "tmdb-trending";
    }

    @Override
    public String title() {
        return "실시간 인기 작품";
    }

    @Override
    public String description() {
        return "TMDB trending movie/day 기준 상위 10개 작품";
    }

    @Override
    public ChartCategory category() {
        return ChartCategory.TRENDING;
    }

    @Override
    public String icon() {
        return "trending-up";
    }

    @Override
    public List<ChartMovieRow> fetch(int limit) {
        return tmdbTrendingService.fetchTrendingChartRows(limit);
    }
}
