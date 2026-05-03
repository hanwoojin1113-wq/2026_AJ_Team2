package com.cinematch.chart.algorithms;

import com.cinematch.chart.ChartAlgorithm;
import com.cinematch.chart.ChartCategory;
import com.cinematch.chart.ChartMovieRow;
import com.cinematch.kobis.KobisBoxOfficeService;
import com.cinematch.tmdb.TmdbTrendingService;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.IntStream;

@Component
@Order(-100)
public class TmdbTrendingChart implements ChartAlgorithm {

    private static final String MOVIE_DETAIL_PREFIX = "/movies/";

    private final KobisBoxOfficeService kobisBoxOfficeService;

    public TmdbTrendingChart(KobisBoxOfficeService kobisBoxOfficeService) {
        this.kobisBoxOfficeService = kobisBoxOfficeService;
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
        return "KOBIS 일별 박스오피스 기준 상위 10개 작품";
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
        List<TmdbTrendingService.TrendingMovieView> movies = kobisBoxOfficeService.fetchBoxOffice(limit).stream()
                .filter(movie -> movie.posterImageUrl() != null && !movie.posterImageUrl().isBlank())
                .toList();

        return IntStream.range(0, movies.size())
                .mapToObj(index -> toChartMovieRow(index + 1, movies.get(index)))
                .toList();
    }

    private ChartMovieRow toChartMovieRow(int rankNo, TmdbTrendingService.TrendingMovieView movie) {
        Integer productionYear = parseProductionYear(movie.subtitle());
        return ChartMovieRow.of(
                rankNo,
                extractMovieCode(movie.detailUrl()),
                movie.title(),
                null,
                movie.posterImageUrl(),
                null,
                productionYear,
                "박스오피스 순위",
                "#" + rankNo
        );
    }

    private String extractMovieCode(String detailUrl) {
        if (detailUrl == null || !detailUrl.startsWith(MOVIE_DETAIL_PREFIX)) {
            return null;
        }
        return detailUrl.substring(MOVIE_DETAIL_PREFIX.length());
    }

    private Integer parseProductionYear(String subtitle) {
        if (subtitle == null || subtitle.isBlank()) {
            return null;
        }
        try {
            return Integer.parseInt(subtitle.trim());
        } catch (NumberFormatException ignored) {
            return null;
        }
    }
}
