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

    private final TmdbTrendingService tmdbTrendingService;
    private final KobisBoxOfficeService kobisBoxOfficeService;

    public TmdbTrendingChart(TmdbTrendingService tmdbTrendingService, KobisBoxOfficeService kobisBoxOfficeService) {
        this.tmdbTrendingService = tmdbTrendingService;
        this.kobisBoxOfficeService = kobisBoxOfficeService;
    }

    @Override
    public String code() { return "tmdb-trending"; }

    @Override
    public String title() { return "실시간 인기 작품"; }

    @Override
    public String description() { return "TMDB 기준 오늘의 실시간 인기 작품 Top 10"; }

    @Override
    public ChartCategory category() { return ChartCategory.TRENDING; }

    @Override
    public String icon() { return "trending-up"; }

    @Override
    public List<ChartMovieRow> fetch(int limit) {
        // TMDB trending 스냅샷 우선 사용
        List<ChartMovieRow> rows = tmdbTrendingService.fetchTrendingChartRows(limit);
        if (!rows.isEmpty()) {
            return rows;
        }
        // TMDB 데이터 없으면 KOBIS 박스오피스 fallback
        List<TmdbTrendingService.TrendingMovieView> kobisMovies = kobisBoxOfficeService.fetchBoxOffice(limit).stream()
                .filter(m -> m.posterImageUrl() != null && !m.posterImageUrl().isBlank())
                .toList();
        return IntStream.range(0, kobisMovies.size())
                .mapToObj(i -> toKobisRow(i + 1, kobisMovies.get(i)))
                .toList();
    }

    private ChartMovieRow toKobisRow(int rankNo, TmdbTrendingService.TrendingMovieView movie) {
        return ChartMovieRow.of(
                rankNo,
                extractMovieCode(movie.detailUrl()),
                movie.title(),
                null,
                movie.posterImageUrl(),
                null,
                parseYear(movie.subtitle()),
                "박스오피스 순위",
                "#" + rankNo
        );
    }

    private String extractMovieCode(String detailUrl) {
        if (detailUrl == null || !detailUrl.startsWith(MOVIE_DETAIL_PREFIX)) return null;
        return detailUrl.substring(MOVIE_DETAIL_PREFIX.length());
    }

    private Integer parseYear(String subtitle) {
        if (subtitle == null || subtitle.isBlank()) return null;
        try {
            return Integer.parseInt(subtitle.trim());
        } catch (NumberFormatException ignored) {
            return null;
        }
    }
}
