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
    public String description() { return "박스오피스 기준 오늘의 실시간 인기 작품 Top 10"; }

    @Override
    public ChartCategory category() { return ChartCategory.TRENDING; }

    @Override
    public String icon() { return "trending-up"; }

    @Override
    public List<ChartMovieRow> fetch(int limit) {
        // /charts 홈과 동일하게 KOBIS 박스오피스를 우선 사용 (공연/콘서트 제외 + 백필 적용된 동일 소스)
        List<TmdbTrendingService.TrendingMovieView> kobisMovies = kobisBoxOfficeService.fetchBoxOffice(limit).stream()
                .filter(m -> m.posterImageUrl() != null && !m.posterImageUrl().isBlank())
                .toList();
        if (!kobisMovies.isEmpty()) {
            return IntStream.range(0, kobisMovies.size())
                    .mapToObj(i -> toKobisRow(i + 1, kobisMovies.get(i)))
                    .toList();
        }
        // KOBIS 데이터 없으면(예: API 키 미설정) TMDB trending fallback
        return tmdbTrendingService.fetchTrendingChartRows(limit);
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
