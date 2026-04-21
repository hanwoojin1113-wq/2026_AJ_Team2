package com.cinematch;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.annotation.Order;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
@Order(2)
@ConditionalOnProperty(value = "kobis.poster-fetch.enabled", havingValue = "true")
public class KobisPosterImageUpdater implements ApplicationRunner {

    private static final String DETAIL_URL_TEMPLATE =
            "https://www.kobis.or.kr/kobis/mobile/mast/mvie/searchMovieDtl.do?movieCd=%s";
    private static final String KOBIS_IMAGE_PATH_FRAGMENT = "/common/mast/movie/";

    private final JdbcTemplate jdbcTemplate;

    public KobisPosterImageUpdater(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void run(ApplicationArguments args) {
        List<MoviePosterTarget> movies = jdbcTemplate.query("""
                SELECT id, movie_cd
                FROM movie
                WHERE poster_image_url IS NULL
                ORDER BY ranking ASC, id ASC
                """, (rs, rowNum) -> new MoviePosterTarget(rs.getLong("id"), rs.getString("movie_cd")));

        for (MoviePosterTarget movie : movies) {
            try {
                String posterUrl = fetchPosterUrl(movie.movieCode());
                if (posterUrl != null) {
                    jdbcTemplate.update("UPDATE movie SET poster_image_url = ? WHERE id = ?", posterUrl, movie.id());
                }
            } catch (IOException | DataAccessException ex) {
                // KOBIS 이미지가 없는 작품도 있고, 네트워크 실패가 전체 기동을 막을 필요는 없다.
            }
        }
    }

    String fetchPosterUrl(String movieCode) throws IOException {
        Document document = Jsoup.connect(DETAIL_URL_TEMPLATE.formatted(movieCode))
                .timeout((int) Duration.ofSeconds(10).toMillis())
                .userAgent("Mozilla/5.0")
                .get();

        return extractPosterUrl(document);
    }

    String extractPosterUrl(Document document) {
        for (Element link : document.select("a[href]")) {
            String href = link.absUrl("href");
            if (href.contains(KOBIS_IMAGE_PATH_FRAGMENT)) {
                return href;
            }
        }
        return null;
    }

    private record MoviePosterTarget(long id, String movieCode) {
    }
}
