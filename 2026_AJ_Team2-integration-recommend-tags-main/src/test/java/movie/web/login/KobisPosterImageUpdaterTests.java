package movie.web.login;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

class KobisPosterImageUpdaterTests {

    private final KobisPosterImageUpdater updater = new KobisPosterImageUpdater((JdbcTemplate) null);

    @Test
    void extractsPosterUrlFromKobisMovieDetailHtml() {
        Document document = Jsoup.parse("""
                <html>
                <body>
                    <h3>명량</h3>
                    <a href="https://www.kobis.or.kr/common/mast/movie/2014/07/7e00a486e94f4428b8b2e54313b5c510.jpg"></a>
                    <ul>
                        <li>스틸컷</li>
                        <li><a href="https://www.kobis.or.kr/common/mast/movie/2014/07/still_01.jpg">still</a></li>
                    </ul>
                </body>
                </html>
                """, "https://www.kobis.or.kr/kobis/mobile/mast/mvie/searchMovieDtl.do?movieCd=20129370");

        String posterUrl = updater.extractPosterUrl(document);

        assertThat(posterUrl)
                .isEqualTo("https://www.kobis.or.kr/common/mast/movie/2014/07/7e00a486e94f4428b8b2e54313b5c510.jpg");
    }

    @Test
    void returnsNullWhenPosterDoesNotExist() {
        Document document = Jsoup.parse("""
                <html>
                <body>
                    <h3>이미지 없음</h3>
                    <div>스틸컷 해당정보없음</div>
                </body>
                </html>
                """, "https://www.kobis.or.kr/kobis/mobile/mast/mvie/searchMovieDtl.do?movieCd=20210327");

        assertThat(updater.extractPosterUrl(document)).isNull();
    }
}
