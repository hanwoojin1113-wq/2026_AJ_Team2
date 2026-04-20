package movie.web.login.tag;

import movie.web.login.LoginApplication;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = LoginApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
class MovieTagEvaluationIT {

    @Autowired
    private MovieTagService movieTagService;

    @Autowired
    private MovieTagKeywordService movieTagKeywordService;

    @Test
    void evaluateCurrentTagCoverage() {
        MovieTagService.RebuildResult rebuild = movieTagService.rebuildTags();
        MovieTagService.TagStats stats = movieTagService.stats();

        System.out.println("=== TAG REBUILD ===");
        System.out.println(rebuild);
        System.out.println("=== TAG STATS ===");
        System.out.println(stats);
        System.out.println("=== KEYWORD SAMPLES ===");
        System.out.println(movieTagKeywordService.sampleKeywords("MOOD", "emotional", 5));
        System.out.println(movieTagKeywordService.sampleKeywords("MOOD", "dark", 5));
        System.out.println(movieTagKeywordService.sampleKeywords("THEME", "disaster", 5));
        System.out.println(movieTagKeywordService.sampleKeywords("THEME", "coming_of_age", 5));
        System.out.println(movieTagKeywordService.sampleKeywords("THEME", "friendship", 5));
        System.out.println(movieTagKeywordService.sampleKeywords("MOOD", "funny", 5));
        System.out.println(movieTagKeywordService.sampleKeywords("MOOD", "spectacle", 5));

        assertThat(stats.movieCount()).isGreaterThan(700);
        assertThat(stats.candidateCount()).isGreaterThan(700);
        assertThat(stats.taggedMovieCount()).isGreaterThan(200);
        assertThat(stats.movieTagRowCount()).isGreaterThan(300);
    }
}
