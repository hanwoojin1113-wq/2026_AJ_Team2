package movie.web.login.tag;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MovieTagController {

    private final MovieTagService movieTagService;
    private final MovieTagKeywordService movieTagKeywordService;

    public MovieTagController(
            MovieTagService movieTagService,
            MovieTagKeywordService movieTagKeywordService
    ) {
        this.movieTagService = movieTagService;
        this.movieTagKeywordService = movieTagKeywordService;
    }

    @GetMapping("/tags/test")
    public String testTags() {
        return "Tag endpoint is ready. Send POST /tags/rebuild";
    }

    @PostMapping("/tags/rebuild")
    public MovieTagService.RebuildResult rebuildTags() {
        return movieTagService.rebuildTags();
    }

    @GetMapping("/tags/keyword-samples")
    public MovieTagKeywordService.KeywordSampleResult keywordSamples(
            @RequestParam(defaultValue = "MOOD") String tagType,
            @RequestParam String tagName,
            @RequestParam(defaultValue = "10") int limit
    ) {
        return movieTagKeywordService.sampleKeywords(tagType, tagName, limit);
    }

    @GetMapping("/tags/keyword-movies")
    public java.util.List<MovieTagKeywordService.MovieKeywordSample> keywordMovies(
            @RequestParam(defaultValue = "MOOD") String tagType,
            @RequestParam String tagName,
            @RequestParam(defaultValue = "10") int limit
    ) {
        return movieTagKeywordService.movieKeywords(tagType, tagName, limit);
    }
}
