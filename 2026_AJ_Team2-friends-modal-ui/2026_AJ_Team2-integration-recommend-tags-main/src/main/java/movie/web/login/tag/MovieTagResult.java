package movie.web.login.tag;

import java.util.List;

public record MovieTagResult(
        RecommendationTag tag,
        int score,
        boolean assigned,
        List<String> matchedGenres,
        List<String> positiveKeywordHits,
        List<String> negativeKeywordHits,
        List<String> reasons
) {
    public String tagName() {
        return tag.code();
    }

    public RecommendationTagType tagType() {
        return tag.type();
    }

    public double confidence() {
        return score / 100.0;
    }
}
