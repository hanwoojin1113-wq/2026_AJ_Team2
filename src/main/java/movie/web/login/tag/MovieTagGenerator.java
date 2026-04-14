package movie.web.login.tag;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

@Component
public class MovieTagGenerator {

    private static final Map<RecommendationTagType, Integer> TYPE_LIMITS = Map.of(
            RecommendationTagType.MOOD, 5,
            RecommendationTagType.CONTEXT, 2,
            RecommendationTagType.CAUTION, 2,
            RecommendationTagType.THEME, 4
    );

    private static final int OVERALL_LIMIT = 10;

    private final DefaultRecommendationTagRules defaultRecommendationTagRules;
    private final TagScoreCalculator tagScoreCalculator;

    public MovieTagGenerator(
            DefaultRecommendationTagRules defaultRecommendationTagRules,
            TagScoreCalculator tagScoreCalculator
    ) {
        this.defaultRecommendationTagRules = defaultRecommendationTagRules;
        this.tagScoreCalculator = tagScoreCalculator;
    }

    public List<MovieTagResult> scoreAll(MovieTagInput input) {
        List<MovieTagResult> results = new ArrayList<>();
        for (TagRule rule : defaultRecommendationTagRules.rules()) {
            results.add(tagScoreCalculator.calculate(input, rule));
        }
        results.sort(Comparator.comparingInt(MovieTagResult::score).reversed()
                .thenComparing(MovieTagResult::tagName));
        return results;
    }

    public List<MovieTagResult> generate(MovieTagInput input) {
        List<MovieTagResult> scored = scoreAll(input).stream()
                .filter(MovieTagResult::assigned)
                .toList();

        Map<RecommendationTagType, Integer> typeCounts = new EnumMap<>(RecommendationTagType.class);
        List<MovieTagResult> selected = new ArrayList<>();

        for (MovieTagResult result : scored) {
            if (selected.size() >= OVERALL_LIMIT) {
                break;
            }

            int used = typeCounts.getOrDefault(result.tagType(), 0);
            int limit = TYPE_LIMITS.getOrDefault(result.tagType(), 1);
            if (used >= limit) {
                continue;
            }

            selected.add(result);
            typeCounts.put(result.tagType(), used + 1);
        }

        return selected;
    }
}
