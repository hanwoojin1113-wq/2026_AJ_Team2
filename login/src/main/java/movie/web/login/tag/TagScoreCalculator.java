package movie.web.login.tag;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class TagScoreCalculator {

    public MovieTagResult calculate(MovieTagInput input, TagRule rule) {
        Set<String> genres = input.genres();
        Set<String> keywords = input.keywords();

        if (!rule.requiredGenresAny().isEmpty() && Collections.disjoint(genres, rule.requiredGenresAny())) {
            return emptyResult(rule.tag(), "required genre not matched");
        }
        if (!rule.requiredKeywordsAny().isEmpty() && Collections.disjoint(keywords, rule.requiredKeywordsAny())) {
            return emptyResult(rule.tag(), "required keyword not matched");
        }
        if (!Collections.disjoint(genres, rule.hardExcludedGenres())) {
            return emptyResult(rule.tag(), "hard excluded genre matched");
        }
        if (!Collections.disjoint(keywords, rule.hardExcludedKeywords())) {
            return emptyResult(rule.tag(), "hard excluded keyword matched");
        }

        int score = 0;
        List<String> matchedGenres = new ArrayList<>();
        List<String> positiveKeywordHits = new ArrayList<>();
        List<String> negativeKeywordHits = new ArrayList<>();
        List<String> reasons = new ArrayList<>();

        score += accumulateGenreWeights(genres, rule.positiveGenreWeights(), matchedGenres, reasons, "+genre");
        score -= accumulateGenreWeights(genres, rule.negativeGenreWeights(), null, reasons, "-genre");
        score += accumulateKeywordWeights(keywords, rule.positiveKeywordWeights(), positiveKeywordHits, reasons, "+keyword");
        score -= accumulateKeywordWeights(keywords, rule.negativeKeywordWeights(), negativeKeywordHits, reasons, "-keyword");

        int runtimeScore = scoreRuntime(input.runtimeMinutes(), rule.runtimeWeights());
        if (runtimeScore > 0) {
            score += runtimeScore;
            reasons.add("runtime +" + runtimeScore);
        }

        int normalizedScore = Math.max(0, Math.min(100, score));
        boolean assigned = normalizedScore >= rule.tag().threshold();

        return new MovieTagResult(
                rule.tag(),
                normalizedScore,
                assigned,
                List.copyOf(matchedGenres),
                List.copyOf(positiveKeywordHits),
                List.copyOf(negativeKeywordHits),
                List.copyOf(reasons)
        );
    }

    private MovieTagResult emptyResult(RecommendationTag tag, String reason) {
        return new MovieTagResult(tag, 0, false, List.of(), List.of(), List.of(), List.of(reason));
    }

    private int accumulateGenreWeights(
            Set<String> genres,
            Map<String, Integer> weights,
            List<String> collectedGenres,
            List<String> collectedReasons,
            String prefix
    ) {
        int score = 0;
        for (Map.Entry<String, Integer> entry : weights.entrySet()) {
            if (!genres.contains(entry.getKey())) {
                continue;
            }
            score += entry.getValue();
            if (collectedGenres != null) {
                collectedGenres.add(entry.getKey());
            }
            collectedReasons.add(prefix + ":" + entry.getKey() + "=" + entry.getValue());
        }
        return score;
    }

    private int accumulateKeywordWeights(
            Set<String> keywords,
            Map<String, Integer> weights,
            List<String> collectedKeywords,
            List<String> collectedReasons,
            String prefix
    ) {
        int score = 0;
        for (Map.Entry<String, Integer> entry : weights.entrySet()) {
            if (!keywords.contains(entry.getKey())) {
                continue;
            }
            score += entry.getValue();
            collectedKeywords.add(entry.getKey());
            collectedReasons.add(prefix + ":" + entry.getKey() + "=" + entry.getValue());
        }
        return score;
    }

    private int scoreRuntime(Integer runtimeMinutes, List<TagRule.RuntimeWeight> runtimeWeights) {
        if (runtimeMinutes == null || runtimeWeights.isEmpty()) {
            return 0;
        }

        int applied = 0;
        for (TagRule.RuntimeWeight runtimeWeight : runtimeWeights) {
            if (runtimeMinutes >= runtimeWeight.minimumMinutesInclusive()) {
                applied = Math.max(applied, runtimeWeight.weight());
            }
        }
        return applied;
    }
}
