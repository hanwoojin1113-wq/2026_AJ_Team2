package com.cinematch.tag;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class TagScoreCalculator {
    /*
     * 태그 1개에 대한 점수를 계산하는 계산기다.
     * 규칙에 정의된 positive/negative/hard-exclude 조건을 순서대로 적용해서
     * 0~100 score를 만들고, threshold 이상이면 태그를 부여한다.
     */

    public MovieTagResult calculate(MovieTagInput input, TagRule rule) {
        Set<String> genres = input.genres();
        Set<String> keywords = input.keywords();

        // required 조건은 "최소한 이 계열 신호가 있어야 함"을 의미한다.
        if (!rule.requiredGenresAny().isEmpty() && Collections.disjoint(genres, rule.requiredGenresAny())) {
            return emptyResult(rule.tag(), "required genre not matched");
        }
        if (!rule.requiredKeywordsAny().isEmpty() && Collections.disjoint(keywords, rule.requiredKeywordsAny())) {
            return emptyResult(rule.tag(), "required keyword not matched");
        }
        // hard exclude는 특정 장르/키워드가 보이면 즉시 태그 부여를 막는 장치다.
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

        // positive는 가산, negative는 감산한다.
        score += accumulateGenreWeights(genres, rule.positiveGenreWeights(), matchedGenres, reasons, "+genre");
        score -= accumulateGenreWeights(genres, rule.negativeGenreWeights(), null, reasons, "-genre");
        score += accumulateKeywordWeights(keywords, rule.positiveKeywordWeights(), positiveKeywordHits, reasons, "+keyword");
        score -= accumulateKeywordWeights(keywords, rule.negativeKeywordWeights(), negativeKeywordHits, reasons, "-keyword");

        // slow_burn, long_running 같은 태그는 runtime도 보조 feature로 반영한다.
        int runtimeScore = scoreRuntime(input.runtimeMinutes(), rule.runtimeWeights());
        if (runtimeScore > 0) {
            score += runtimeScore;
            reasons.add("runtime +" + runtimeScore);
        }

        int normalizedScore = Math.max(0, Math.min(100, score));
        // threshold는 태그별 난이도다. 좀비/크리피 같은 태그는 더 엄격하게 부여한다.
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
        // 실제로 맞은 장르만 점수에 반영하고, 디버깅을 위해 reason도 같이 남긴다.
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
        // 키워드는 태그의 분위기/테마를 세밀하게 구분하는 핵심 신호다.
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

        // 여러 runtime rule 중 가장 높은 가중치만 적용한다.
        int applied = 0;
        for (TagRule.RuntimeWeight runtimeWeight : runtimeWeights) {
            if (runtimeMinutes >= runtimeWeight.minimumMinutesInclusive()) {
                applied = Math.max(applied, runtimeWeight.weight());
            }
        }
        return applied;
    }
}
