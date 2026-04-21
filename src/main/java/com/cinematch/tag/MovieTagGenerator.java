package com.cinematch.tag;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

@Component
public class MovieTagGenerator {
    /*
     * 태그 생성기의 역할은 "모든 규칙을 점수화한 뒤, 실제로 부여할 태그만 추리는 것"이다.
     * 즉,
     * 1) 각 규칙별 score 계산
     * 2) threshold를 넘은 태그만 남김
     * 3) 타입별 개수 제한 + 전체 개수 제한 적용
     * 순서로 동작한다.
     */

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
        // 영화 1편에 대해 모든 규칙을 평가해 태그별 점수 후보를 만든다.
        List<MovieTagResult> results = new ArrayList<>();
        for (TagRule rule : defaultRecommendationTagRules.rules()) {
            results.add(tagScoreCalculator.calculate(input, rule));
        }
        results.sort(Comparator.comparingInt(MovieTagResult::score).reversed()
                .thenComparing(MovieTagResult::tagName));
        return results;
    }

    public List<MovieTagResult> generate(MovieTagInput input) {
        // threshold를 넘은 태그만 실제 부여 후보로 사용한다.
        List<MovieTagResult> scored = scoreAll(input).stream()
                .filter(MovieTagResult::assigned)
                .toList();

        Map<RecommendationTagType, Integer> typeCounts = new EnumMap<>(RecommendationTagType.class);
        List<MovieTagResult> selected = new ArrayList<>();

        for (MovieTagResult result : scored) {
            if (selected.size() >= OVERALL_LIMIT) {
                break;
            }

            // 특정 타입 태그가 너무 많아지면 추천 설명성이 떨어져서 타입별 limit를 둔다.
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
