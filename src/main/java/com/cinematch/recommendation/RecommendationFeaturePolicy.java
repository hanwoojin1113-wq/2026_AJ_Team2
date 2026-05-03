package com.cinematch.recommendation;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

@Component
public class RecommendationFeaturePolicy {

    public static final String ALGORITHM_VERSION = "content-ranking-v3.3";

    private static final int ACTOR_LIMIT = 3;
    private static final int KEYWORD_LIMIT = 8;
    private static final int DEFAULT_PROFILE_FEATURE_LIMIT = 12;
    private static final int RANKING_BLOCK_SLICE_LIMIT = 120;
    private static final int RANKING_BLOCK_ITEM_LIMIT = 10;
    private static final int BLOCK_MIN_SIZE = 4;

    private static final int DIRECTOR_BLOCK_MIN_SOURCE_MOVIES = 2;
    private static final int ACTOR_BLOCK_MIN_SOURCE_MOVIES = 3;
    private static final double DIRECTOR_BLOCK_MIN_SIGNAL_WEIGHT = 4.5;
    private static final double ACTOR_BLOCK_MIN_SIGNAL_WEIGHT = 7.0;

    private static final int GENRE_DOMINANCE_UNLOCK_MIN_COUNT = 4;
    private static final double GENRE_DOMINANCE_UNLOCK_MIN_WEIGHT = 8.5;
    private static final double PRE_UNLOCK_GENRE_MULTIPLIER = 0.40;
    private static final double POST_UNLOCK_GENRE_MULTIPLIER = 0.80;
    private static final double GENRE_SATURATION_CONSTANT = 3.0;
    private static final double TAG_SATURATION_CONSTANT = 4.0;
    private static final double ANIMATION_EXTRA_MULTIPLIER_PRE_UNLOCK = 0.70;
    private static final double ANIMATION_EXTRA_MULTIPLIER_POST_UNLOCK = 0.90;

    private static final int PERSONALIZED_POOL_SIZE = 60;
    private static final int THEMATIC_OVERLAP_CAP = 2;
    private static final int PERSONALIZED_GENRE_CAP = 3;
    private static final int PERSONALIZED_TAG_CAP = 2;
    private static final int PERSONALIZED_DIRECTOR_CAP = 2;
    private static final int PERSONALIZED_REASON_CAP = 2;
    private static final double GENRE_REPEAT_PENALTY_WEIGHT = 0.030;
    private static final double TAG_REPEAT_PENALTY_WEIGHT = 0.036;
    private static final double DIRECTOR_REPEAT_PENALTY_WEIGHT = 0.032;
    private static final double REASON_REPEAT_PENALTY_WEIGHT = 0.024;
    private static final double THEMATIC_OVERLAP_PENALTY_WEIGHT = 0.060;
    private static final double MILD_DIVERSITY_BONUS_WEIGHT = 0.010;
    private static final double TRENDING_BONUS_MAX = 0.08;
    private static final double TRENDING_BONUS_MIN = 0.02;

    private static final double LIFE_SIGNAL_WEIGHT = 5.0;
    private static final double LIKE_SIGNAL_WEIGHT = 3.0;
    private static final double STORE_SIGNAL_WEIGHT = 1.5;

    private static final double UNRATED_WATCHED_SIGNAL = 0.4;
    private static final Map<Integer, Double> WATCHED_RATING_SIGNALS = Map.of(
            1, -1.2,
            2, -0.6,
            3, 0.0,
            4, 1.0,
            5, 1.4
    );

    private static final Map<String, Double> NEGATIVE_FEATURE_SCALES = Map.of(
            "TAG", 1.00,
            "GENRE", 1.00,
            "KEYWORD", 0.60,
            "DIRECTOR", 0.35,
            "ACTOR", 0.25,
            "PROVIDER", 0.00,
            "CAUTION", 0.00
    );

    private static final String PROVIDER_REGION_CODE = "KR";
    private static final String PROVIDER_TYPE = "FLATRATE";

    private static final Set<String> KEYWORD_BLACKLIST = Set.of(
            "sequel",
            "duringcreditsstinger",
            "aftercreditsstinger",
            "post credits scene",
            "based on novel or book",
            "based on young adult novel",
            "spin off",
            "spin-off"
    );

    private static final Map<String, Double> BROAD_GENRE_WEIGHTS = Map.of(
            "모험", 0.65,
            "드라마", 0.75,
            "액션", 0.82,
            "코미디", 0.85
    );

    private static final Map<String, Integer> PROFILE_FEATURE_LIMITS = Map.of(
            "TAG", 8,
            "CAUTION", 4,
            "GENRE", 8,
            "DIRECTOR", 6,
            "ACTOR", 8,
            "KEYWORD", 16,
            "PROVIDER", 4
    );

    private static final Map<String, Double> PROFILE_MULTIPLIERS = Map.of(
            "TAG", 1.00,
            "CAUTION", 0.60,
            "GENRE", 0.72,
            "DIRECTOR", 0.60,
            "ACTOR", 0.34,
            "KEYWORD", 0.18,
            "PROVIDER", 0.08
    );

    private static final Map<String, Double> RANKING_WEIGHTS = Map.of(
            "TAG", 0.34,
            "GENRE", 0.18,
            "KEYWORD", 0.08,
            "PEOPLE", 0.16,
            "PROVIDER", 0.02,
            "POPULARITY", 0.10,
            "FRESHNESS", 0.04
    );

    private static final Map<String, Integer> IDEAL_MATCH_COUNTS = Map.of(
            "TAG", 2,
            "CAUTION", 1,
            "GENRE", 2,
            "DIRECTOR", 1,
            "ACTOR", 1,
            "KEYWORD", 2,
            "PROVIDER", 1
    );

    private static final Set<String> ANIMATION_GENRE_NAMES = Set.of("애니메이션", "animation");
    private static final Set<String> PERFORMANCE_GENRE_NAMES = Set.of(
            "공연",
            "공연실황",
            "콘서트",
            "라이브",
            "concert",
            "live"
    );

    private static final List<String> PERFORMANCE_KEYWORD_PATTERNS = List.of(
            "concert",
            "live",
            "performance",
            "stage",
            "tour",
            "music documentary",
            "공연실황",
            "라이브",
            "콘서트",
            "투어",
            "무대"
    );

    private static final List<String> PERFORMANCE_TITLE_PATTERNS = List.of(
            "concert",
            "live in",
            "live at",
            "world tour",
            "concert film",
            "공연실황",
            "콘서트",
            "라이브",
            "월드투어"
    );

    private static final Map<String, String> TAG_LABELS = Map.ofEntries(
            Map.entry("funny", "유쾌한"),
            Map.entry("tense", "긴장감 있는"),
            Map.entry("dark", "어두운"),
            Map.entry("emotional", "감성적인"),
            Map.entry("romantic", "로맨틱한"),
            Map.entry("hopeful", "희망적인"),
            Map.entry("healing", "힐링"),
            Map.entry("spectacle", "볼거리 있는"),
            Map.entry("creepy", "으스스한"),
            Map.entry("with_family", "가족과 보기 좋은"),
            Map.entry("with_partner", "연인과 보기 좋은"),
            Map.entry("late_night", "늦은 밤에 어울리는"),
            Map.entry("violent", "폭력적인"),
            Map.entry("sad", "슬픈"),
            Map.entry("slow_burn", "느리게 쌓이는"),
            Map.entry("long_running", "긴 호흡의"),
            Map.entry("investigation", "수사"),
            Map.entry("mystery", "미스터리"),
            Map.entry("zombie", "좀비"),
            Map.entry("disaster", "재난"),
            Map.entry("true_story", "실화"),
            Map.entry("coming_of_age", "성장"),
            Map.entry("friendship", "우정"),
            Map.entry("survival", "생존"),
            Map.entry("revenge", "복수")
    );

    public int actorLimit() {
        return ACTOR_LIMIT;
    }

    public int keywordLimit() {
        return KEYWORD_LIMIT;
    }

    public String preferredProviderRegionCode() {
        return PROVIDER_REGION_CODE;
    }

    public String preferredProviderType() {
        return PROVIDER_TYPE;
    }

    public boolean isKeywordAllowed(String keyword) {
        if (keyword == null || keyword.isBlank()) {
            return false;
        }
        return !KEYWORD_BLACKLIST.contains(normalizeKeyword(keyword));
    }

    public double profileMultiplier(String featureType) {
        return PROFILE_MULTIPLIERS.getOrDefault(featureType, 0.0);
    }

    public double rankingWeight(String featureType) {
        return RANKING_WEIGHTS.getOrDefault(featureType, 0.0);
    }

    public int profileFeatureLimit(String featureType) {
        return PROFILE_FEATURE_LIMITS.getOrDefault(featureType, DEFAULT_PROFILE_FEATURE_LIMIT);
    }

    public double lifeSignalWeight() {
        return LIFE_SIGNAL_WEIGHT;
    }

    public double likeSignalWeight() {
        return LIKE_SIGNAL_WEIGHT;
    }

    public double storeSignalWeight() {
        return STORE_SIGNAL_WEIGHT;
    }

    public double watchedSignalWeight(Integer rating) {
        if (rating == null) {
            return UNRATED_WATCHED_SIGNAL;
        }
        return WATCHED_RATING_SIGNALS.getOrDefault(rating, 0.0);
    }

    public double negativeFeatureScale(String featureType) {
        return NEGATIVE_FEATURE_SCALES.getOrDefault(featureType, 0.0);
    }

    public int idealMatchCount(String featureType) {
        return IDEAL_MATCH_COUNTS.getOrDefault(featureType, 1);
    }

    public double cautionPenaltyWeight() {
        return 0.08;
    }

    public double multiSignalBonus(int matchedSignalCount) {
        return Math.min(0.05, matchedSignalCount * 0.018);
    }

    public double evidenceBase(String featureType) {
        return switch (featureType) {
            case "TAG" -> 0.48;
            case "GENRE" -> 0.58;
            case "KEYWORD" -> 0.60;
            case "PROVIDER" -> 0.70;
            default -> 0.68;
        };
    }

    public double densityBase(String featureType) {
        return switch (featureType) {
            case "TAG" -> 0.64;
            case "GENRE" -> 0.72;
            default -> 0.78;
        };
    }

    public double featureSpecificWeight(String featureType, String featureName) {
        if ("GENRE".equals(featureType)) {
            return broadGenreWeight(featureName);
        }
        return 1.0;
    }

    public boolean isBroadGenre(String genreName) {
        return BROAD_GENRE_WEIGHTS.containsKey(genreName);
    }

    public double broadGenreWeight(String genreName) {
        return BROAD_GENRE_WEIGHTS.getOrDefault(genreName, 1.0);
    }

    public int blockSliceLimit() {
        return RANKING_BLOCK_SLICE_LIMIT;
    }

    public int blockItemLimit() {
        return RANKING_BLOCK_ITEM_LIMIT;
    }

    public int blockMinimumSize() {
        return BLOCK_MIN_SIZE;
    }

    public int minimumDirectorBlockSourceMovies() {
        return DIRECTOR_BLOCK_MIN_SOURCE_MOVIES;
    }

    public int minimumActorBlockSourceMovies() {
        return ACTOR_BLOCK_MIN_SOURCE_MOVIES;
    }

    public double minimumDirectorBlockSignalWeight() {
        return DIRECTOR_BLOCK_MIN_SIGNAL_WEIGHT;
    }

    public double minimumActorBlockSignalWeight() {
        return ACTOR_BLOCK_MIN_SIGNAL_WEIGHT;
    }

    public boolean isGenreBlockAllowed(String genreName, int sampleCount) {
        if (!isBroadGenre(genreName)) {
            return sampleCount >= blockMinimumSize();
        }
        return sampleCount >= blockMinimumSize() + 2;
    }

    public int genreDominanceUnlockMinCount() {
        return GENRE_DOMINANCE_UNLOCK_MIN_COUNT;
    }

    public double genreDominanceUnlockMinWeight() {
        return GENRE_DOMINANCE_UNLOCK_MIN_WEIGHT;
    }

    public double preUnlockGenreMultiplier() {
        return PRE_UNLOCK_GENRE_MULTIPLIER;
    }

    public double postUnlockGenreMultiplier() {
        return POST_UNLOCK_GENRE_MULTIPLIER;
    }

    public double genreSaturationConstant() {
        return GENRE_SATURATION_CONSTANT;
    }

    public double tagSaturationConstant() {
        return TAG_SATURATION_CONSTANT;
    }

    public double animationExtraMultiplierPreUnlock() {
        return ANIMATION_EXTRA_MULTIPLIER_PRE_UNLOCK;
    }

    public double animationExtraMultiplierPostUnlock() {
        return ANIMATION_EXTRA_MULTIPLIER_POST_UNLOCK;
    }

    public int personalizedPoolSize() {
        return PERSONALIZED_POOL_SIZE;
    }

    public int thematicOverlapCap() {
        return THEMATIC_OVERLAP_CAP;
    }

    public int personalizedGenreCap() {
        return PERSONALIZED_GENRE_CAP;
    }

    public int personalizedTagCap() {
        return PERSONALIZED_TAG_CAP;
    }

    public int personalizedDirectorCap() {
        return PERSONALIZED_DIRECTOR_CAP;
    }

    public int personalizedReasonCap() {
        return PERSONALIZED_REASON_CAP;
    }

    public double genreRepeatPenaltyWeight() {
        return GENRE_REPEAT_PENALTY_WEIGHT;
    }

    public double tagRepeatPenaltyWeight() {
        return TAG_REPEAT_PENALTY_WEIGHT;
    }

    public double directorRepeatPenaltyWeight() {
        return DIRECTOR_REPEAT_PENALTY_WEIGHT;
    }

    public double reasonRepeatPenaltyWeight() {
        return REASON_REPEAT_PENALTY_WEIGHT;
    }

    public double thematicOverlapPenaltyWeight() {
        return THEMATIC_OVERLAP_PENALTY_WEIGHT;
    }

    public double mildDiversityBonusWeight() {
        return MILD_DIVERSITY_BONUS_WEIGHT;
    }

    public double trendingBonusMax() {
        return TRENDING_BONUS_MAX;
    }

    public double trendingBonusMin() {
        return TRENDING_BONUS_MIN;
    }

    public double trendingBonusForRank(Integer rankNo) {
        if (rankNo == null || rankNo < 1 || rankNo > 10) {
            return 0.0;
        }
        return TRENDING_BONUS_MIN
                + ((TRENDING_BONUS_MAX - TRENDING_BONUS_MIN) * ((10 - rankNo) / 9.0));
    }

    public boolean isGenreDominanceUnlocked(int movieCount, double signalWeightSum) {
        return movieCount >= GENRE_DOMINANCE_UNLOCK_MIN_COUNT
                && signalWeightSum >= GENRE_DOMINANCE_UNLOCK_MIN_WEIGHT;
    }

    public double genreDominanceMultiplier(int movieCount, double signalWeightSum) {
        return isGenreDominanceUnlocked(movieCount, signalWeightSum)
                ? POST_UNLOCK_GENRE_MULTIPLIER
                : PRE_UNLOCK_GENRE_MULTIPLIER;
    }

    public double animationExtraGenreMultiplier(int movieCount, double signalWeightSum) {
        return isGenreDominanceUnlocked(movieCount, signalWeightSum)
                ? ANIMATION_EXTRA_MULTIPLIER_POST_UNLOCK
                : ANIMATION_EXTRA_MULTIPLIER_PRE_UNLOCK;
    }

    public double applyGenreDominanceControl(
            String genreName,
            double rawScore,
            int positiveMovieCount,
            double positiveSignalWeight
    ) {
        if (rawScore == 0.0) {
            return 0.0;
        }

        double adjustedScore = saturateGenreScore(Math.abs(rawScore))
                * genreDominanceMultiplier(positiveMovieCount, positiveSignalWeight);

        if (isAnimationGenreName(genreName)) {
            adjustedScore *= animationExtraGenreMultiplier(positiveMovieCount, positiveSignalWeight);
        }
        return Math.signum(rawScore) * adjustedScore;
    }

    public double saturateGenreScore(double rawScore) {
        return saturate(rawScore, GENRE_SATURATION_CONSTANT);
    }

    public double saturateTagScore(double rawScore) {
        return saturate(rawScore, TAG_SATURATION_CONSTANT);
    }

    public double applyMatchDominanceDamping(
            String featureType,
            String primaryFeatureName,
            double normalizedScore,
            double topShare,
            int matchedFeatureCount
    ) {
        if (normalizedScore <= 0.0) {
            return 0.0;
        }
        if (matchedFeatureCount <= 0) {
            return normalizedScore;
        }

        double damping = 1.0;
        if ("GENRE".equals(featureType)) {
            damping = matchedFeatureCount == 1
                    ? 0.62
                    : Math.min(1.0, 0.70 + ((1.0 - topShare) * 0.42));
            if (isAnimationGenreName(primaryFeatureName)) {
                damping *= matchedFeatureCount == 1 ? 0.82 : 0.92;
            }
        } else if ("TAG".equals(featureType)) {
            damping = matchedFeatureCount == 1
                    ? 0.78
                    : Math.min(1.0, 0.82 + ((1.0 - topShare) * 0.35));
        }
        return normalizedScore * damping;
    }

    public boolean isAnimationMovie(Set<String> genres) {
        if (genres == null || genres.isEmpty()) {
            return false;
        }
        Set<String> normalizedGenres = genres.stream()
                .filter(name -> name != null && !name.isBlank())
                .map(this::normalizeRuleText)
                .collect(Collectors.toSet());
        return ANIMATION_GENRE_NAMES.stream()
                .map(this::normalizeRuleText)
                .anyMatch(normalizedGenres::contains);
    }

    public boolean isAnimationGenreName(String genreName) {
        if (genreName == null || genreName.isBlank()) {
            return false;
        }
        String normalizedGenre = normalizeRuleText(genreName);
        return ANIMATION_GENRE_NAMES.stream()
                .map(this::normalizeRuleText)
                .anyMatch(normalizedGenre::equals);
    }

    public boolean isRecommendationEligible(
            Set<String> genres,
            Set<String> keywords,
            String title,
            String originalTitle
    ) {
        return !isPerformanceContent(genres, keywords, title, originalTitle);
    }

    public boolean isPerformanceContent(
            Set<String> genres,
            Set<String> keywords,
            String title,
            String originalTitle
    ) {
        if (matchesPerformanceGenre(genres)) {
            return true;
        }

        int keywordPatternHits = countPatternHits(keywords, PERFORMANCE_KEYWORD_PATTERNS);
        if (keywordPatternHits >= 2 || containsStrongPerformanceKeyword(keywords)) {
            return true;
        }

        return matchesPerformanceTitle(title) || matchesPerformanceTitle(originalTitle);
    }

    public String tagDisplayName(String tagCode) {
        return TAG_LABELS.getOrDefault(tagCode, tagCode.replace('_', ' '));
    }

    public String tagBlockTitle(String tagCode) {
        return switch (tagCode) {
            case "tense" -> "긴장감 있는 영화 추천";
            case "mystery" -> "미스터리 영화 추천";
            case "healing" -> "힐링 영화 추천";
            case "romantic" -> "로맨틱한 영화 추천";
            case "with_family" -> "가족과 보기 좋은 영화";
            case "with_partner" -> "연인과 보기 좋은 영화";
            case "dark" -> "어두운 분위기의 영화 추천";
            case "investigation" -> "수사물 영화 추천";
            case "coming_of_age" -> "성장 영화 추천";
            case "true_story" -> "실화 기반 영화 추천";
            default -> tagDisplayName(tagCode) + " 영화 추천";
        };
    }

    public String keywordBlacklistSqlLiteralList() {
        return KEYWORD_BLACKLIST.stream()
                .sorted()
                .map(keyword -> "'" + keyword.replace("'", "''") + "'")
                .reduce((left, right) -> left + ", " + right)
                .orElse("''");
    }

    public List<String> keywordBlacklist() {
        return KEYWORD_BLACKLIST.stream().sorted().toList();
    }

    private boolean matchesPerformanceGenre(Set<String> genres) {
        if (genres == null || genres.isEmpty()) {
            return false;
        }
        Set<String> normalizedGenres = genres.stream()
                .filter(name -> name != null && !name.isBlank())
                .map(this::normalizeRuleText)
                .collect(Collectors.toSet());
        return PERFORMANCE_GENRE_NAMES.stream()
                .map(this::normalizeRuleText)
                .anyMatch(normalizedGenres::contains);
    }

    private boolean containsStrongPerformanceKeyword(Set<String> keywords) {
        if (keywords == null || keywords.isEmpty()) {
            return false;
        }
        return keywords.stream()
                .filter(keyword -> keyword != null && !keyword.isBlank())
                .map(this::normalizeRuleText)
                .anyMatch(keyword -> keyword.contains("concert")
                        || keyword.contains("music documentary")
                        || keyword.contains("공연실황")
                        || keyword.contains("콘서트"));
    }

    private boolean matchesPerformanceTitle(String title) {
        if (title == null || title.isBlank()) {
            return false;
        }
        String normalizedTitle = normalizeRuleText(title);
        return PERFORMANCE_TITLE_PATTERNS.stream()
                .map(this::normalizeRuleText)
                .anyMatch(normalizedTitle::contains);
    }

    private int countPatternHits(Set<String> values, List<String> patterns) {
        if (values == null || values.isEmpty()) {
            return 0;
        }
        Set<String> normalizedValues = values.stream()
                .filter(value -> value != null && !value.isBlank())
                .map(this::normalizeRuleText)
                .collect(Collectors.toSet());

        int hits = 0;
        for (String pattern : patterns) {
            String normalizedPattern = normalizeRuleText(pattern);
            boolean matched = normalizedValues.stream().anyMatch(value -> value.contains(normalizedPattern));
            if (matched) {
                hits++;
            }
        }
        return hits;
    }

    private double saturate(double rawScore, double saturationConstant) {
        if (rawScore <= 0.0) {
            return 0.0;
        }
        return rawScore / (rawScore + saturationConstant);
    }

    private String normalizeKeyword(String keyword) {
        return keyword.trim().toLowerCase(Locale.ROOT);
    }

    private String normalizeRuleText(String text) {
        return text == null ? "" : text.trim().toLowerCase(Locale.ROOT);
    }
}
