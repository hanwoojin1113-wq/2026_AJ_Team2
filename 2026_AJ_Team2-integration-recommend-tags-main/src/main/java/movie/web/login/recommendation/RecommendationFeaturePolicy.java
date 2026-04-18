package movie.web.login.recommendation;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.springframework.stereotype.Component;

@Component
public class RecommendationFeaturePolicy {

    public static final String ALGORITHM_VERSION = "content-ranking-v3";

    private static final int ACTOR_LIMIT = 5;
    private static final int KEYWORD_LIMIT = 8;
    private static final int DEFAULT_PROFILE_FEATURE_LIMIT = 12;
    private static final int RANKING_BLOCK_SLICE_LIMIT = 120;
    private static final int RANKING_BLOCK_ITEM_LIMIT = 10;
    private static final int BLOCK_MIN_SIZE = 4;

    private static final double LIFE_SIGNAL_WEIGHT = 5.0;
    private static final double LIKE_SIGNAL_WEIGHT = 3.0;
    private static final double WATCHED_SIGNAL_WEIGHT = 2.2;
    private static final double STORE_SIGNAL_WEIGHT = 1.5;
    private static final double UNRATED_WATCHED_MULTIPLIER = 0.70;
    private static final Map<Integer, Double> WATCHED_RATING_MULTIPLIERS = Map.of(
            1, 0.15,
            2, 0.45,
            3, 0.85,
            4, 1.05,
            5, 1.20
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
            "DIRECTOR", 8,
            "ACTOR", 20,
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

    private static final Map<String, String> TAG_LABELS = Map.ofEntries(
            Map.entry("funny", "유쾌한"),
            Map.entry("tense", "긴장감 있는"),
            Map.entry("dark", "어두운"),
            Map.entry("emotional", "감성적인"),
            Map.entry("romantic", "로맨틱한"),
            Map.entry("hopeful", "희망적인"),
            Map.entry("healing", "힐링"),
            Map.entry("spectacle", "볼거리 있는"),
            Map.entry("creepy", "오싹한"),
            Map.entry("with_family", "가족과 보기 좋은"),
            Map.entry("with_partner", "함께 보기 좋은"),
            Map.entry("late_night", "야심한 밤에 어울리는"),
            Map.entry("violent", "폭력적인"),
            Map.entry("sad", "슬픈"),
            Map.entry("slow_burn", "느리게 쌓이는"),
            Map.entry("long_running", "긴 러닝타임"),
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
        return WATCHED_SIGNAL_WEIGHT * watchedRatingMultiplier(rating);
    }

    public double watchedRatingMultiplier(Integer rating) {
        if (rating == null) {
            return UNRATED_WATCHED_MULTIPLIER;
        }
        return WATCHED_RATING_MULTIPLIERS.getOrDefault(rating, UNRATED_WATCHED_MULTIPLIER);
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

    public boolean isGenreBlockAllowed(String genreName, int sampleCount) {
        if (!isBroadGenre(genreName)) {
            return sampleCount >= blockMinimumSize();
        }
        return sampleCount >= blockMinimumSize() + 2;
    }

    public String tagDisplayName(String tagCode) {
        return TAG_LABELS.getOrDefault(tagCode, tagCode.replace('_', ' '));
    }

    public String tagBlockTitle(String tagCode) {
        return switch (tagCode) {
            case "tense" -> "긴장감 있는 영화 추천";
            case "mystery" -> "미스터리 영화 추천";
            case "healing" -> "힐링 영화 추천";
            case "romantic" -> "로맨스 영화 추천";
            case "with_family" -> "가족과 보기 좋은 영화";
            case "with_partner" -> "함께 보기 좋은 영화";
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

    private String normalizeKeyword(String keyword) {
        return keyword.trim().toLowerCase(Locale.ROOT);
    }
}
