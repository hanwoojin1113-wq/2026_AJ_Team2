package com.cinematch.tag;

public enum RecommendationTag {
    /*
     * code: DB와 추천 로직에서 쓰는 내부 식별자
     * type: 태그의 성격(MOOD/CONTEXT/CAUTION/THEME)
     * threshold: 이 점수 이상일 때만 실제로 태그를 부여한다
     */

    FUNNY("funny", RecommendationTagType.MOOD, 54),
    TENSE("tense", RecommendationTagType.MOOD, 55),
    DARK("dark", RecommendationTagType.MOOD, 52),
    EMOTIONAL("emotional", RecommendationTagType.MOOD, 52),
    ROMANTIC("romantic", RecommendationTagType.MOOD, 54),
    HOPEFUL("hopeful", RecommendationTagType.MOOD, 52),
    HEALING("healing", RecommendationTagType.MOOD, 52),
    SPECTACLE("spectacle", RecommendationTagType.MOOD, 55),
    CREEPY("creepy", RecommendationTagType.MOOD, 60),

    WITH_FAMILY("with_family", RecommendationTagType.CONTEXT, 52),
    WITH_PARTNER("with_partner", RecommendationTagType.CONTEXT, 53),
    LATE_NIGHT("late_night", RecommendationTagType.CONTEXT, 55),

    VIOLENT("violent", RecommendationTagType.CAUTION, 52),
    SAD("sad", RecommendationTagType.CAUTION, 52),
    SLOW_BURN("slow_burn", RecommendationTagType.CAUTION, 52),
    LONG_RUNNING("long_running", RecommendationTagType.CAUTION, 55),

    INVESTIGATION("investigation", RecommendationTagType.THEME, 53),
    MYSTERY("mystery", RecommendationTagType.THEME, 52),
    ZOMBIE("zombie", RecommendationTagType.THEME, 60),
    DISASTER("disaster", RecommendationTagType.THEME, 52),
    TRUE_STORY("true_story", RecommendationTagType.THEME, 55),
    COMING_OF_AGE("coming_of_age", RecommendationTagType.THEME, 50),
    FRIENDSHIP("friendship", RecommendationTagType.THEME, 52),
    SURVIVAL("survival", RecommendationTagType.THEME, 52),
    REVENGE("revenge", RecommendationTagType.THEME, 52);

    private final String code;
    private final RecommendationTagType type;
    private final int threshold;

    RecommendationTag(String code, RecommendationTagType type, int threshold) {
        this.code = code;
        this.type = type;
        this.threshold = threshold;
    }

    public String code() {
        return code;
    }

    public RecommendationTagType type() {
        return type;
    }

    public int threshold() {
        return threshold;
    }
}
