package movie.web.login.tag;

public enum RecommendationTag {

    FUNNY("funny", RecommendationTagType.MOOD, 55),
    TENSE("tense", RecommendationTagType.MOOD, 55),
    DARK("dark", RecommendationTagType.MOOD, 55),
    EMOTIONAL("emotional", RecommendationTagType.MOOD, 60),
    ROMANTIC("romantic", RecommendationTagType.MOOD, 60),
    HOPEFUL("hopeful", RecommendationTagType.MOOD, 60),
    SPECTACLE("spectacle", RecommendationTagType.MOOD, 55),
    CREEPY("creepy", RecommendationTagType.MOOD, 60),

    WITH_FAMILY("with_family", RecommendationTagType.CONTEXT, 60),
    WITH_PARTNER("with_partner", RecommendationTagType.CONTEXT, 60),
    LATE_NIGHT("late_night", RecommendationTagType.CONTEXT, 55),

    VIOLENT("violent", RecommendationTagType.CAUTION, 60),
    SAD("sad", RecommendationTagType.CAUTION, 60),
    SLOW_BURN("slow_burn", RecommendationTagType.CAUTION, 55),
    LONG_RUNNING("long_running", RecommendationTagType.CAUTION, 55),

    INVESTIGATION("investigation", RecommendationTagType.THEME, 60),
    ZOMBIE("zombie", RecommendationTagType.THEME, 60),
    DISASTER("disaster", RecommendationTagType.THEME, 60),
    TRUE_STORY("true_story", RecommendationTagType.THEME, 55),
    COMING_OF_AGE("coming_of_age", RecommendationTagType.THEME, 60);

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
