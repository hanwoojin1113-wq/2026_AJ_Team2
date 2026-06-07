package com.cinematch;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@RequiredArgsConstructor
public class BadgeService {

    private final JdbcTemplate jdbcTemplate;

    private volatile boolean tablesReady = false;

    // ── Badge Definitions ──────────────────────────────────────────────

    public record BadgeDef(String code, String name, String description, int tier, String icon, String conditionLabel) {}

    public static final List<BadgeDef> ALL_BADGES = List.of(
        // Tier 1 – Bronze
        new BadgeDef("WELCOME",         "첫 걸음",      "CineMatch에 가입했어요",          1, "🎬", "가입 완료"),
        new BadgeDef("FIRST_WATCH",     "영화 입문",    "첫 영화를 기록했어요",             1, "👁",  "1편 이상 시청"),
        new BadgeDef("FIRST_POST",      "첫 포스트",    "첫 게시물을 작성했어요",           1, "✍",  "1개 이상 포스트"),
        new BadgeDef("FIRST_FOLLOW",    "첫 연결",      "첫 팔로우를 시작했어요",           1, "🤝", "1명 이상 팔로우"),
        // Tier 2 – Silver
        new BadgeDef("MOVIE_FAN",       "영화 팬",      "5편 이상 시청 기록을 남겼어요",    2, "🎭", "5편 이상 시청"),
        new BadgeDef("TASTE_EXPLORER",  "취향 탐구자",  "5편 이상 좋아요를 남겼어요",       2, "🧭", "5편 이상 좋아요"),
        new BadgeDef("ACTIVE_REVIEWER", "열정 리뷰어",  "3개 이상 게시물을 작성했어요",     2, "📝", "3개 이상 포스트"),
        new BadgeDef("SOCIAL_BUDDY",    "소셜 버디",    "5명 이상의 팔로워를 얻었어요",     2, "👥", "팔로워 5명 이상"),
        // Tier 3 – Gold
        new BadgeDef("CINEPHILE",       "시네필",       "15편 이상 시청 기록을 남겼어요",   3, "🎞", "15편 이상 시청"),
        new BadgeDef("INFLUENCER",      "인플루언서",   "10명 이상의 팔로워를 얻었어요",    3, "💫", "팔로워 10명 이상"),
        new BadgeDef("MOVIE_MANIAC",    "영화 마니아",  "10편 이상 좋아요를 남겼어요",      3, "❤",  "10편 이상 좋아요"),
        // Tier 4 – Platinum
        new BadgeDef("CINEMASTER",      "시네마스터",   "30편 이상 시청 기록을 남겼어요",   4, "👑", "30편 이상 시청"),
        new BadgeDef("COMMUNITY_STAR",  "커뮤니티 스타","25명 이상의 팔로워를 얻었어요",    4, "🌟", "팔로워 25명 이상")
    );

    private static final Map<String, BadgeDef> BADGE_MAP;
    static {
        Map<String, BadgeDef> m = new LinkedHashMap<>();
        for (BadgeDef b : ALL_BADGES) m.put(b.code(), b);
        BADGE_MAP = Collections.unmodifiableMap(m);
    }

    public static BadgeDef getBadgeDef(String code) {
        return code == null ? null : BADGE_MAP.get(code);
    }

    // ── Table Bootstrap ───────────────────────────────────────────────

    public void ensureTables() {
        if (tablesReady) return;
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS user_badge_earned (
                    user_id    BIGINT      NOT NULL,
                    badge_code VARCHAR(50) NOT NULL,
                    earned_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, badge_code)
                )
                """);
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS user_badge_selected (
                    user_id    BIGINT      NOT NULL PRIMARY KEY,
                    badge_code VARCHAR(50),
                    updated_at TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """);
        tablesReady = true;
    }

    // ── Core Logic ────────────────────────────────────────────────────

    public void refreshBadges(Long userId) {
        ensureTables();
        int watched   = countWatched(userId);
        int liked     = countLiked(userId);
        int posts     = countPosts(userId);
        int follows   = countFollows(userId);
        int followers = countFollowers(userId);

        Set<String> earned = loadEarnedCodes(userId);
        award(userId, "WELCOME",         true,             earned);
        award(userId, "FIRST_WATCH",     watched >= 1,     earned);
        award(userId, "FIRST_POST",      posts >= 1,       earned);
        award(userId, "FIRST_FOLLOW",    follows >= 1,     earned);
        award(userId, "MOVIE_FAN",       watched >= 5,     earned);
        award(userId, "TASTE_EXPLORER",  liked >= 5,       earned);
        award(userId, "ACTIVE_REVIEWER", posts >= 3,       earned);
        award(userId, "SOCIAL_BUDDY",    followers >= 5,   earned);
        award(userId, "CINEPHILE",       watched >= 15,    earned);
        award(userId, "INFLUENCER",      followers >= 10,  earned);
        award(userId, "MOVIE_MANIAC",    liked >= 10,      earned);
        award(userId, "CINEMASTER",      watched >= 30,    earned);
        award(userId, "COMMUNITY_STAR",  followers >= 25,  earned);
    }

    public record BadgeStatus(BadgeDef def, boolean earned, int progress, int goal, String earnedAt) {}

    public List<BadgeStatus> getBadgeStatuses(Long userId) {
        ensureTables();
        refreshBadges(userId);

        int watched   = countWatched(userId);
        int liked     = countLiked(userId);
        int posts     = countPosts(userId);
        int follows   = countFollows(userId);
        int followers = countFollowers(userId);

        Set<String> earned = loadEarnedCodes(userId);
        Map<String, String> dates = loadEarnedDates(userId);

        List<BadgeStatus> result = new ArrayList<>();
        for (BadgeDef def : ALL_BADGES) {
            boolean e = earned.contains(def.code());
            String at = dates.getOrDefault(def.code(), null);
            int prog = 0, goal = 0;
            switch (def.code()) {
                case "WELCOME"         -> { prog = 1;                            goal = 1; }
                case "FIRST_WATCH"     -> { prog = Math.min(watched, 1);         goal = 1; }
                case "FIRST_POST"      -> { prog = Math.min(posts, 1);           goal = 1; }
                case "FIRST_FOLLOW"    -> { prog = Math.min(follows, 1);         goal = 1; }
                case "MOVIE_FAN"       -> { prog = Math.min(watched, 5);         goal = 5; }
                case "TASTE_EXPLORER"  -> { prog = Math.min(liked, 5);           goal = 5; }
                case "ACTIVE_REVIEWER" -> { prog = Math.min(posts, 3);           goal = 3; }
                case "SOCIAL_BUDDY"    -> { prog = Math.min(followers, 5);       goal = 5; }
                case "CINEPHILE"       -> { prog = Math.min(watched, 15);        goal = 15; }
                case "INFLUENCER"      -> { prog = Math.min(followers, 10);      goal = 10; }
                case "MOVIE_MANIAC"    -> { prog = Math.min(liked, 10);          goal = 10; }
                case "CINEMASTER"      -> { prog = Math.min(watched, 30);        goal = 30; }
                case "COMMUNITY_STAR"  -> { prog = Math.min(followers, 25);      goal = 25; }
            }
            result.add(new BadgeStatus(def, e, prog, goal, at));
        }
        return result;
    }

    public String getSelectedBadgeCode(Long userId) {
        ensureTables();
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT badge_code FROM user_badge_selected WHERE user_id = ?",
                    String.class, userId);
        } catch (Exception e) {
            return null;
        }
    }

    /** Returns the selected badge code for a given login_id (used in feed enrichment). */
    public String getSelectedBadgeCodeByLoginId(String loginId) {
        ensureTables();
        try {
            Long userId = jdbcTemplate.queryForObject(
                    "SELECT id FROM \"USER\" WHERE login_id = ?", Long.class, loginId);
            if (userId == null) return null;
            return getSelectedBadgeCode(userId);
        } catch (Exception e) {
            return null;
        }
    }

    public void selectBadge(Long userId, String badgeCode) {
        ensureTables();
        if (badgeCode == null || badgeCode.isBlank()) {
            jdbcTemplate.update("DELETE FROM user_badge_selected WHERE user_id = ?", userId);
            return;
        }
        if (!loadEarnedCodes(userId).contains(badgeCode)) {
            throw new IllegalArgumentException("Badge not earned: " + badgeCode);
        }
        Integer exists = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM user_badge_selected WHERE user_id = ?", Integer.class, userId);
        if (exists != null && exists > 0) {
            jdbcTemplate.update(
                    "UPDATE user_badge_selected SET badge_code = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?",
                    badgeCode, userId);
        } else {
            jdbcTemplate.update(
                    "INSERT INTO user_badge_selected (user_id, badge_code, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                    userId, badgeCode);
        }
    }

    // ── Private Helpers ───────────────────────────────────────────────

    private void award(Long userId, String code, boolean condition, Set<String> already) {
        if (!condition || already.contains(code)) return;
        try {
            jdbcTemplate.update(
                    "INSERT INTO user_badge_earned (user_id, badge_code, earned_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                    userId, code);
        } catch (Exception ignored) {}
        already.add(code);
    }

    private Set<String> loadEarnedCodes(Long userId) {
        return new HashSet<>(jdbcTemplate.queryForList(
                "SELECT badge_code FROM user_badge_earned WHERE user_id = ?", String.class, userId));
    }

    private Map<String, String> loadEarnedDates(Long userId) {
        Map<String, String> m = new HashMap<>();
        List<String[]> rows = jdbcTemplate.query(
                "SELECT badge_code, earned_at FROM user_badge_earned WHERE user_id = ?",
                (rs, i) -> new String[]{rs.getString("badge_code"),
                                        rs.getTimestamp("earned_at").toLocalDateTime().toLocalDate().toString()},
                userId);
        for (String[] row : rows) m.put(row[0], row[1]);
        return m;
    }

    private int countWatched(Long userId) {
        Integer n = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM user_movie_watched WHERE user_id = ?", Integer.class, userId);
        return n != null ? n : 0;
    }

    private int countLiked(Long userId) {
        Integer n = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM user_movie_like WHERE user_id = ? AND liked = TRUE", Integer.class, userId);
        return n != null ? n : 0;
    }

    private int countPosts(Long userId) {
        try {
            Integer n = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM social_post WHERE user_id = ? AND is_deleted = FALSE",
                    Integer.class, userId);
            return n != null ? n : 0;
        } catch (Exception e) { return 0; }
    }

    private int countFollows(Long userId) {
        Integer n = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM user_follow WHERE follower_user_id = ?", Integer.class, userId);
        return n != null ? n : 0;
    }

    private int countFollowers(Long userId) {
        Integer n = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM user_follow WHERE following_user_id = ?", Integer.class, userId);
        return n != null ? n : 0;
    }
}
