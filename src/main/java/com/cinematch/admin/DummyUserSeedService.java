package com.cinematch.admin;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class DummyUserSeedService {

    private static final String TEST_USER_LOGIN_PREFIX = "testuser";
    private static final String TEST_USER_LOGIN_PATTERN = TEST_USER_LOGIN_PREFIX + "%";
    // Test seed only. Production passwords should be stored as hashes.
    private static final String TEST_USER_PASSWORD = "Test1234!";

    private static final List<DummyUserPersona> PERSONA_CATALOG = List.of(
            new DummyUserPersona(
                    "testuser01",
                    "긴장추적01",
                    "MALE",
                    27,
                    "스릴러/미스터리 선호",
                    List.of("tense", "mystery", "investigation", "dark"),
                    8,
                    5,
                    2,
                    10
            ),
            new DummyUserPersona(
                    "testuser02",
                    "로맨스감성02",
                    "FEMALE",
                    25,
                    "로맨스/감성 선호",
                    List.of("romantic", "emotional", "with_partner"),
                    7,
                    4,
                    1,
                    8
            ),
            new DummyUserPersona(
                    "testuser03",
                    "패밀리애니03",
                    "FEMALE",
                    33,
                    "가족/애니메이션 선호",
                    List.of("with_family", "hopeful", "friendship"),
                    6,
                    5,
                    2,
                    9
            ),
            new DummyUserPersona(
                    "testuser04",
                    "액션모험04",
                    "MALE",
                    24,
                    "액션/모험 선호",
                    List.of("spectacle", "survival", "adventure"),
                    8,
                    6,
                    1,
                    11
            ),
            new DummyUserPersona(
                    "testuser05",
                    "다크스릴05",
                    "MALE",
                    31,
                    "dark/tense 강취향",
                    List.of("dark", "tense", "revenge"),
                    7,
                    5,
                    2,
                    9
            ),
            new DummyUserPersona(
                    "testuser06",
                    "힐링잔잔06",
                    "FEMALE",
                    29,
                    "힐링/잔잔한 영화 선호",
                    List.of("healing", "hopeful", "friendship"),
                    6,
                    4,
                    1,
                    8
            ),
            new DummyUserPersona(
                    "testuser07",
                    "성장판타지07",
                    "FEMALE",
                    22,
                    "fantasy/coming_of_age 선호",
                    List.of("coming_of_age", "friendship", "hopeful", "fantasy"),
                    7,
                    4,
                    2,
                    9
            ),
            new DummyUserPersona(
                    "testuser08",
                    "재난생존08",
                    "MALE",
                    28,
                    "재난/생존 장르 선호",
                    List.of("disaster", "survival", "tense"),
                    7,
                    4,
                    1,
                    8
            ),
            new DummyUserPersona(
                    "testuser09",
                    "좀비호러09",
                    "MALE",
                    26,
                    "좀비/호러 취향",
                    List.of("zombie", "creepy", "dark"),
                    6,
                    4,
                    1,
                    7
            ),
            new DummyUserPersona(
                    "testuser10",
                    "감독취향10",
                    "FEMALE",
                    35,
                    "감독 중심 선호",
                    List.of("director_focus", "signature_style"),
                    7,
                    5,
                    2,
                    8
            ),
            new DummyUserPersona(
                    "testuser11",
                    "배우취향11",
                    "FEMALE",
                    30,
                    "배우 중심 선호",
                    List.of("actor_focus", "star_cast"),
                    7,
                    5,
                    1,
                    8
            ),
            new DummyUserPersona(
                    "testuser12",
                    "넷플릭스만12",
                    "MALE",
                    23,
                    "OTT 제약이 강한 유저",
                    List.of("provider_focus", "netflix", "tense"),
                    6,
                    6,
                    1,
                    10
            ),
            new DummyUserPersona(
                    "testuser13",
                    "디즈니패밀리13",
                    "FEMALE",
                    38,
                    "Disney Plus 기반 가족 취향",
                    List.of("provider_focus", "disney_plus", "with_family"),
                    6,
                    5,
                    2,
                    9
            ),
            new DummyUserPersona(
                    "testuser14",
                    "올라운더14",
                    "MALE",
                    32,
                    "취향이 넓은 유저",
                    List.of("broad_taste", "action", "romance", "thriller"),
                    10,
                    8,
                    1,
                    14
            ),
            new DummyUserPersona(
                    "testuser15",
                    "취향확실15",
                    "FEMALE",
                    21,
                    "취향이 좁고 강한 유저",
                    List.of("narrow_taste", "single_tag_focus"),
                    5,
                    3,
                    2,
                    6
            ),
            new DummyUserPersona(
                    "testuser16",
                    "실화감동16",
                    "FEMALE",
                    34,
                    "실화/감동형 취향",
                    List.of("true_story", "emotional", "friendship"),
                    7,
                    4,
                    1,
                    8
            )
    );

    private final JdbcTemplate jdbcTemplate;

    public DummyUserSeedService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<DummyUserPersona> catalog() {
        return PERSONA_CATALOG;
    }

    @Transactional
    public DummyUserSeedResult seedTestUsers(boolean reset) {
        List<Long> existingUserIds = loadExistingTestUserIds();
        Set<String> existingLoginIds = loadExistingTestUserLoginIds();

        DeletionSummary deletionSummary = DeletionSummary.empty();
        if (reset) {
            deletionSummary = deleteExistingTestUsers(existingUserIds);
            existingLoginIds = Set.of();
        }

        List<SeededDummyUser> users = new ArrayList<>();
        int createdCount = 0;
        int skippedCount = 0;

        for (DummyUserPersona entry : PERSONA_CATALOG) {
            if (existingLoginIds.contains(entry.loginId())) {
                skippedCount++;
                users.add(SeededDummyUser.from(entry, "SKIPPED"));
                continue;
            }

            jdbcTemplate.update("""
                    INSERT INTO "USER" (login_id, login_pw, nickname, gender, age)
                    VALUES (?, ?, ?, ?, ?)
                    """, entry.loginId(), TEST_USER_PASSWORD, entry.nickname(), entry.gender(), entry.age());
            createdCount++;
            users.add(SeededDummyUser.from(entry, "CREATED"));
        }

        return new DummyUserSeedResult(
                reset,
                PERSONA_CATALOG.size(),
                createdCount,
                skippedCount,
                TEST_USER_LOGIN_PREFIX,
                TEST_USER_PASSWORD,
                deletionSummary,
                users
        );
    }

    private DeletionSummary deleteExistingTestUsers(List<Long> userIds) {
        if (userIds.isEmpty()) {
            return DeletionSummary.empty();
        }

        int recommendationResultCount = deleteUserScopedRows("user_recommendation_result", userIds);
        int preferenceProfileCount = deleteUserScopedRows("user_preference_profile", userIds);
        int refreshStateCount = deleteUserScopedRows("user_recommendation_refresh_state", userIds);
        int likeCount = deleteUserScopedRows("user_movie_like", userIds);
        int collectionCount = deleteUserScopedRows("user_movie_collection", userIds);
        int storeCount = deleteUserScopedRows("user_movie_store", userIds);
        int watchedCount = deleteUserScopedRows("user_movie_watched", userIds);
        int lifeCount = deleteUserScopedRows("user_movie_life", userIds);
        int userCount = deleteUsers(userIds);

        return new DeletionSummary(
                recommendationResultCount,
                preferenceProfileCount,
                refreshStateCount,
                likeCount + collectionCount,
                storeCount,
                watchedCount,
                lifeCount,
                userCount
        );
    }

    private int deleteUserScopedRows(String tableName, List<Long> userIds) {
        if (userIds.isEmpty()) {
            return 0;
        }

        String sql = "DELETE FROM " + tableName + " WHERE user_id IN (" + placeholders(userIds.size()) + ")";
        return jdbcTemplate.update(sql, userIds.toArray());
    }

    private int deleteUsers(List<Long> userIds) {
        if (userIds.isEmpty()) {
            return 0;
        }

        String sql = "DELETE FROM \"USER\" WHERE id IN (" + placeholders(userIds.size()) + ")";
        return jdbcTemplate.update(sql, userIds.toArray());
    }

    private List<Long> loadExistingTestUserIds() {
        return jdbcTemplate.query("""
                SELECT id
                FROM "USER"
                WHERE login_id LIKE ?
                ORDER BY id
                """, (rs, rowNum) -> rs.getLong("id"), TEST_USER_LOGIN_PATTERN);
    }

    private Set<String> loadExistingTestUserLoginIds() {
        return new LinkedHashSet<>(jdbcTemplate.query("""
                SELECT login_id
                FROM "USER"
                WHERE login_id LIKE ?
                ORDER BY id
                """, (rs, rowNum) -> rs.getString("login_id"), TEST_USER_LOGIN_PATTERN));
    }

    private String placeholders(int count) {
        return java.util.stream.IntStream.range(0, count)
                .mapToObj(index -> "?")
                .collect(Collectors.joining(", "));
    }

    public record DummyUserPersona(
            String loginId,
            String nickname,
            String gender,
            int age,
            String preferenceTypeSummary,
            List<String> representativeFeatures,
            int targetLikeCount,
            int targetStoreCount,
            int targetLifeCount,
            int plannedWatchedCount
    ) {
    }

    public record DeletionSummary(
            int recommendationResultCount,
            int preferenceProfileCount,
            int refreshStateCount,
            int likeCount,
            int storeCount,
            int watchedCount,
            int lifeCount,
            int userCount
    ) {
        private static DeletionSummary empty() {
            return new DeletionSummary(0, 0, 0, 0, 0, 0, 0, 0);
        }
    }

    public record SeededDummyUser(
            String loginId,
            String nickname,
            String gender,
            int age,
            String preferenceTypeSummary,
            List<String> representativeFeatures,
            int targetLikeCount,
            int targetStoreCount,
            int targetLifeCount,
            int plannedWatchedCount,
            String status
    ) {
        private static SeededDummyUser from(DummyUserPersona entry, String status) {
            return new SeededDummyUser(
                    entry.loginId(),
                    entry.nickname(),
                    entry.gender(),
                    entry.age(),
                    entry.preferenceTypeSummary(),
                    entry.representativeFeatures(),
                    entry.targetLikeCount(),
                    entry.targetStoreCount(),
                    entry.targetLifeCount(),
                    entry.plannedWatchedCount(),
                    status
            );
        }
    }

    public record DummyUserSeedResult(
            boolean reset,
            int catalogSize,
            int createdCount,
            int skippedCount,
            String loginIdPrefix,
            String commonPassword,
            DeletionSummary deletionSummary,
            List<SeededDummyUser> users
    ) {
    }
}
