package com.cinematch.admin;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.cinematch.recommendation.RecommendationFeaturePolicy;
import com.cinematch.recommendation.RecommendationMaintenanceService;
import com.cinematch.recommendation.RecommendationRefreshStateService;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class DummyUserActivitySeedService {

    private static final String TEST_USER_LOGIN_PATTERN = "testuser%";
    private static final Map<String, PersonaActivityRule> PERSONA_RULES = Map.ofEntries(
            Map.entry("testuser01", new PersonaActivityRule(
                    List.of("tense", "mystery", "investigation", "dark"),
                    List.of("revenge"),
                    List.of("스릴러", "미스터리", "범죄"),
                    List.of("드라마"),
                    List.of(),
                    List.of("survival"),
                    List.of("액션"),
                    FocusMode.NONE
            )),
            Map.entry("testuser02", new PersonaActivityRule(
                    List.of("romantic", "emotional", "with_partner"),
                    List.of("hopeful", "friendship"),
                    List.of("로맨스", "드라마"),
                    List.of("코미디"),
                    List.of(),
                    List.of("with_family"),
                    List.of("모험"),
                    FocusMode.NONE
            )),
            Map.entry("testuser03", new PersonaActivityRule(
                    List.of("with_family", "hopeful", "friendship"),
                    List.of("funny"),
                    List.of("가족", "애니메이션", "모험"),
                    List.of("코미디"),
                    List.of("disney"),
                    List.of("healing"),
                    List.of("판타지"),
                    FocusMode.NONE
            )),
            Map.entry("testuser04", new PersonaActivityRule(
                    List.of("spectacle", "survival", "tense"),
                    List.of("disaster"),
                    List.of("액션", "모험", "SF"),
                    List.of("스릴러"),
                    List.of(),
                    List.of("revenge"),
                    List.of("범죄"),
                    FocusMode.NONE
            )),
            Map.entry("testuser05", new PersonaActivityRule(
                    List.of("dark", "tense", "revenge"),
                    List.of("mystery"),
                    List.of("스릴러", "범죄", "미스터리"),
                    List.of("드라마"),
                    List.of(),
                    List.of("creepy"),
                    List.of("공포"),
                    FocusMode.NONE
            )),
            Map.entry("testuser06", new PersonaActivityRule(
                    List.of("healing", "hopeful", "friendship"),
                    List.of("with_family"),
                    List.of("드라마", "코미디"),
                    List.of("가족"),
                    List.of(),
                    List.of("romantic"),
                    List.of("로맨스"),
                    FocusMode.NONE
            )),
            Map.entry("testuser07", new PersonaActivityRule(
                    List.of("coming_of_age", "friendship", "hopeful"),
                    List.of("healing"),
                    List.of("판타지", "모험", "애니메이션"),
                    List.of("가족"),
                    List.of(),
                    List.of("romantic"),
                    List.of("드라마"),
                    FocusMode.NONE
            )),
            Map.entry("testuser08", new PersonaActivityRule(
                    List.of("disaster", "survival", "tense"),
                    List.of("spectacle"),
                    List.of("스릴러", "액션", "SF"),
                    List.of("모험"),
                    List.of(),
                    List.of("investigation"),
                    List.of("드라마"),
                    FocusMode.NONE
            )),
            Map.entry("testuser09", new PersonaActivityRule(
                    List.of("zombie", "creepy", "dark"),
                    List.of("survival"),
                    List.of("공포", "스릴러"),
                    List.of("액션"),
                    List.of(),
                    List.of("disaster"),
                    List.of("SF"),
                    FocusMode.NONE
            )),
            Map.entry("testuser10", new PersonaActivityRule(
                    List.of("emotional", "mystery", "tense"),
                    List.of("true_story"),
                    List.of("드라마", "미스터리", "스릴러"),
                    List.of("범죄"),
                    List.of(),
                    List.of("friendship"),
                    List.of("로맨스"),
                    FocusMode.DIRECTOR
            )),
            Map.entry("testuser11", new PersonaActivityRule(
                    List.of("romantic", "emotional", "tense"),
                    List.of("friendship"),
                    List.of("드라마", "로맨스", "스릴러"),
                    List.of("미스터리"),
                    List.of(),
                    List.of("hopeful"),
                    List.of("코미디"),
                    FocusMode.ACTOR
            )),
            Map.entry("testuser12", new PersonaActivityRule(
                    List.of("tense", "mystery"),
                    List.of("emotional"),
                    List.of("스릴러", "드라마"),
                    List.of("미스터리"),
                    List.of("netflix"),
                    List.of("survival"),
                    List.of("액션"),
                    FocusMode.NONE
            )),
            Map.entry("testuser13", new PersonaActivityRule(
                    List.of("with_family", "hopeful", "friendship"),
                    List.of("spectacle"),
                    List.of("가족", "애니메이션", "모험"),
                    List.of("판타지"),
                    List.of("disney"),
                    List.of("funny"),
                    List.of("코미디"),
                    FocusMode.NONE
            )),
            Map.entry("testuser14", new PersonaActivityRule(
                    List.of("tense", "funny", "romantic", "hopeful"),
                    List.of("with_partner", "with_family"),
                    List.of("드라마", "액션", "코미디", "로맨스", "스릴러"),
                    List.of("모험", "미스터리"),
                    List.of("netflix", "disney", "wavve"),
                    List.of("true_story"),
                    List.of("가족"),
                    FocusMode.NONE
            )),
            Map.entry("testuser15", new PersonaActivityRule(
                    List.of("mystery", "investigation"),
                    List.of("dark"),
                    List.of("미스터리"),
                    List.of("스릴러"),
                    List.of(),
                    List.of("emotional"),
                    List.of("드라마"),
                    FocusMode.NONE
            )),
            Map.entry("testuser16", new PersonaActivityRule(
                    List.of("true_story", "emotional", "friendship"),
                    List.of("hopeful"),
                    List.of("드라마", "역사"),
                    List.of("로맨스"),
                    List.of(),
                    List.of("with_family"),
                    List.of("가족"),
                    FocusMode.NONE
            ))
    );

    private final JdbcTemplate jdbcTemplate;
    private final DummyUserSeedService dummyUserSeedService;
    private final RecommendationFeaturePolicy recommendationFeaturePolicy;
    private final RecommendationRefreshStateService recommendationRefreshStateService;
    private final RecommendationMaintenanceService recommendationMaintenanceService;

    public DummyUserActivitySeedService(
            JdbcTemplate jdbcTemplate,
            DummyUserSeedService dummyUserSeedService,
            RecommendationFeaturePolicy recommendationFeaturePolicy,
            RecommendationRefreshStateService recommendationRefreshStateService,
            RecommendationMaintenanceService recommendationMaintenanceService
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.dummyUserSeedService = dummyUserSeedService;
        this.recommendationFeaturePolicy = recommendationFeaturePolicy;
        this.recommendationRefreshStateService = recommendationRefreshStateService;
        this.recommendationMaintenanceService = recommendationMaintenanceService;
    }

    @Transactional
    public DummyUserActivitySeedResult seedActivities(boolean reset, boolean refreshRecommendations) {
        dummyUserSeedService.seedTestUsers(false);

        List<DummyUserSeedService.DummyUserPersona> personas = dummyUserSeedService.catalog();
        Map<String, Long> userIdsByLogin = loadTestUserIdsByLogin();
        DeletionSummary deletionSummary = reset
                ? deleteExistingActivities(userIdsByLogin.values())
                : DeletionSummary.empty();

        List<MovieSeedCandidate> movieCatalog = loadMovieCatalog();
        List<SeededDummyUserActivity> users = new ArrayList<>();
        int seededUserCount = 0;
        int skippedUserCount = 0;
        int insertedLikeCount = 0;
        int insertedStoreCount = 0;
        int insertedLifeCount = 0;

        for (DummyUserSeedService.DummyUserPersona persona : personas) {
            Long userId = userIdsByLogin.get(persona.loginId());
            if (userId == null) {
                users.add(SeededDummyUserActivity.missing(persona));
                continue;
            }

            ActivityCount existing = currentActivityCount(userId);
            if (!reset && existing.totalCount() > 0) {
                skippedUserCount++;
                users.add(SeededDummyUserActivity.skipped(persona, existing));
                continue;
            }

            PersonaActivityRule rule = PERSONA_RULES.getOrDefault(persona.loginId(), PersonaActivityRule.empty());
            FocusSelection focusSelection = resolveFocus(rule, movieCatalog);
            List<ScoredSeedMovie> scoredMovies = scoreMovies(movieCatalog, rule, focusSelection);
            ActivitySelection selection = selectMovies(scoredMovies, persona);

            insertedLikeCount += insertLikes(userId, selection.likeMovieIds());
            insertedStoreCount += insertStores(userId, selection.storeMovieIds());
            insertedLifeCount += insertLifeMovies(userId, selection.lifeMovieIds());

            recommendationRefreshStateService.markDirty(userId);
            if (refreshRecommendations) {
                recommendationMaintenanceService.ensureRecommendations(userId, 200);
            }

            seededUserCount++;
            users.add(new SeededDummyUserActivity(
                    persona.loginId(),
                    persona.nickname(),
                    persona.preferenceTypeSummary(),
                    "SEEDED",
                    selection.likeMovieIds().size(),
                    selection.storeMovieIds().size(),
                    selection.lifeMovieIds().size(),
                    focusSelection.directorName(),
                    focusSelection.actorName(),
                    selection.previewTitles()
            ));
        }

        return new DummyUserActivitySeedResult(
                reset,
                refreshRecommendations,
                seededUserCount,
                skippedUserCount,
                insertedLikeCount,
                insertedStoreCount,
                insertedLifeCount,
                deletionSummary,
                users
        );
    }

    private DeletionSummary deleteExistingActivities(Collection<Long> userIds) {
        List<Long> ids = userIds.stream().distinct().sorted().toList();
        if (ids.isEmpty()) {
            return DeletionSummary.empty();
        }

        int likeCount = deleteUserScopedRows("user_movie_like", ids);
        int collectionCount = deleteUserScopedRows("user_movie_collection", ids);
        int storeCount = deleteUserScopedRows("user_movie_store", ids);
        int watchedCount = deleteUserScopedRows("user_movie_watched", ids);
        int lifeCount = deleteUserScopedRows("user_movie_life", ids);
        int recommendationCount = deleteUserScopedRows("user_recommendation_result", ids);
        int profileCount = deleteUserScopedRows("user_preference_profile", ids);
        int refreshStateCount = deleteUserScopedRows("user_recommendation_refresh_state", ids);
        return new DeletionSummary(likeCount + collectionCount, storeCount, watchedCount, lifeCount, recommendationCount, profileCount, refreshStateCount);
    }

    private int deleteUserScopedRows(String tableName, List<Long> userIds) {
        String sql = "DELETE FROM " + tableName + " WHERE user_id IN (" + placeholders(userIds.size()) + ")";
        return jdbcTemplate.update(sql, userIds.toArray());
    }

    private Map<String, Long> loadTestUserIdsByLogin() {
        return jdbcTemplate.query("""
                SELECT id, login_id
                FROM "USER"
                WHERE login_id LIKE ?
                ORDER BY login_id
                """, rs -> {
            Map<String, Long> result = new LinkedHashMap<>();
            while (rs.next()) {
                result.put(rs.getString("login_id"), rs.getLong("id"));
            }
            return result;
        }, TEST_USER_LOGIN_PATTERN);
    }

    private ActivityCount currentActivityCount(Long userId) {
        int likeCount = countRows("user_movie_like", userId);
        int storeCount = countRows("user_movie_store", userId);
        int lifeCount = countRows("user_movie_life", userId);
        return new ActivityCount(likeCount, storeCount, lifeCount);
    }

    private int countRows(String tableName, Long userId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM " + tableName + " WHERE user_id = ?",
                Integer.class,
                userId
        );
        return count == null ? 0 : count;
    }

    private List<MovieSeedCandidate> loadMovieCatalog() {
        Map<Long, MovieSeedCandidateBuilder> builders = jdbcTemplate.query("""
                SELECT
                    id,
                    movie_cd,
                    COALESCE(title, movie_name) AS display_title,
                    ranking,
                    popularity
                FROM movie
                """, rs -> {
            Map<Long, MovieSeedCandidateBuilder> result = new LinkedHashMap<>();
            while (rs.next()) {
                long movieId = rs.getLong("id");
                result.put(movieId, new MovieSeedCandidateBuilder(
                        movieId,
                        rs.getString("movie_cd"),
                        rs.getString("display_title"),
                        rs.getObject("ranking", Integer.class),
                        rs.getObject("popularity", Double.class)
                ));
            }
            return result;
        });

        attachNamedFeatures(builders, """
                SELECT mt.movie_id, t.tag_name AS feature_name
                FROM movie_tag mt
                JOIN tag t ON t.id = mt.tag_id
                WHERE t.tag_type <> 'CAUTION'
                """, MovieSeedCandidateBuilder::addTag);
        attachNamedFeatures(builders, """
                SELECT mg.movie_id, g.name AS feature_name
                FROM movie_genre mg
                JOIN genre g ON g.id = mg.genre_id
                """, MovieSeedCandidateBuilder::addGenre);
        attachNamedFeatures(builders, """
                SELECT DISTINCT mp.movie_id, p.provider_name AS feature_name
                FROM movie_provider mp
                JOIN provider p ON p.id = mp.provider_id
                WHERE mp.provider_type = ?
                  AND mp.region_code = ?
                """, MovieSeedCandidateBuilder::addProvider,
                recommendationFeaturePolicy.preferredProviderType(),
                recommendationFeaturePolicy.preferredProviderRegionCode());
        attachNamedFeatures(builders, """
                SELECT md.movie_id, p.name AS feature_name
                FROM movie_director md
                JOIN person p ON p.id = md.person_id
                """, MovieSeedCandidateBuilder::addDirector);
        attachNamedFeatures(builders, """
                SELECT ma.movie_id, p.name AS feature_name
                FROM movie_actor ma
                JOIN person p ON p.id = ma.person_id
                WHERE ma.display_order <= ?
                """, MovieSeedCandidateBuilder::addActor,
                recommendationFeaturePolicy.actorLimit());

        return builders.values().stream()
                .map(MovieSeedCandidateBuilder::build)
                .toList();
    }

    private void attachNamedFeatures(
            Map<Long, MovieSeedCandidateBuilder> builders,
            String sql,
            FeatureConsumer consumer,
            Object... params
    ) {
        jdbcTemplate.query(sql, rs -> {
            MovieSeedCandidateBuilder builder = builders.get(rs.getLong("movie_id"));
            if (builder == null) {
                return;
            }
            String featureName = rs.getString("feature_name");
            if (featureName == null || featureName.isBlank()) {
                return;
            }
            consumer.accept(builder, featureName);
        }, params);
    }

    private FocusSelection resolveFocus(PersonaActivityRule rule, List<MovieSeedCandidate> movieCatalog) {
        if (rule.focusMode() == FocusMode.NONE) {
            return FocusSelection.none();
        }

        List<ScoredSeedMovie> preliminary = scoreMovies(movieCatalog, rule.withFocusMode(FocusMode.NONE), FocusSelection.none())
                .stream()
                .filter(movie -> movie.totalScore() > 0.0)
                .limit(120)
                .toList();

        if (rule.focusMode() == FocusMode.DIRECTOR) {
            String director = selectFrequentPerson(preliminary, scoredMovie -> scoredMovie.movie().directors(), 4);
            return director == null ? FocusSelection.none() : new FocusSelection(director, null);
        }

        String actor = selectFrequentPerson(preliminary, scoredMovie -> scoredMovie.movie().actors(), 4);
        return actor == null ? FocusSelection.none() : new FocusSelection(null, actor);
    }

    private String selectFrequentPerson(
            List<ScoredSeedMovie> preliminary,
            Function<ScoredSeedMovie, Set<String>> extractor,
            int minimumCount
    ) {
        Map<String, MutablePersonAggregate> aggregates = new LinkedHashMap<>();
        for (ScoredSeedMovie scoredMovie : preliminary) {
            for (String personName : extractor.apply(scoredMovie)) {
                MutablePersonAggregate aggregate = aggregates.computeIfAbsent(personName, ignored -> new MutablePersonAggregate(personName));
                aggregate.count++;
                aggregate.totalRank += scoredMovie.movie().rankingOrDefault(999);
            }
        }

        return aggregates.values().stream()
                .filter(aggregate -> aggregate.count >= minimumCount)
                .sorted(Comparator
                        .comparingInt(MutablePersonAggregate::count).reversed()
                        .thenComparingDouble(MutablePersonAggregate::averageRank)
                        .thenComparing(MutablePersonAggregate::name))
                .map(MutablePersonAggregate::name)
                .findFirst()
                .orElse(null);
    }

    private List<ScoredSeedMovie> scoreMovies(
            List<MovieSeedCandidate> movieCatalog,
            PersonaActivityRule rule,
            FocusSelection focusSelection
    ) {
        return movieCatalog.stream()
                .map(movie -> scoreMovie(movie, rule, focusSelection))
                .filter(scored -> scored.totalScore() > 0.0)
                .sorted(Comparator
                        .comparingDouble(ScoredSeedMovie::totalScore).reversed()
                        .thenComparing(Comparator.comparingDouble(ScoredSeedMovie::primaryScore).reversed())
                        .thenComparingInt(ScoredSeedMovie::ranking))
                .toList();
    }

    private ScoredSeedMovie scoreMovie(
            MovieSeedCandidate movie,
            PersonaActivityRule rule,
            FocusSelection focusSelection
    ) {
        double primaryScore = 0.0;
        double secondaryScore = 0.0;
        double noiseScore = 0.0;

        primaryScore += movie.tags().stream().filter(rule.primaryTags()::contains).count() * 2.8;
        primaryScore += genreScore(movie.genres(), rule.primaryGenres(), 1.9);
        primaryScore += providerScore(movie.providers(), rule.preferredProviderKeywords());

        secondaryScore += movie.tags().stream().filter(rule.secondaryTags()::contains).count() * 1.1;
        secondaryScore += genreScore(movie.genres(), rule.secondaryGenres(), 0.9);

        noiseScore += movie.tags().stream().filter(rule.noiseTags()::contains).count() * 0.55;
        noiseScore += genreScore(movie.genres(), rule.noiseGenres(), 0.45);

        double focusScore = 0.0;
        if (focusSelection.directorName() != null && movie.directors().contains(focusSelection.directorName())) {
            focusScore += 3.2;
        }
        if (focusSelection.actorName() != null && movie.actors().contains(focusSelection.actorName())) {
            focusScore += 2.8;
        }

        double popularityScore = movie.popularity() == null ? 0.0 : Math.min(1.0, movie.popularity() / 100.0) * 0.35;
        double rankingScore = movie.ranking() == null || movie.ranking() <= 0
                ? 0.0
                : Math.max(0.0, 1.0 - ((movie.ranking() - 1) / 150.0)) * 0.25;

        double totalScore = primaryScore + secondaryScore + noiseScore + focusScore + popularityScore + rankingScore;
        return new ScoredSeedMovie(
                movie,
                roundScore(totalScore),
                roundScore(primaryScore + focusScore),
                roundScore(secondaryScore),
                roundScore(noiseScore)
        );
    }

    private double genreScore(Set<String> movieGenres, List<String> targetGenres, double baseWeight) {
        double score = 0.0;
        for (String genre : targetGenres) {
            if (movieGenres.contains(genre)) {
                score += baseWeight * recommendationFeaturePolicy.broadGenreWeight(genre);
            }
        }
        return score;
    }

    private double providerScore(Set<String> providers, List<String> preferredProviderKeywords) {
        double score = 0.0;
        for (String provider : providers) {
            String normalizedProvider = normalize(provider);
            for (String keyword : preferredProviderKeywords) {
                if (normalizedProvider.contains(keyword)) {
                    score += 1.4;
                    break;
                }
            }
        }
        return score;
    }

    private ActivitySelection selectMovies(
            List<ScoredSeedMovie> scoredMovies,
            DummyUserSeedService.DummyUserPersona persona
    ) {
        LinkedHashSet<Long> used = new LinkedHashSet<>();

        List<ScoredSeedMovie> lifePool = scoredMovies.stream()
                .filter(movie -> movie.primaryScore() >= 4.2 || movie.totalScore() >= 6.2)
                .toList();
        List<ScoredSeedMovie> likePool = scoredMovies.stream()
                .filter(movie -> movie.primaryScore() >= 2.8 || movie.totalScore() >= 4.4)
                .toList();
        List<ScoredSeedMovie> storePool = scoredMovies;
        List<ScoredSeedMovie> noisePool = scoredMovies.stream()
                .filter(movie -> movie.noiseScore() > 0.0 && movie.primaryScore() < 4.0)
                .toList();

        List<Long> lifeMovieIds = pickMovieIds(lifePool, persona.targetLifeCount(), used);
        int likeNoiseCount = Math.min(1, Math.max(0, persona.targetLikeCount() / 6));
        List<Long> likeMovieIds = new ArrayList<>(pickMovieIds(likePool, persona.targetLikeCount() - likeNoiseCount, used));
        likeMovieIds.addAll(pickMovieIds(noisePool, likeNoiseCount, used));

        int storeNoiseCount = Math.min(2, Math.max(1, persona.targetStoreCount() / 4));
        List<Long> storeMovieIds = new ArrayList<>(pickMovieIds(storePool, persona.targetStoreCount() - storeNoiseCount, used));
        storeMovieIds.addAll(pickMovieIds(noisePool, storeNoiseCount, used));

        List<String> previewTitles = scoredMovies.stream()
                .filter(movie -> lifeMovieIds.contains(movie.movie().movieId())
                        || likeMovieIds.contains(movie.movie().movieId())
                        || storeMovieIds.contains(movie.movie().movieId()))
                .limit(6)
                .map(movie -> movie.movie().title())
                .toList();

        return new ActivitySelection(likeMovieIds, storeMovieIds, lifeMovieIds, previewTitles);
    }

    private List<Long> pickMovieIds(List<ScoredSeedMovie> pool, int targetCount, Set<Long> used) {
        if (targetCount <= 0) {
            return List.of();
        }

        List<Long> selected = new ArrayList<>();
        for (ScoredSeedMovie candidate : pool) {
            if (used.contains(candidate.movie().movieId())) {
                continue;
            }
            used.add(candidate.movie().movieId());
            selected.add(candidate.movie().movieId());
            if (selected.size() >= targetCount) {
                break;
            }
        }
        return selected;
    }

    private int insertLikes(Long userId, List<Long> movieIds) {
        if (movieIds.isEmpty()) {
            return 0;
        }
        jdbcTemplate.batchUpdate("""
                INSERT INTO user_movie_like (user_id, movie_id, liked, created_at)
                VALUES (?, ?, TRUE, ?)
                """, movieIds, movieIds.size(), (PreparedStatement ps, Long movieId) -> {
            ps.setLong(1, userId);
            ps.setLong(2, movieId);
            ps.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
        });
        return movieIds.size();
    }

    private int insertStores(Long userId, List<Long> movieIds) {
        if (movieIds.isEmpty()) {
            return 0;
        }
        jdbcTemplate.batchUpdate("""
                INSERT INTO user_movie_store (user_id, movie_id, created_at)
                VALUES (?, ?, ?)
                """, movieIds, movieIds.size(), (PreparedStatement ps, Long movieId) -> {
            ps.setLong(1, userId);
            ps.setLong(2, movieId);
            ps.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
        });
        return movieIds.size();
    }

    private int insertLifeMovies(Long userId, List<Long> movieIds) {
        if (movieIds.isEmpty()) {
            return 0;
        }
        jdbcTemplate.batchUpdate("""
                INSERT INTO user_movie_life (user_id, movie_id, created_at)
                VALUES (?, ?, ?)
                """, movieIds, movieIds.size(), (PreparedStatement ps, Long movieId) -> {
            ps.setLong(1, userId);
            ps.setLong(2, movieId);
            ps.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
        });
        return movieIds.size();
    }

    private String placeholders(int count) {
        return java.util.stream.IntStream.range(0, count)
                .mapToObj(index -> "?")
                .collect(Collectors.joining(", "));
    }

    private String normalize(String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    }

    private double roundScore(double value) {
        return Math.round(value * 1000.0) / 1000.0;
    }

    private enum FocusMode {
        NONE,
        DIRECTOR,
        ACTOR
    }

    private record PersonaActivityRule(
            List<String> primaryTags,
            List<String> secondaryTags,
            List<String> primaryGenres,
            List<String> secondaryGenres,
            List<String> preferredProviderKeywords,
            List<String> noiseTags,
            List<String> noiseGenres,
            FocusMode focusMode
    ) {
        private static PersonaActivityRule empty() {
            return new PersonaActivityRule(
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of(),
                    FocusMode.NONE
            );
        }

        private PersonaActivityRule withFocusMode(FocusMode updatedFocusMode) {
            return new PersonaActivityRule(
                    primaryTags,
                    secondaryTags,
                    primaryGenres,
                    secondaryGenres,
                    preferredProviderKeywords,
                    noiseTags,
                    noiseGenres,
                    updatedFocusMode
            );
        }
    }

    private record FocusSelection(String directorName, String actorName) {
        private static FocusSelection none() {
            return new FocusSelection(null, null);
        }
    }

    private record MovieSeedCandidate(
            long movieId,
            String movieCode,
            String title,
            Integer ranking,
            Double popularity,
            Set<String> tags,
            Set<String> genres,
            Set<String> providers,
            Set<String> directors,
            Set<String> actors
    ) {
        private int rankingOrDefault(int fallback) {
            return ranking == null ? fallback : ranking;
        }
    }

    private static final class MovieSeedCandidateBuilder {
        private final long movieId;
        private final String movieCode;
        private final String title;
        private final Integer ranking;
        private final Double popularity;
        private final Set<String> tags = new LinkedHashSet<>();
        private final Set<String> genres = new LinkedHashSet<>();
        private final Set<String> providers = new LinkedHashSet<>();
        private final Set<String> directors = new LinkedHashSet<>();
        private final Set<String> actors = new LinkedHashSet<>();

        private MovieSeedCandidateBuilder(long movieId, String movieCode, String title, Integer ranking, Double popularity) {
            this.movieId = movieId;
            this.movieCode = movieCode;
            this.title = title;
            this.ranking = ranking;
            this.popularity = popularity;
        }

        private void addTag(String tagName) {
            tags.add(tagName);
        }

        private void addGenre(String genreName) {
            genres.add(genreName);
        }

        private void addProvider(String providerName) {
            providers.add(providerName);
        }

        private void addDirector(String directorName) {
            directors.add(directorName);
        }

        private void addActor(String actorName) {
            actors.add(actorName);
        }

        private MovieSeedCandidate build() {
            return new MovieSeedCandidate(
                    movieId,
                    movieCode,
                    title,
                    ranking,
                    popularity,
                    Collections.unmodifiableSet(tags),
                    Collections.unmodifiableSet(genres),
                    Collections.unmodifiableSet(providers),
                    Collections.unmodifiableSet(directors),
                    Collections.unmodifiableSet(actors)
            );
        }
    }

    @FunctionalInterface
    private interface FeatureConsumer {
        void accept(MovieSeedCandidateBuilder builder, String featureName);
    }

    private record ScoredSeedMovie(
            MovieSeedCandidate movie,
            double totalScore,
            double primaryScore,
            double secondaryScore,
            double noiseScore
    ) {
        private int ranking() {
            return movie.rankingOrDefault(999);
        }
    }

    private record ActivitySelection(
            List<Long> likeMovieIds,
            List<Long> storeMovieIds,
            List<Long> lifeMovieIds,
            List<String> previewTitles
    ) {
    }

    private record ActivityCount(
            int likeCount,
            int storeCount,
            int lifeCount
    ) {
        private int totalCount() {
            return likeCount + storeCount + lifeCount;
        }
    }

    private static final class MutablePersonAggregate {
        private final String name;
        private int count;
        private int totalRank;

        private MutablePersonAggregate(String name) {
            this.name = name;
        }

        private String name() {
            return name;
        }

        private int count() {
            return count;
        }

        private double averageRank() {
            return count == 0 ? 999.0 : totalRank / (double) count;
        }
    }

    public record DeletionSummary(
            int likeCount,
            int storeCount,
            int watchedCount,
            int lifeCount,
            int recommendationCount,
            int profileCount,
            int refreshStateCount
    ) {
        private static DeletionSummary empty() {
            return new DeletionSummary(0, 0, 0, 0, 0, 0, 0);
        }
    }

    public record SeededDummyUserActivity(
            String loginId,
            String nickname,
            String preferenceTypeSummary,
            String status,
            int likeCount,
            int storeCount,
            int lifeCount,
            String focusDirector,
            String focusActor,
            List<String> previewTitles
    ) {
        private static SeededDummyUserActivity skipped(DummyUserSeedService.DummyUserPersona persona, ActivityCount existing) {
            return new SeededDummyUserActivity(
                    persona.loginId(),
                    persona.nickname(),
                    persona.preferenceTypeSummary(),
                    "SKIPPED",
                    existing.likeCount(),
                    existing.storeCount(),
                    existing.lifeCount(),
                    null,
                    null,
                    List.of()
            );
        }

        private static SeededDummyUserActivity missing(DummyUserSeedService.DummyUserPersona persona) {
            return new SeededDummyUserActivity(
                    persona.loginId(),
                    persona.nickname(),
                    persona.preferenceTypeSummary(),
                    "MISSING_USER",
                    0,
                    0,
                    0,
                    null,
                    null,
                    List.of()
            );
        }
    }

    public record DummyUserActivitySeedResult(
            boolean reset,
            boolean refreshRecommendations,
            int seededUserCount,
            int skippedUserCount,
            int insertedLikeCount,
            int insertedStoreCount,
            int insertedLifeCount,
            DeletionSummary deletionSummary,
            List<SeededDummyUserActivity> users
    ) {
    }
}
