package movie.web.login.recommendation;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class RecommendationRankingService {

    private static final int DEFAULT_LIMIT = 200;
    private static final int MAX_LIMIT = 300;

    private static final double DIRECTOR_SHARE = 0.70;
    private static final double ACTOR_SHARE = 0.30;

    private final JdbcTemplate jdbcTemplate;
    private final RecommendationRefreshStateService recommendationRefreshStateService;
    private final RecommendationFeaturePolicy recommendationFeaturePolicy;

    public RecommendationRankingService(
            JdbcTemplate jdbcTemplate,
            RecommendationRefreshStateService recommendationRefreshStateService,
            RecommendationFeaturePolicy recommendationFeaturePolicy
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.recommendationRefreshStateService = recommendationRefreshStateService;
        this.recommendationFeaturePolicy = recommendationFeaturePolicy;
    }

    public RankingRebuildResult rebuildRanking(Long userId, Integer limit) {
        initializeRecommendationResultTable();

        int normalizedLimit = normalizeLimit(limit);
        PreferenceProfile profile = loadProfile(userId);

        deleteExistingResults(userId);

        if (profile.isEmpty()) {
            recommendationRefreshStateService.markRefreshed(userId, RecommendationFeaturePolicy.ALGORITHM_VERSION);
            return new RankingRebuildResult(userId, 0, 0, normalizedLimit, RecommendationFeaturePolicy.ALGORITHM_VERSION);
        }

        Set<Long> excludedMovieIds = loadExcludedMovieIds(userId);
        List<CandidateMovie> candidateMovies = loadCandidateMovies(profile, excludedMovieIds);
        if (candidateMovies.isEmpty()) {
            recommendationRefreshStateService.markRefreshed(userId, RecommendationFeaturePolicy.ALGORITHM_VERSION);
            return new RankingRebuildResult(userId, 0, 0, normalizedLimit, RecommendationFeaturePolicy.ALGORITHM_VERSION);
        }

        Set<Long> candidateMovieIds = candidateMovies.stream()
                .map(CandidateMovie::movieId)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Map<Long, Set<String>> tagsByMovie = loadNamedFeatureSetMap(candidateMovieIds, """
                SELECT mt.movie_id, t.tag_name AS feature_name
                FROM movie_tag mt
                JOIN tag t ON t.id = mt.tag_id
                WHERE mt.movie_id IN (%s)
                  AND t.tag_type <> 'CAUTION'
                """);
        Map<Long, Set<String>> cautionTagsByMovie = loadNamedFeatureSetMap(candidateMovieIds, """
                SELECT mt.movie_id, t.tag_name AS feature_name
                FROM movie_tag mt
                JOIN tag t ON t.id = mt.tag_id
                WHERE mt.movie_id IN (%s)
                  AND t.tag_type = 'CAUTION'
                """);
        Map<Long, Set<String>> genresByMovie = loadNamedFeatureSetMap(candidateMovieIds, """
                SELECT mg.movie_id, g.name AS feature_name
                FROM movie_genre mg
                JOIN genre g ON g.id = mg.genre_id
                WHERE mg.movie_id IN (%s)
                """);
        Map<Long, Set<String>> directorsByMovie = loadNamedFeatureSetMap(candidateMovieIds, """
                SELECT md.movie_id, p.name AS feature_name
                FROM movie_director md
                JOIN person p ON p.id = md.person_id
                WHERE md.movie_id IN (%s)
                """);
        Map<Long, Set<String>> actorsByMovie = loadNamedFeatureSetMap(candidateMovieIds, """
                SELECT ma.movie_id, p.name AS feature_name
                FROM movie_actor ma
                JOIN person p ON p.id = ma.person_id
                WHERE ma.movie_id IN (%s)
                  AND ma.display_order <= %d
                """.formatted("%s", recommendationFeaturePolicy.actorLimit()));
        Map<Long, Set<String>> keywordsByMovie = loadNamedFeatureSetMap(candidateMovieIds, """
                SELECT mk.movie_id, k.name AS feature_name
                FROM movie_keyword mk
                JOIN keyword k ON k.id = mk.keyword_id
                WHERE mk.movie_id IN (%s)
                  AND mk.display_order <= %d
                  AND LOWER(k.name) NOT IN (%s)
                """.formatted("%s",
                recommendationFeaturePolicy.keywordLimit(),
                recommendationFeaturePolicy.keywordBlacklistSqlLiteralList()));
        Map<Long, Set<String>> providersByMovie = loadNamedFeatureSetMap(candidateMovieIds, """
                SELECT DISTINCT mp.movie_id, p.provider_name AS feature_name
                FROM movie_provider mp
                JOIN provider p ON p.id = mp.provider_id
                WHERE mp.movie_id IN (%s)
                  AND mp.provider_type = '%s'
                  AND mp.region_code = '%s'
                """.formatted("%s",
                recommendationFeaturePolicy.preferredProviderType(),
                recommendationFeaturePolicy.preferredProviderRegionCode()));

        List<ScoredRecommendation> rankedRecommendations = new ArrayList<>();
        for (CandidateMovie movie : candidateMovies) {
            MatchSummary tagMatch = computeMatch("TAG", profile.tagScores(), profile.tagTotalScore(),
                    tagsByMovie.getOrDefault(movie.movieId(), Collections.emptySet()));
            MatchSummary genreMatch = computeMatch("GENRE", profile.genreScores(), profile.genreTotalScore(),
                    genresByMovie.getOrDefault(movie.movieId(), Collections.emptySet()));
            MatchSummary directorMatch = computeMatch("DIRECTOR", profile.directorScores(), profile.directorTotalScore(),
                    directorsByMovie.getOrDefault(movie.movieId(), Collections.emptySet()));
            MatchSummary actorMatch = computeMatch("ACTOR", profile.actorScores(), profile.actorTotalScore(),
                    actorsByMovie.getOrDefault(movie.movieId(), Collections.emptySet()));
            MatchSummary keywordMatch = computeMatch("KEYWORD", profile.keywordScores(), profile.keywordTotalScore(),
                    keywordsByMovie.getOrDefault(movie.movieId(), Collections.emptySet()));
            MatchSummary providerMatch = computeMatch("PROVIDER", profile.providerScores(), profile.providerTotalScore(),
                    providersByMovie.getOrDefault(movie.movieId(), Collections.emptySet()));
            MatchSummary cautionMatch = computeMatch("CAUTION", profile.cautionScores(), profile.cautionTotalScore(),
                    cautionTagsByMovie.getOrDefault(movie.movieId(), Collections.emptySet()));

            double tagScore = tagMatch.normalizedScore();
            double genreScore = genreMatch.normalizedScore();
            double keywordScore = keywordMatch.normalizedScore();
            double peopleScore = Math.min(1.0,
                    (directorMatch.normalizedScore() * DIRECTOR_SHARE) + (actorMatch.normalizedScore() * ACTOR_SHARE));
            double providerScore = providerMatch.normalizedScore();
            double penaltyScore = roundScore(cautionMatch.normalizedScore() * recommendationFeaturePolicy.cautionPenaltyWeight());
            double popularityScore = popularityScore(movie);
            double freshnessBonus = freshnessBonus(movie);

            int matchedSignalCount = countMatchedSignals(tagScore, genreScore, peopleScore, keywordScore, providerScore);
            double matchCoverage = tagScore + genreScore + peopleScore + keywordScore + providerScore;
            if (matchCoverage <= 0.0) {
                continue;
            }

            double finalScore = roundScore(
                    (recommendationFeaturePolicy.rankingWeight("TAG") * tagScore)
                            + (recommendationFeaturePolicy.rankingWeight("GENRE") * genreScore)
                            + (recommendationFeaturePolicy.rankingWeight("KEYWORD") * keywordScore)
                            + (recommendationFeaturePolicy.rankingWeight("PEOPLE") * peopleScore)
                            + (recommendationFeaturePolicy.rankingWeight("PROVIDER") * providerScore)
                            + (recommendationFeaturePolicy.rankingWeight("POPULARITY") * popularityScore)
                            + (recommendationFeaturePolicy.rankingWeight("FRESHNESS") * freshnessBonus)
                            + recommendationFeaturePolicy.multiSignalBonus(matchedSignalCount)
                            - penaltyScore
            );

            rankedRecommendations.add(new ScoredRecommendation(
                    movie,
                    finalScore,
                    roundScore(tagScore),
                    roundScore(genreScore),
                    roundScore(peopleScore),
                    roundScore(keywordScore),
                    roundScore(providerScore),
                    roundScore(penaltyScore),
                    buildReasonSummary(tagMatch, genreMatch, directorMatch, actorMatch, keywordMatch, providerMatch),
                    tagMatch.primaryFeatureName(),
                    genreMatch.primaryFeatureName(),
                    directorMatch.primaryFeatureName(),
                    actorMatch.primaryFeatureName()
            ));
        }

        rankedRecommendations.sort(Comparator
                .comparingDouble(ScoredRecommendation::finalScore).reversed()
                .thenComparing(Comparator.comparingDouble(ScoredRecommendation::tagScore).reversed())
                .thenComparing(Comparator.comparingDouble(ScoredRecommendation::genreScore).reversed())
                .thenComparing(Comparator.comparingDouble(ScoredRecommendation::peopleScore).reversed())
                .thenComparing(ScoredRecommendation::movieRanking, Comparator.nullsLast(Integer::compareTo))
                .thenComparing(ScoredRecommendation::movieTitle, Comparator.nullsLast(String::compareTo)));

        List<ScoredRecommendation> topRecommendations = applyDiversityReRank(rankedRecommendations, normalizedLimit);

        saveRecommendations(userId, topRecommendations);
        recommendationRefreshStateService.markRefreshed(userId, RecommendationFeaturePolicy.ALGORITHM_VERSION);

        return new RankingRebuildResult(
                userId,
                rankedRecommendations.size(),
                topRecommendations.size(),
                normalizedLimit,
                RecommendationFeaturePolicy.ALGORITHM_VERSION
        );
    }

    private void initializeRecommendationResultTable() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS user_recommendation_result (
                    user_id BIGINT NOT NULL,
                    movie_id BIGINT NOT NULL,
                    rank_no INT NOT NULL,
                    final_score DOUBLE PRECISION NOT NULL,
                    tag_score DOUBLE PRECISION NOT NULL,
                    genre_score DOUBLE PRECISION NOT NULL,
                    people_score DOUBLE PRECISION NOT NULL,
                    keyword_score DOUBLE PRECISION NOT NULL,
                    provider_score DOUBLE PRECISION NOT NULL,
                    penalty_score DOUBLE PRECISION NOT NULL,
                    reason_summary VARCHAR(500),
                    algorithm_version VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, movie_id),
                    CONSTRAINT uk_user_recommendation_result_rank UNIQUE (user_id, rank_no),
                    CONSTRAINT fk_user_recommendation_result_user FOREIGN KEY (user_id) REFERENCES "USER"(id),
                    CONSTRAINT fk_user_recommendation_result_movie FOREIGN KEY (movie_id) REFERENCES movie(id)
                )
                """);
    }

    private PreferenceProfile loadProfile(Long userId) {
        Map<String, Double> tagScores = new LinkedHashMap<>();
        Map<String, Double> cautionScores = new LinkedHashMap<>();
        Map<String, Double> genreScores = new LinkedHashMap<>();
        Map<String, Double> directorScores = new LinkedHashMap<>();
        Map<String, Double> actorScores = new LinkedHashMap<>();
        Map<String, Double> keywordScores = new LinkedHashMap<>();
        Map<String, Double> providerScores = new LinkedHashMap<>();

        jdbcTemplate.query("""
                SELECT feature_type, feature_name, score
                FROM user_preference_profile
                WHERE user_id = ?
                """, rs -> {
            Map<String, Double> target = switch (rs.getString("feature_type")) {
                case "TAG" -> tagScores;
                case "CAUTION" -> cautionScores;
                case "GENRE" -> genreScores;
                case "DIRECTOR" -> directorScores;
                case "ACTOR" -> actorScores;
                case "KEYWORD" -> keywordScores;
                case "PROVIDER" -> providerScores;
                default -> null;
            };
            if (target != null) {
                target.put(rs.getString("feature_name"), rs.getDouble("score"));
            }
        }, userId);

        return new PreferenceProfile(
                tagScores,
                totalScore(tagScores),
                cautionScores,
                totalScore(cautionScores),
                genreScores,
                totalScore(genreScores),
                directorScores,
                totalScore(directorScores),
                actorScores,
                totalScore(actorScores),
                keywordScores,
                totalScore(keywordScores),
                providerScores,
                totalScore(providerScores)
        );
    }

    private double totalScore(Map<String, Double> scores) {
        return scores.values().stream().mapToDouble(Double::doubleValue).sum();
    }

    private Set<Long> loadExcludedMovieIds(Long userId) {
        initializeWatchedSignalTable();
        Set<Long> excludedMovieIds = new LinkedHashSet<>();
        jdbcTemplate.query("""
                SELECT movie_id
                FROM (
                    SELECT movie_id FROM user_movie_life WHERE user_id = ?
                    UNION
                    SELECT movie_id FROM user_movie_like WHERE user_id = ? AND liked = TRUE
                    UNION
                    SELECT movie_id FROM user_movie_store WHERE user_id = ?
                    UNION
                    SELECT movie_id
                    FROM user_movie_watched
                    WHERE user_id = ?
                      AND COALESCE(status, 'WATCHED') = 'WATCHED'
                ) excluded
                """, (org.springframework.jdbc.core.RowCallbackHandler) rs ->
                        excludedMovieIds.add(rs.getLong("movie_id")), userId, userId, userId, userId);
        return excludedMovieIds;
    }

    private void initializeWatchedSignalTable() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS user_movie_watched (
                    user_id BIGINT NOT NULL,
                    movie_id BIGINT NOT NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'WATCHED',
                    rating INT,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, movie_id),
                    CONSTRAINT fk_user_movie_watched_user FOREIGN KEY (user_id) REFERENCES "USER"(id),
                    CONSTRAINT fk_user_movie_watched_movie FOREIGN KEY (movie_id) REFERENCES movie(id)
                )
                """);
        jdbcTemplate.execute("""
                ALTER TABLE user_movie_watched
                ADD COLUMN IF NOT EXISTS rating INT
                """);
        jdbcTemplate.execute("""
                ALTER TABLE user_movie_watched
                ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'WATCHED'
                """);
        jdbcTemplate.update("""
                UPDATE user_movie_watched
                SET status = 'WATCHED'
                WHERE status IS NULL
                """);
    }

    private List<CandidateMovie> loadCandidateMovies(PreferenceProfile profile, Set<Long> excludedMovieIds) {
        List<String> matchConditions = new ArrayList<>();
        List<Object> matchParams = new ArrayList<>();

        addExistsCondition(matchConditions, matchParams, profile.tagScores().keySet(), """
                EXISTS (
                    SELECT 1
                    FROM movie_tag mt
                    JOIN tag t ON t.id = mt.tag_id
                    WHERE mt.movie_id = m.id
                      AND t.tag_type <> 'CAUTION'
                      AND t.tag_name IN (%s)
                )
                """);
        addExistsCondition(matchConditions, matchParams, profile.genreScores().keySet(), """
                EXISTS (
                    SELECT 1
                    FROM movie_genre mg
                    JOIN genre g ON g.id = mg.genre_id
                    WHERE mg.movie_id = m.id
                      AND g.name IN (%s)
                )
                """);
        addExistsCondition(matchConditions, matchParams, profile.directorScores().keySet(), """
                EXISTS (
                    SELECT 1
                    FROM movie_director md
                    JOIN person p ON p.id = md.person_id
                    WHERE md.movie_id = m.id
                      AND p.name IN (%s)
                )
                """);
        addExistsCondition(matchConditions, matchParams, profile.actorScores().keySet(), """
                EXISTS (
                    SELECT 1
                    FROM movie_actor ma
                    JOIN person p ON p.id = ma.person_id
                    WHERE ma.movie_id = m.id
                      AND ma.display_order <= %d
                      AND p.name IN (%s)
                )
                """.formatted(recommendationFeaturePolicy.actorLimit(), "%s"));
        addExistsCondition(matchConditions, matchParams, profile.keywordScores().keySet(), """
                EXISTS (
                    SELECT 1
                    FROM movie_keyword mk
                    JOIN keyword k ON k.id = mk.keyword_id
                    WHERE mk.movie_id = m.id
                      AND mk.display_order <= %d
                      AND LOWER(k.name) NOT IN (%s)
                      AND k.name IN (%s)
                )
                """.formatted(
                recommendationFeaturePolicy.keywordLimit(),
                recommendationFeaturePolicy.keywordBlacklistSqlLiteralList(),
                "%s"));
        addExistsCondition(matchConditions, matchParams, profile.providerScores().keySet(), """
                EXISTS (
                    SELECT 1
                    FROM movie_provider mp
                    JOIN provider p ON p.id = mp.provider_id
                    WHERE mp.movie_id = m.id
                      AND mp.provider_type = '%s'
                      AND mp.region_code = '%s'
                      AND p.provider_name IN (%s)
                )
                """.formatted(
                recommendationFeaturePolicy.preferredProviderType(),
                recommendationFeaturePolicy.preferredProviderRegionCode(),
                "%s"));

        if (matchConditions.isEmpty()) {
            return List.of();
        }

        List<Object> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder("""
                SELECT
                    m.id,
                    m.movie_cd,
                    COALESCE(m.title, m.movie_name) AS display_title,
                    COALESCE(m.release_date, m.movie_info_open_date, m.box_office_open_date) AS release_date,
                    m.production_year,
                    m.popularity,
                    m.vote_average,
                    m.vote_count,
                    m.ranking
                FROM movie m
                WHERE 1 = 1
                """);

        if (!excludedMovieIds.isEmpty()) {
            sql.append(" AND m.id NOT IN (").append(placeholders(excludedMovieIds.size())).append(")");
            params.addAll(excludedMovieIds);
        }

        sql.append(" AND (").append(String.join(" OR ", matchConditions)).append(")");
        params.addAll(matchParams);

        return jdbcTemplate.query(sql.toString(), (rs, rowNum) -> new CandidateMovie(
                rs.getLong("id"),
                rs.getString("movie_cd"),
                rs.getString("display_title"),
                rs.getObject("release_date", LocalDate.class),
                rs.getObject("production_year", Integer.class),
                rs.getObject("popularity", Double.class),
                rs.getObject("vote_average", Double.class),
                rs.getObject("vote_count", Integer.class),
                rs.getObject("ranking", Integer.class)
        ), params.toArray());
    }

    private void addExistsCondition(
            List<String> conditions,
            List<Object> params,
            Set<String> features,
            String template
    ) {
        if (features.isEmpty()) {
            return;
        }
        conditions.add(template.formatted(placeholders(features.size())));
        params.addAll(features);
    }

    private Map<Long, Set<String>> loadNamedFeatureSetMap(Set<Long> movieIds, String template) {
        if (movieIds.isEmpty()) {
            return Collections.emptyMap();
        }

        String sql = template.formatted(placeholders(movieIds.size()));
        Map<Long, Set<String>> featuresByMovie = new LinkedHashMap<>();
        List<Object> params = new ArrayList<>(movieIds);

        jdbcTemplate.query(sql, (org.springframework.jdbc.core.RowCallbackHandler) rs -> {
            long movieId = rs.getLong("movie_id");
            String featureName = rs.getString("feature_name");
            if (featureName == null || featureName.isBlank()) {
                return;
            }
            featuresByMovie.computeIfAbsent(movieId, ignored -> new LinkedHashSet<>())
                    .add(featureName);
        }, params.toArray());

        return featuresByMovie;
    }

    private MatchSummary computeMatch(
            String featureType,
            Map<String, Double> profileScores,
            double totalProfileScore,
            Set<String> candidateFeatures
    ) {
        if (profileScores.isEmpty() || totalProfileScore <= 0.0 || candidateFeatures.isEmpty()) {
            return MatchSummary.empty();
        }

        List<WeightedFeature> matchedFeatures = candidateFeatures.stream()
                .map(feature -> {
                    double baseScore = profileScores.getOrDefault(feature, 0.0);
                    double weightedScore = baseScore * recommendationFeaturePolicy.featureSpecificWeight(featureType, feature);
                    return new WeightedFeature(feature, weightedScore);
                })
                .filter(feature -> feature.score() > 0.0)
                .sorted(Comparator.comparingDouble(WeightedFeature::score).reversed()
                        .thenComparing(WeightedFeature::name))
                .toList();

        if (matchedFeatures.isEmpty()) {
            return MatchSummary.empty();
        }

        double matchedScore = matchedFeatures.stream().mapToDouble(WeightedFeature::score).sum();
        double rawShare = Math.min(1.0, matchedScore / totalProfileScore);
        double evidenceFactor = Math.min(
                1.0,
                (double) matchedFeatures.size() / recommendationFeaturePolicy.idealMatchCount(featureType)
        );
        double densityFactor = (double) matchedFeatures.size() / candidateFeatures.size();
        double normalizedScore = Math.min(
                1.0,
                rawShare
                        * (recommendationFeaturePolicy.evidenceBase(featureType)
                        + ((1.0 - recommendationFeaturePolicy.evidenceBase(featureType)) * evidenceFactor))
                        * (recommendationFeaturePolicy.densityBase(featureType)
                        + ((1.0 - recommendationFeaturePolicy.densityBase(featureType)) * densityFactor))
        );

        return new MatchSummary(matchedScore, roundScore(normalizedScore), matchedFeatures);
    }

    private double popularityScore(CandidateMovie movie) {
        double popularity = movie.popularity() == null ? 0.0 : movie.popularity();
        double popularityComponent = Math.min(1.0, popularity / 100.0);

        double voteAverage = movie.voteAverage() == null ? 0.0 : movie.voteAverage();
        double voteCount = movie.voteCount() == null ? 0.0 : movie.voteCount();
        double voteComponent = voteAverage <= 0.0
                ? 0.0
                : Math.min(1.0, (voteAverage / 10.0) * Math.min(1.0, voteCount / 500.0));

        double rankingComponent = movie.ranking() == null || movie.ranking() <= 0
                ? 0.0
                : Math.max(0.0, 1.0 - ((movie.ranking() - 1) / 100.0));

        return roundScore(Math.min(1.0, popularityComponent * 0.6 + voteComponent * 0.25 + rankingComponent * 0.15));
    }

    private double freshnessBonus(CandidateMovie movie) {
        LocalDate releaseDate = movie.releaseDate();
        Integer productionYear = movie.productionYear();
        if (releaseDate == null && productionYear == null) {
            return 0.0;
        }

        int releaseYear = releaseDate != null ? releaseDate.getYear() : productionYear;
        long yearsOld = Math.max(0, ChronoUnit.YEARS.between(LocalDate.of(releaseYear, 1, 1), LocalDate.now()));
        return roundScore(Math.max(0.0, 1.0 - (yearsOld / 20.0)));
    }

    private List<ScoredRecommendation> applyDiversityReRank(List<ScoredRecommendation> rankedRecommendations, int limit) {
        if (rankedRecommendations.isEmpty()) {
            return List.of();
        }

        int poolSize = Math.min(rankedRecommendations.size(), Math.max(limit * 3, 30));
        List<ScoredRecommendation> pool = new ArrayList<>(rankedRecommendations.subList(0, poolSize));
        List<ScoredRecommendation> selected = new ArrayList<>();

        while (!pool.isEmpty() && selected.size() < limit) {
            ScoredRecommendation bestRecommendation = null;
            double bestAdjustedScore = Double.NEGATIVE_INFINITY;

            for (ScoredRecommendation candidate : pool) {
                double adjustedScore = candidate.finalScore() - diversityPenalty(candidate, selected);
                if (bestRecommendation == null
                        || adjustedScore > bestAdjustedScore
                        || (adjustedScore == bestAdjustedScore && candidate.finalScore() > bestRecommendation.finalScore())) {
                    bestRecommendation = candidate;
                    bestAdjustedScore = adjustedScore;
                }
            }

            selected.add(bestRecommendation);
            pool.remove(bestRecommendation);
        }

        return selected;
    }

    private double diversityPenalty(ScoredRecommendation candidate, List<ScoredRecommendation> selected) {
        if (selected.isEmpty()) {
            return 0.0;
        }

        double penalty = 0.0;
        int windowStart = Math.max(0, selected.size() - 2);
        for (int index = windowStart; index < selected.size(); index++) {
            ScoredRecommendation previous = selected.get(index);
            if (sameValue(candidate.primaryTag(), previous.primaryTag())) {
                penalty += 0.035;
            }
            if (sameValue(candidate.primaryGenre(), previous.primaryGenre())) {
                penalty += 0.018;
            }
            if (sameValue(candidate.primaryDirector(), previous.primaryDirector())) {
                penalty += 0.020;
            }
        }
        return penalty;
    }

    private boolean sameValue(String left, String right) {
        return left != null && right != null && left.equals(right);
    }

    private void saveRecommendations(Long userId, List<ScoredRecommendation> recommendations) {
        if (recommendations.isEmpty()) {
            return;
        }

        LocalDateTime createdAt = LocalDateTime.now();
        List<RankedRecommendationRow> rows = new ArrayList<>();
        for (int index = 0; index < recommendations.size(); index++) {
            rows.add(new RankedRecommendationRow(index + 1, recommendations.get(index)));
        }

        jdbcTemplate.batchUpdate("""
                INSERT INTO user_recommendation_result (
                    user_id,
                    movie_id,
                    rank_no,
                    final_score,
                    tag_score,
                    genre_score,
                    people_score,
                    keyword_score,
                    provider_score,
                    penalty_score,
                    reason_summary,
                    algorithm_version,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, rows, rows.size(), (PreparedStatement ps, RankedRecommendationRow row) -> {
            ps.setLong(1, userId);
            ps.setLong(2, row.recommendation().movie().movieId());
            ps.setInt(3, row.rankNo());
            ps.setDouble(4, row.recommendation().finalScore());
            ps.setDouble(5, row.recommendation().tagScore());
            ps.setDouble(6, row.recommendation().genreScore());
            ps.setDouble(7, row.recommendation().peopleScore());
            ps.setDouble(8, row.recommendation().keywordScore());
            ps.setDouble(9, row.recommendation().providerScore());
            ps.setDouble(10, row.recommendation().penaltyScore());
            ps.setString(11, row.recommendation().reasonSummary());
            ps.setString(12, RecommendationFeaturePolicy.ALGORITHM_VERSION);
            ps.setTimestamp(13, Timestamp.valueOf(createdAt));
        });
    }

    private void deleteExistingResults(Long userId) {
        jdbcTemplate.update("""
                DELETE FROM user_recommendation_result
                WHERE user_id = ?
                """, userId);
    }

    private int normalizeLimit(Integer limit) {
        if (limit == null) {
            return DEFAULT_LIMIT;
        }
        return Math.max(1, Math.min(limit, MAX_LIMIT));
    }

    private int countMatchedSignals(double tagScore, double genreScore, double peopleScore, double keywordScore, double providerScore) {
        int count = 0;
        if (tagScore > 0.0) {
            count++;
        }
        if (genreScore > 0.0) {
            count++;
        }
        if (peopleScore > 0.0) {
            count++;
        }
        if (keywordScore > 0.0) {
            count++;
        }
        if (providerScore > 0.0) {
            count++;
        }
        return count;
    }

    private String buildReasonSummary(
            MatchSummary tagMatch,
            MatchSummary genreMatch,
            MatchSummary directorMatch,
            MatchSummary actorMatch,
            MatchSummary keywordMatch,
            MatchSummary providerMatch
    ) {
        List<String> reasons = new ArrayList<>();

        if (tagMatch.primaryFeatureName() != null) {
            reasons.add(recommendationFeaturePolicy.tagDisplayName(tagMatch.primaryFeatureName()) + " 태그");
        }
        if (genreMatch.primaryFeatureName() != null) {
            reasons.add(genreMatch.primaryFeatureName() + " 장르");
        }
        if (directorMatch.primaryFeatureName() != null) {
            reasons.add(directorMatch.primaryFeatureName() + " 감독");
        } else if (actorMatch.primaryFeatureName() != null) {
            reasons.add(actorMatch.primaryFeatureName() + " 배우");
        }
        if (reasons.size() < 2 && keywordMatch.primaryFeatureName() != null) {
            reasons.add(keywordMatch.primaryFeatureName() + " 키워드");
        }
        if (reasons.size() < 2 && providerMatch.primaryFeatureName() != null) {
            reasons.add(providerMatch.primaryFeatureName() + " 시청 가능");
        }

        if (reasons.isEmpty()) {
            return "선호 특징과 유사한 영화";
        }
        if (reasons.size() == 1) {
            return "추천 이유: " + reasons.get(0) + " 일치";
        }
        return "추천 이유: " + reasons.get(0) + " + " + reasons.get(1);
    }

    private String placeholders(int count) {
        return java.util.stream.IntStream.range(0, count)
                .mapToObj(index -> "?")
                .collect(Collectors.joining(", "));
    }

    private double roundScore(double value) {
        return Math.round(value * 1000.0) / 1000.0;
    }

    private record PreferenceProfile(
            Map<String, Double> tagScores,
            double tagTotalScore,
            Map<String, Double> cautionScores,
            double cautionTotalScore,
            Map<String, Double> genreScores,
            double genreTotalScore,
            Map<String, Double> directorScores,
            double directorTotalScore,
            Map<String, Double> actorScores,
            double actorTotalScore,
            Map<String, Double> keywordScores,
            double keywordTotalScore,
            Map<String, Double> providerScores,
            double providerTotalScore
    ) {
        private boolean isEmpty() {
            return tagScores.isEmpty()
                    && genreScores.isEmpty()
                    && directorScores.isEmpty()
                    && actorScores.isEmpty()
                    && keywordScores.isEmpty()
                    && providerScores.isEmpty();
        }
    }

    private record CandidateMovie(
            long movieId,
            String movieCode,
            String title,
            LocalDate releaseDate,
            Integer productionYear,
            Double popularity,
            Double voteAverage,
            Integer voteCount,
            Integer ranking
    ) {
    }

    private record WeightedFeature(String name, double score) {
    }

    private record MatchSummary(
            double matchedScore,
            double normalizedScore,
            List<WeightedFeature> matchedFeatures
    ) {
        private static MatchSummary empty() {
            return new MatchSummary(0.0, 0.0, List.of());
        }

        private String primaryFeatureName() {
            return matchedFeatures.isEmpty() ? null : matchedFeatures.get(0).name();
        }
    }

    private record ScoredRecommendation(
            CandidateMovie movie,
            double finalScore,
            double tagScore,
            double genreScore,
            double peopleScore,
            double keywordScore,
            double providerScore,
            double penaltyScore,
            String reasonSummary,
            String primaryTag,
            String primaryGenre,
            String primaryDirector,
            String primaryActor
    ) {
        private Integer movieRanking() {
            return movie.ranking();
        }

        private String movieTitle() {
            return movie.title();
        }
    }

    private record RankedRecommendationRow(int rankNo, ScoredRecommendation recommendation) {
    }

    public record RankingRebuildResult(
            Long userId,
            int candidateCount,
            int savedCount,
            int limit,
            String algorithmVersion
    ) {
    }
}
